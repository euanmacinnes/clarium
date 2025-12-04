use anyhow::Result;
use polars::prelude::*;
use crate::tprintln;
use crate::server::query::query_common::Query;
use crate::server::query::query_common::WhereExpr;
use crate::server::query::query_common::CompOp;
use crate::server::query::query_common::ArithExpr as AE;
use crate::server::query::query_common::ArithTerm as AT;
use crate::server::query::query_common::WhereExpr as WE;
use crate::server::query::query_common::ArithExpr;
use crate::server::query::query_common::DateFunc;
use crate::server::query::query_common::StrSliceBound;
use crate::server::query::query_common::JoinType;
use crate::server::exec::exec_common::{build_where_expr, collect_where_columns};
use crate::scripts::get_script_registry;

/// Resolve a column name against the current DataFrame columns allowing unqualified/suffix matches
fn resolve_name_in_df(df: &DataFrame, name: &str) -> anyhow::Result<String> {
    if name.contains('.') {
        if df.get_column_names().iter().any(|n| n.as_str() == name) {
            return Ok(name.to_string());
        }
        anyhow::bail!(format!("Column not found: {}", name));
    }
    if df.get_column_names().iter().any(|n| n.as_str() == name) {
        return Ok(name.to_string());
    }
    let needle = format!(".{}", name);
    let matches: Vec<String> = df
        .get_column_names()
        .iter()
        .filter_map(|c| {
            let s = c.as_str();
            if s.ends_with(&needle) { Some(s.to_string()) } else { None }
        })
        .collect();
    if matches.len() == 1 { return Ok(matches[0].clone()); }
    if matches.is_empty() { anyhow::bail!(format!("Column not found: {}", name)); }
    anyhow::bail!(format!("Ambiguous column '{}'; qualify with table alias", name));
}

fn qualify_having_arith(df: &DataFrame, a: &ArithExpr) -> ArithExpr {    
    match a {
        AE::Term(AT::Col { name, previous: false }) => {
            let qn = resolve_name_in_df(df, name).unwrap_or_else(|_| name.to_string());
            AE::Term(AT::Col { name: qn, previous: false })
        }
        AE::Term(_) => a.clone(),
        AE::Cast { expr, ty } => {
            AE::Cast { expr: Box::new(qualify_having_arith(df, expr)), ty: ty.clone() }
        }
        AE::BinOp { left, op, right } => AE::BinOp { left: Box::new(qualify_having_arith(df, left)), op: op.clone(), right: Box::new(qualify_having_arith(df, right)) },
        AE::Concat(parts) => AE::Concat(parts.iter().map(|p| qualify_having_arith(df, p)).collect()),
        AE::Call { name, args } => AE::Call { name: name.clone(), args: args.iter().map(|p| qualify_having_arith(df, p)).collect() },
        AE::Func(dfm) => {            
            match dfm {
                DateFunc::DatePart(part, a1) => AE::Func(DateFunc::DatePart(part.clone(), Box::new(qualify_having_arith(df, a1)))),
                DateFunc::DateAdd(part, a1, a2) => AE::Func(DateFunc::DateAdd(part.clone(), Box::new(qualify_having_arith(df, a1)), Box::new(qualify_having_arith(df, a2)))),
                DateFunc::DateDiff(part, a1, a2) => AE::Func(DateFunc::DateDiff(part.clone(), Box::new(qualify_having_arith(df, a1)), Box::new(qualify_having_arith(df, a2)))),
            }
        }
        AE::Slice { base, start, stop, step } => {            
            let qbase = Box::new(qualify_having_arith(df, base));
            let qstart = match start {
                Some(StrSliceBound::Pattern { expr, include }) => Some(StrSliceBound::Pattern { expr: Box::new(qualify_having_arith(df, expr)), include: *include }),
                Some(other) => Some(other.clone()),
                None => None,
            };
            let qstop = match stop {
                Some(StrSliceBound::Pattern { expr, include }) => Some(StrSliceBound::Pattern { expr: Box::new(qualify_having_arith(df, expr)), include: *include }),
                Some(other) => Some(other.clone()),
                None => None,
            };
            AE::Slice { base: qbase, start: qstart, stop: qstop, step: *step }
        }
        AE::Predicate(w) => AE::Predicate(Box::new(qualify_having_where(df, w))),
        AE::Case { when_clauses, else_expr } => {
            let qualified_when = when_clauses.iter().map(|(cond, val)| {
                (qualify_having_where(df, cond), qualify_having_arith(df, val))
            }).collect();
            let qualified_else = else_expr.as_ref().map(|e| Box::new(qualify_having_arith(df, e)));
            AE::Case { when_clauses: qualified_when, else_expr: qualified_else }
        }
    }
}
fn qualify_having_where(df: &DataFrame, w: &WhereExpr) -> WhereExpr {    
    match w {
        WE::Comp { left, op, right } => WE::Comp { left: qualify_having_arith(df, left), op: op.clone(), right: qualify_having_arith(df, right) },
        WE::And(a, b) => WE::And(Box::new(qualify_having_where(df, a)), Box::new(qualify_having_where(df, b))),
        WE::Or(a, b) => WE::Or(Box::new(qualify_having_where(df, a)), Box::new(qualify_having_where(df, b))),
        WE::IsNull { expr, negated } => WE::IsNull { expr: qualify_having_arith(df, expr), negated: *negated },
        WE::Exists { negated, subquery } => WE::Exists { negated: *negated, subquery: subquery.clone() },
        WE::All { left, op, subquery, negated } => WE::All { left: qualify_having_arith(df, left), op: op.clone(), subquery: subquery.clone(), negated: *negated },
        WE::Any { left, op, subquery, negated } => WE::Any { left: qualify_having_arith(df, left), op: op.clone(), subquery: subquery.clone(), negated: *negated },
    }
}

/// Validate that HAVING references only final SELECT output columns and known UDFs
pub fn validate_having_refs(df: &DataFrame, h: &WhereExpr) -> Result<()> {
    // Diagnostics: entry into HAVING validation
    tprintln!(
        "[HAVING] validate: rows={} cols={:?} expr={:?}",
        df.height(),
        df.get_column_names(),
        h
    );
    // UDFs check first: error semantics in tests expect missing-UDF to surface before column errors
    let mut udf_names: Vec<String> = Vec::new();
    collect_udf_names_where(h, &mut udf_names);
    if let Some(reg) = get_script_registry() {
        for n in udf_names {
            if !reg.has_function(&n) {
                tprintln!("[HAVING] validate: missing UDF '{}' detected", n);
                anyhow::bail!(format!("UDF '{}' not found in HAVING clause", n));
            }
        }
    } else if !udf_names.is_empty() {
        // Tests expect clause-specific error even if registry is not initialized
        let n = udf_names.into_iter().next().unwrap();
        tprintln!("[HAVING] validate: registry not initialized; missing UDF '{}'", n);
        anyhow::bail!(format!("UDF '{}' not found in HAVING clause", n));
    }
    // Columns check (against current DF output); allow suffix match to support unqualified references
    let mut cols: Vec<String> = Vec::new();
    collect_where_columns(h, &mut cols);
    let existing = df.get_column_names();
    tprintln!("[HAVING] validate: referenced columns={:?}", cols);
    'outer: for c in cols {
        for ex in &existing {
            // HAVING must reference final projection labels exactly (no suffix matching)
            if ex.as_str() == c.as_str() { continue 'outer; }
        }
        tprintln!(
            "[HAVING] validate: column '{}' not found in final projection; existing={:?}",
            c,
            existing
        );
        return Err(crate::server::data_context::DataContext::column_not_found_error(&c, "HAVING", df));
    }
    tprintln!("[HAVING] validate: OK");
    Ok(())
}

/// Apply HAVING with validation, preferring eager application when supported
pub fn apply_having_with_validation(mut df: DataFrame, h: &WhereExpr, ctx: &crate::server::data_context::DataContext) -> Result<DataFrame> {
    tprintln!(
        "[HAVING] apply: start rows={} cols={:?} expr={:?}",
        df.height(),
        df.get_column_names(),
        h
    );
    validate_having_refs(&df, h)?;
    // Validate UDF presence in HAVING expressions
    if let Some(reg) = crate::scripts::get_script_registry() {
        let mut udf_names: Vec<String> = Vec::new();
        collect_udf_names_where(h, &mut udf_names);
        for n in udf_names {
            if !reg.has_function(&n) {
                anyhow::bail!("UDF '{}' not found in HAVING clause", n);
            }
        }
    }
    // Qualify names in the HAVING expression against the current DF so unqualified/suffix names resolve
    let qh = qualify_having_where(&df, h);
    tprintln!("[HAVING] apply: qualified expr={:?}", qh);

    // DIAGNOSTIC: materialize the boolean mask and display its summary alongside the
    // referenced columns to understand why a filter may yield zero rows.
    // We resolve the column names used in the predicate against the current DF
    // and show a small preview of their values next to the mask.
    {
        let mut ref_cols: Vec<String> = Vec::new();
        collect_where_columns(&qh, &mut ref_cols);
        // De-duplicate while preserving order
        let mut seen = std::collections::HashSet::new();
        ref_cols.retain(|c| seen.insert(c.clone()));
        // Resolve names present in DF (best-effort)
        let mut resolved_cols: Vec<String> = Vec::new();
        for c in &ref_cols {
            if let Ok(q) = resolve_name_in_df(&df, c) { resolved_cols.push(q) }
        }
        let mut sel: Vec<Expr> = Vec::with_capacity(1 + resolved_cols.len());
        sel.push(build_where_expr(&qh, ctx).alias("__m__"));
        for c in &resolved_cols { sel.push(col(c.as_str())); }
        if let Ok(mask_df) = df.clone().lazy().select(sel).collect() {
            // Count number of true values in mask
            let mut trues = 0usize;
            if let Ok(ca) = mask_df.column("__m__").and_then(|s| s.bool()) {
                for v in ca.into_iter().flatten() { if v { trues += 1; } }
            }
            tprintln!(
                "[HAVING] apply: mask diagnostics -> rows={} true_count={} shown_cols={:?}\n{}",
                mask_df.height(), trues, resolved_cols, mask_df
            );
        } else {
            tprintln!("[HAVING] apply: mask diagnostics unavailable (select failed)");
        }
    }
    if let Some(f) = try_apply_having_eager(&df, &qh)? {
        tprintln!("[HAVING] apply: using eager path");
        df = f;
    } else {
        tprintln!("[HAVING] apply: using lazy filter path");
        df = df.lazy().filter(build_where_expr(&qh, ctx)).collect()?;
    }
    tprintln!(
        "[HAVING] apply: done rows={} cols={:?}",
        df.height(),
        df.get_column_names()
    );
    Ok(df)
}

/// Returns Some(filtered_df) if eager application is supported; otherwise None to fallback to lazy engine
pub fn try_apply_having_eager(df: &DataFrame, _h: &WhereExpr) -> Result<Option<DataFrame>> {
    // Currently disabled: prefer general lazy filtering for robustness across dtypes
    tprintln!(
        "[HAVING] eager: disabled (passthrough to lazy); rows={} cols={:?}",
        df.height(),
        df.get_column_names()
    );
    Ok(None)
}

// --- helpers (local) ---
// Minimal UDF name collector mirroring exec_select.rs behavior for HAVING validation
fn collect_udf_names_arith(a: &ArithExpr, out: &mut Vec<String>) {    
    match a {
        AE::Call { name, args } => { out.push(name.clone()); for x in args { collect_udf_names_arith(x, out); } },
        AE::BinOp { left, right, .. } => { collect_udf_names_arith(left, out); collect_udf_names_arith(right, out); },
        AE::Concat(parts) => { for p in parts { collect_udf_names_arith(p, out); } },
        _ => {}
    }
}
fn collect_udf_names_where(w: &WhereExpr, out: &mut Vec<String>) {    
    match w {
        WE::Comp { left, right, .. } => { collect_udf_names_arith(left, out); collect_udf_names_arith(right, out); }
        WE::And(a, b) | WE::Or(a, b) => { collect_udf_names_where(a, out); collect_udf_names_where(b, out); }
        WE::IsNull { expr, .. } => { collect_udf_names_arith(expr, out); }
        WE::Exists { subquery, .. } => {
            // Collect UDFs from subquery's WHERE clause
            if let Some(w) = &subquery.where_clause {
                collect_udf_names_where(w, out);
            }
        }
        WE::All { left, subquery, .. } | WE::Any { left, subquery, .. } => {
            // Collect UDFs from left expression
            collect_udf_names_arith(left, out);
            // Collect UDFs from subquery's WHERE clause
            if let Some(w) = &subquery.where_clause {
                collect_udf_names_where(w, out);
            }
        }
    }
}
