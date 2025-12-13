//! Shared helpers to evaluate WHERE expressions that contain subqueries (EXISTS/ANY/ALL)
use anyhow::Result;
use polars::prelude::*;

use crate::server::query::query_common::Query;
use crate::server::query::query_common::WhereExpr;
use crate::server::query::query_common::CompOp;
use crate::server::query::query_common::ArithExpr as AE;
use crate::server::query::query_common::ArithTerm as AT;
use crate::server::query::query_common::WhereExpr as WE;
use crate::server::data_context::DataContext;
use crate::storage::SharedStore;
use crate::server::exec::exec_common::{build_where_expr};
use crate::server::exec::exec_common::build_arith_expr as build_arith_expr_public;
use crate::server::exec::internal::constants::{TMP_BOOL_ALIAS, TMP_LEFT_ALIAS};
use crate::server::exec::exec_select::run_select_with_context;

/// Returns true if the WHERE expression contains any subquery operator.
pub(crate) fn where_contains_subquery(w: &WhereExpr) -> bool {
    match w {
        WhereExpr::Exists { .. } | WhereExpr::All { .. } | WhereExpr::Any { .. } => true,
        WhereExpr::And(a, b) | WhereExpr::Or(a, b) => where_contains_subquery(a) || where_contains_subquery(b),
        WhereExpr::Comp { .. } | WhereExpr::IsNull { .. } => false,
    }
}

/// Evaluate a WHERE expression into a boolean mask Series over df, with support for subqueries.
/// The expression should already be qualified if needed by the caller.
pub(crate) fn eval_where_mask(df: &DataFrame, ctx: &DataContext, store: &SharedStore, w: &WhereExpr) -> Result<BooleanChunked> {
    use crate::server::query::WhereExpr as WE;
    match w {
        WE::And(a, b) => {
            // If neither child contains subqueries, delegate the entire AND to Polars
            if !where_contains_subquery(a) && !where_contains_subquery(b) {
                let expr = build_where_expr(w, ctx).alias(TMP_BOOL_ALIAS);
                let mdf = df.clone().lazy().select([expr]).collect()?;
                Ok(mdf.column(TMP_BOOL_ALIAS)?.bool()?.clone())
            } else {
                let la = eval_where_mask(df, ctx, store, a)?;
                let lb = eval_where_mask(df, ctx, store, b)?;
                // Combine boolean masks with SQL/Polars semantics: null treated as false in filters
                let len = la.len().max(lb.len());
                let iter = (0..len).map(|i| {
                    let av = la.get(i).unwrap_or(false);
                    let bv = lb.get(i).unwrap_or(false);
                    Some(av && bv)
                });
                Ok(BooleanChunked::from_iter_options("".into(), iter))
            }
        }
        WE::Or(a, b) => {
            // If neither child contains subqueries, delegate the entire OR to Polars
            if !where_contains_subquery(a) && !where_contains_subquery(b) {
                let expr = build_where_expr(w, ctx).alias(TMP_BOOL_ALIAS);
                let mdf = df.clone().lazy().select([expr]).collect()?;
                Ok(mdf.column(TMP_BOOL_ALIAS)?.bool()?.clone())
            } else {
                let la = eval_where_mask(df, ctx, store, a)?;
                let lb = eval_where_mask(df, ctx, store, b)?;
                let len = la.len().max(lb.len());
                let iter = (0..len).map(|i| {
                    let av = la.get(i).unwrap_or(false);
                    let bv = lb.get(i).unwrap_or(false);
                    Some(av || bv)
                });
                Ok(BooleanChunked::from_iter_options("".into(), iter))
            }
        }
        WE::Comp { .. } | WE::IsNull { .. } => {
            // Delegate to Polars for simple predicates
            let expr = build_where_expr(w, ctx).alias(TMP_BOOL_ALIAS);
            let mdf = df.clone().lazy().select([expr]).collect()?;
            Ok(mdf.column(TMP_BOOL_ALIAS)?.bool()?.clone())
        }
        WE::Exists { negated, subquery } => {
            // For each row, execute correlated subquery and check non-empty
            let mut out: Vec<Option<bool>> = Vec::with_capacity(df.height());
            for i in 0..df.height() {
                let sq = substitute_outer_refs_in_query(df, i, subquery, ctx)?;
                let sq_df = run_select_with_context(store, &sq, Some(ctx)).unwrap_or(DataFrame::new(Vec::new())?);
                eprintln!("[EXISTS dbg] row={} subquery_sql='{}' rows={}", i, sq.original_sql, sq_df.height());
                let mut val = sq_df.height() > 0;
                if *negated { val = !val; }
                out.push(Some(val));
            }
            Ok(BooleanChunked::from_iter_options("".into(), out.into_iter()))
        }
        WE::Any { left, op, subquery, negated } | WE::All { left, op, subquery, negated } => {
            // Precompute left values for all rows
            let ldf = df.clone().lazy().select([build_arith_expr_public(left, ctx).alias(TMP_LEFT_ALIAS)]).collect()?;
            // Evaluate per row with correlated subquery
            let is_all = matches!(w, WE::All { .. });
            let mut out: Vec<Option<bool>> = Vec::with_capacity(df.height());
            for i in 0..df.height() {
                let lhs = ldf.column(TMP_LEFT_ALIAS)?.get(i)?;
                // Build and run correlated subquery for this row
                let sq = substitute_outer_refs_in_query(df, i, subquery, ctx)?;
                let sq_df = run_select_with_context(store, &sq, Some(ctx)).unwrap_or(DataFrame::new(Vec::new())?);
                // Pull first non-_time column as the value list
                let sub_vals: Vec<polars::prelude::AnyValue> = if sq_df.width() == 0 || sq_df.height() == 0 {
                    Vec::new()
                } else {
                    let names = sq_df.get_column_names();
                    let col_name_ref = names.iter().find(|n| n.as_str() != "_time").unwrap_or(&names[0]);
                    let col_name = col_name_ref.clone();
                    let s = sq_df.column(col_name.as_str())?;
                    let mut vals: Vec<polars::prelude::AnyValue> = Vec::with_capacity(s.len());
                    for j in 0..s.len() { if let Ok(v) = s.get(j) { vals.push(v); } }
                    vals
                };

                let res = eval_any_all(&lhs, op.clone(), &sub_vals, is_all);
                let res = if *negated { Some(!res.unwrap_or(false)) } else { res };
                out.push(res);
            }
            Ok(BooleanChunked::from_iter_options("".into(), out.into_iter()))
        }
    }
}

fn eval_any_all(lhs: &polars::prelude::AnyValue, op: CompOp, vals: &Vec<polars::prelude::AnyValue>, is_all: bool) -> Option<bool> {
    // SQL semantics:
    // ANY: true if any comparison is true; false if none true (empty -> false)
    // ALL: true if all comparisons are true; true for empty set; false if any false
    if vals.is_empty() { return Some(!is_all); /* ANY:false, ALL:true */ }
    let mut saw_null = false;
    let mut any_true = false;
    let mut any_false = false;
    for v in vals {
        match compare_anyvalue(lhs, &op, v) {
            Some(true) => any_true = true,
            Some(false) => any_false = true,
            None => saw_null = true,
        }
        if !is_all && any_true { return Some(true); }
        if is_all && any_false { return Some(false); };
    }
    if is_all {
        if any_false { Some(false) } else if saw_null { Some(false) } else { Some(true) }
    } else if any_true { Some(true) } else { Some(false) }
}

fn compare_anyvalue(a: &polars::prelude::AnyValue, op: &CompOp, b: &polars::prelude::AnyValue) -> Option<bool> {
    
    use polars::prelude::AnyValue as AV;
    match (a, b) {
        (AV::Null, _) | (_, AV::Null) => None,
        (AV::Int64(ai), AV::Int64(bi)) => cmp_f(*ai as f64, op, *bi as f64),
        (AV::Float64(af), AV::Float64(bf)) => cmp_f(*af, op, *bf),
        (AV::Int64(ai), AV::Float64(bf)) => cmp_f(*ai as f64, op, *bf),
        (AV::Float64(af), AV::Int64(bi)) => cmp_f(*af, op, *bi as f64),
        (AV::String(asv), AV::String(bsv)) => cmp_s(asv, op, bsv),
        (AV::StringOwned(asv), AV::StringOwned(bsv)) => cmp_s(asv, op, bsv),
        (AV::String(asv), AV::StringOwned(bsv)) => cmp_s(asv, op, bsv),
        (AV::StringOwned(asv), AV::String(bsv)) => cmp_s(asv, op, bsv),
        // Fallback: try string compare
        (x, y) => {
            let xs = x.to_string();
            let ys = y.to_string();
            cmp_s(&xs, op, &ys)
        }
    }
}

fn cmp_f(a: f64, op: &CompOp, b: f64) -> Option<bool> {    
    Some(match *op { CompOp::Gt => a > b, CompOp::Ge => a >= b, CompOp::Lt => a < b, CompOp::Le => a <= b, CompOp::Eq => (a - b).abs() < f64::EPSILON, CompOp::Ne => (a - b).abs() >= f64::EPSILON, CompOp::Like | CompOp::NotLike => false })
}

fn cmp_s(a: &str, op: &CompOp, b: &str) -> Option<bool> {    
    Some(match *op {
        CompOp::Eq => a == b,
        CompOp::Ne => a != b,
        CompOp::Gt => a > b,
        CompOp::Ge => a >= b,
        CompOp::Lt => a < b,
        CompOp::Le => a <= b,
        CompOp::Like | CompOp::NotLike => false,
    })
}

/// Produce a cloned subquery where any column reference that resolves to an outer df column
/// is replaced with a literal value from row i (for correlated subqueries).
pub(crate) fn substitute_outer_refs_in_query(df: &DataFrame, row_idx: usize, sub: &Query, ctx: &DataContext) -> anyhow::Result<Query> {    

    // Try to resolve a column reference coming from the subquery that actually
    // points to the OUTER query. The outer DataFrame typically carries
    // unqualified column names, while the subquery AST may keep qualified
    // identifiers such as "c.customer_id". This helper matches by exact name
    // first, and then by the last path segment (split by '.', '/', or '\\').
    fn resolve_outer_col_name(df: &DataFrame, name: &str, inner_aliases: &std::collections::HashSet<String>, outer_aliases: &std::collections::HashSet<String>) -> Option<String> {
        // Helper: try to find a unique column in df that ends with ".<suffix>"
        fn unique_suffix_match(df: &DataFrame, suffix: &str) -> Option<String> {
            let needle_dot = format!(".{}", suffix);
            let needle_fwd = format!("/{}", suffix);
            let needle_back = format!("\\{}", suffix);
            let matches: Vec<String> = df
                .get_column_names()
                .iter()
                .map(|c| c.as_str().to_string())
                .filter(|c| c.ends_with(&needle_dot) || c.ends_with(&needle_fwd) || c.ends_with(&needle_back))
                .collect();
            if matches.len() == 1 { Some(matches[0].clone()) } else { None }
        }

        // Check if name is qualified (alias.name or path-like)
        let qualifier = name
            .rsplit_once('.')
            .map(|(q, _)| q)
            .or_else(|| name.rsplit_once('/').map(|(q, _)| q))
            .or_else(|| name.rsplit_once('\\').map(|(q, _)| q));

        if let Some(q) = qualifier {
            // If qualifier matches an inner alias, do NOT substitute (it's an inner reference)
            if inner_aliases.contains(q) {
                return None;
            }
            // If qualifier matches an outer alias, substitute by suffix
            if outer_aliases.contains(q) {
                let suffix = name
                    .rsplit_once('.')
                    .map(|(_, s)| s)
                    .or_else(|| name.rsplit_once('/').map(|(_, s)| s))
                    .or_else(|| name.rsplit_once('\\').map(|(_, s)| s))
                    .unwrap_or(name);
                // Exact qualified name
                if df.get_column_names().iter().any(|n| n.as_str() == name) {
                    return Some(name.to_string());
                }
                // Exact unqualified suffix
                if df.get_column_names().iter().any(|n| n.as_str() == suffix) {
                    return Some(suffix.to_string());
                }
                // Try unique suffix by ".suffix" against fully-qualified df column names
                if let Some(m) = unique_suffix_match(df, suffix) { return Some(m); }
            } else {
                // Alias is neither known inner nor known outer: try a best-effort suffix match anyway.
                // This helps when the outer alias set wasn't propagated correctly but the DF still has the column.
                let suffix = name
                    .rsplit_once('.')
                    .map(|(_, s)| s)
                    .or_else(|| name.rsplit_once('/').map(|(_, s)| s))
                    .or_else(|| name.rsplit_once('\\').map(|(_, s)| s))
                    .unwrap_or(name);
                if df.get_column_names().iter().any(|n| n.as_str() == suffix) {
                    return Some(suffix.to_string());
                }
                if let Some(m) = unique_suffix_match(df, suffix) { return Some(m); }
            }
        } else {
            // Unqualified name: try exact first
            if df.get_column_names().iter().any(|n| n.as_str() == name) {
                return Some(name.to_string());
            }
            // Try unique suffix match (columns are often stored as fully qualified paths)
            if let Some(m) = unique_suffix_match(df, name) { return Some(m); }
        }
        // No match found
        None
    }

    fn subst_arith(df: &DataFrame, row_idx: usize, a: &AE, inner_aliases: &std::collections::HashSet<String>, outer_aliases: &std::collections::HashSet<String>) -> AE {
        match a {
            AE::Term(AT::Col { name, previous: false }) => {
                // If this column exists in outer df (possibly by suffix),
                // capture its current-row value as literal for correlation
                if let Some(resolved) = resolve_outer_col_name(df, name.as_str(), inner_aliases, outer_aliases) {
                    eprintln!("[CORRELATE dbg] substituting outer ref '{}' -> df column '{}' (row={})", name, resolved, row_idx);
                    if let Ok(s) = df.column(resolved.as_str()) {
                        if let Ok(v) = s.get(row_idx) {
                            return match v {
                                polars::prelude::AnyValue::Null => AE::Term(AT::Null),
                                polars::prelude::AnyValue::Int64(i) => AE::Term(AT::Number(i as f64)),
                                polars::prelude::AnyValue::Float64(f) => AE::Term(AT::Number(f)),
                                polars::prelude::AnyValue::String(s) => AE::Term(AT::Str(s.to_string())),
                                polars::prelude::AnyValue::StringOwned(s) => AE::Term(AT::Str(s.to_string())),
                                _ => AE::Term(AT::Null),
                            };
                        }
                    }
                }
                a.clone()
            }
            AE::Term(_) => a.clone(),
            AE::Cast { expr, ty } => AE::Cast { expr: Box::new(subst_arith(df, row_idx, expr, inner_aliases, outer_aliases)), ty: ty.clone() },
            AE::BinOp { left, op, right } => AE::BinOp { left: Box::new(subst_arith(df, row_idx, left, inner_aliases, outer_aliases)), op: op.clone(), right: Box::new(subst_arith(df, row_idx, right, inner_aliases, outer_aliases)) },
            AE::Func(f) => AE::Func(f.clone()),
            AE::Slice { base, start, stop, step } => AE::Slice { base: Box::new(subst_arith(df, row_idx, base, inner_aliases, outer_aliases)), start: start.clone(), stop: stop.clone(), step: *step },
            AE::Concat(parts) => AE::Concat(parts.iter().map(|p| subst_arith(df, row_idx, p, inner_aliases, outer_aliases)).collect()),
            AE::Call { name, args } => AE::Call { name: name.clone(), args: args.iter().map(|p| subst_arith(df, row_idx, p, inner_aliases, outer_aliases)).collect() },
            AE::Predicate(w) => AE::Predicate(Box::new(subst_where(df, row_idx, w, inner_aliases, outer_aliases))),
            AE::Case { when_clauses, else_expr } => AE::Case { when_clauses: when_clauses.iter().map(|(c,v)| (subst_where(df, row_idx, c, inner_aliases, outer_aliases), subst_arith(df, row_idx, v, inner_aliases, outer_aliases))).collect(), else_expr: else_expr.as_ref().map(|e| Box::new(subst_arith(df, row_idx, e, inner_aliases, outer_aliases))).clone() },
        }
    }
    fn subst_where(df: &DataFrame, row_idx: usize, w: &WE, inner_aliases: &std::collections::HashSet<String>, outer_aliases: &std::collections::HashSet<String>) -> WE {
        match w {
            WE::Comp { left, op, right } => WE::Comp { left: subst_arith(df, row_idx, left, inner_aliases, outer_aliases), op: op.clone(), right: subst_arith(df, row_idx, right, inner_aliases, outer_aliases) },
            WE::And(a, b) => WE::And(Box::new(subst_where(df, row_idx, a, inner_aliases, outer_aliases)), Box::new(subst_where(df, row_idx, b, inner_aliases, outer_aliases))),
            WE::Or(a, b) => WE::Or(Box::new(subst_where(df, row_idx, a, inner_aliases, outer_aliases)), Box::new(subst_where(df, row_idx, b, inner_aliases, outer_aliases))),
            WE::IsNull { expr, negated } => WE::IsNull { expr: subst_arith(df, row_idx, expr, inner_aliases, outer_aliases), negated: *negated },
            WE::Exists { negated, subquery } => WE::Exists { negated: *negated, subquery: Box::new(subst_query(df, row_idx, subquery, inner_aliases, outer_aliases)) },
            WE::All { left, op, subquery, negated } => WE::All { left: subst_arith(df, row_idx, left, inner_aliases, outer_aliases), op: op.clone(), subquery: Box::new(subst_query(df, row_idx, subquery, inner_aliases, outer_aliases)), negated: *negated },
            WE::Any { left, op, subquery, negated } => WE::Any { left: subst_arith(df, row_idx, left, inner_aliases, outer_aliases), op: op.clone(), subquery: Box::new(subst_query(df, row_idx, subquery, inner_aliases, outer_aliases)), negated: *negated },
        }
    }
    fn subst_query(df: &DataFrame, row_idx: usize, q: &Query, inner_aliases: &std::collections::HashSet<String>, outer_aliases: &std::collections::HashSet<String>) -> Query {
        let mut out = q.clone();
        if let Some(w) = &q.where_clause { out.where_clause = Some(subst_where(df, row_idx, w, inner_aliases, outer_aliases)); }
        // project expressions
        for si in &mut out.select {
            if let Some(expr) = &si.expr { si.expr = Some(subst_arith(df, row_idx, expr, inner_aliases, outer_aliases)); }
        }
        out
    }

    // Collect inner aliases present in the subquery so we don't hijack them
    let mut inner_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(bt) = &sub.base_table {
        if let Some(a) = bt.alias() { inner_aliases.insert(a.to_string()); }
    }
    if let Some(joins) = &sub.joins { for j in joins { if let Some(a) = j.right.alias() { inner_aliases.insert(a.to_string()); } } }

    // Collect outer aliases for proper correlation.
    // IMPORTANT: ctx here is the OUTER query's context (before subquery execution),
    // so we use ctx.sources which contains the outer query's tables.
    // ctx.parent_sources would be used if we were already inside a subquery context.
    let mut outer_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    
    // First, check if we have parent_sources (for nested subqueries)
    for src in &ctx.parent_sources {
        if let Some(a) = src.alias() {
            outer_aliases.insert(a.to_string());
        }
        if let Some(n) = src.table_name() {
            outer_aliases.insert(n.to_string());
        }
    }
    
    // Also include ctx.sources (the immediate outer query level)
    for src in &ctx.sources {
        if let Some(a) = src.alias() {
            outer_aliases.insert(a.to_string());
        }
        if let Some(n) = src.table_name() {
            outer_aliases.insert(n.to_string());
        }
    }
    eprintln!("[CORRELATE dbg] row={} inner_aliases={:?} outer_aliases={:?} df_cols={:?}", row_idx, inner_aliases, outer_aliases, df.get_column_names());
    
    let newq = subst_query(df, row_idx, sub, &inner_aliases, &outer_aliases);
    Ok(newq)
}
