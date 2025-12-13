//! SELECT projection stage
//! Handles non-aggregated projections (wildcard, passthrough, string funcs, arithmetic/UDF exprs)
//! and simple aggregate projections when no BY/GROUP BY is present.

use anyhow::Result;
use polars::prelude::*;

use crate::server::data_context::{DataContext, SelectStage};
use crate::server::query::query_common::Query;
use crate::server::query::query_common::WhereExpr;
use crate::server::query::query_common::AggFunc;
use crate::server::query::query_common::StrFunc;
use crate::server::query::query_common::ArithExpr as AE;
use crate::server::query::query_common::ArithTerm as AT;
use crate::server::query::query_common::WhereExpr as WE;
use crate::server::query::query_common::ArithExpr;
use crate::server::query::query_common::DateFunc;
use crate::server::query::query_common::WindowFunc;
use crate::server::query::query_common::TableRef;
use crate::server::query::query_common::StrSliceBound;
use crate::server::exec::exec_common::build_arith_expr;
use crate::server::exec::internal::constants::{ARG_PREFIX, WINDOW_ORDER_PREFIX};
use crate::scripts::get_script_registry;

pub fn project_select(df: DataFrame, q: &Query, ctx: &mut DataContext) -> Result<DataFrame> {
    // Clause-aware validation for SELECT projection against incoming DataFrame
    
    // Helpers to collect column and UDF references from expressions
    fn collect_cols_arith(a: &ArithExpr, out: &mut Vec<String>) {        
        match a {
            AE::Term(AT::Col { name, previous }) => { if !*previous { out.push(name.clone()); } },
            AE::Cast { expr, .. } => { collect_cols_arith(expr, out); },
            AE::BinOp { left, right, .. } => { collect_cols_arith(left, out); collect_cols_arith(right, out); },
            AE::Concat(parts) => { for p in parts { collect_cols_arith(p, out); } },
            AE::Func(_) => {},
            AE::Slice { base, start: _, stop: _, step: _ } => { collect_cols_arith(base, out); },
            AE::Predicate(w) => { crate::server::exec::exec_common::collect_where_columns(w, out); },
            AE::Call { args, .. } => { for a in args { collect_cols_arith(a, out); } },
            AE::Case { when_clauses, else_expr } => {
                for (cond, val) in when_clauses {
                    crate::server::exec::exec_common::collect_where_columns(cond, out);
                    collect_cols_arith(val, out);
                }
                if let Some(e) = else_expr { collect_cols_arith(e, out); }
            },
            AE::Term(_) => {},
        }
    }
    fn collect_udf_names_arith(a: &ArithExpr, out: &mut Vec<String>) {        
        match a {
            AE::Call { name, args } => { out.push(name.clone()); for x in args { collect_udf_names_arith(x, out); } },
            AE::Cast { expr, .. } => { collect_udf_names_arith(expr, out); },
            AE::BinOp { left, right, .. } => { collect_udf_names_arith(left, out); collect_udf_names_arith(right, out); },
            AE::Concat(parts) => { for p in parts { collect_udf_names_arith(p, out); } },
            AE::Slice { base, .. } => { collect_udf_names_arith(base, out); },
            AE::Predicate(w) => { match w.as_ref() { _ => {} } },
            AE::Case { when_clauses, else_expr } => {
                for (_cond, val) in when_clauses {
                    collect_udf_names_arith(val, out);
                }
                if let Some(e) = else_expr { collect_udf_names_arith(e, out); }
            },
            _ => {}
        }
    }
    // Helper to resolve column name using DataContext (supports unqualified/suffix matches)
    fn resolve_col_name_ctx(df: &DataFrame, ctx: &DataContext, name: &str) -> anyhow::Result<String> {
        ctx.resolve_column_at_stage(df, name, SelectStage::ProjectSelect)
    }
    // Qualify arithmetic expressions against current DF/Context
    fn qualify_arith_ctx(df: &DataFrame, ctx: &DataContext, a: &ArithExpr, clause: &str) -> anyhow::Result<ArithExpr> {        
        Ok(match a {
            AE::Term(AT::Col { name, previous }) => {
                let qn = resolve_col_name_ctx(df, ctx, name).map_err(|_| crate::server::data_context::DataContext::column_not_found_error(name, clause, df))?;
                AE::Term(AT::Col { name: qn, previous: *previous })
            }
            AE::Term(_) => a.clone(),
            AE::Cast { expr, ty } => {
                AE::Cast { expr: Box::new(qualify_arith_ctx(df, ctx, expr, clause)?), ty: ty.clone() }
            }
            AE::BinOp { left, op, right } => AE::BinOp { left: Box::new(qualify_arith_ctx(df, ctx, left, clause)?), op: op.clone(), right: Box::new(qualify_arith_ctx(df, ctx, right, clause)?) },
            AE::Concat(parts) => AE::Concat(parts.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()?),
            AE::Call { name, args } => AE::Call { name: name.clone(), args: args.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()? },
            AE::Func(dfm) => {
                use DateFunc;
                match dfm {
                    DateFunc::DatePart(part, a1) => AE::Func(DateFunc::DatePart(part.clone(), Box::new(qualify_arith_ctx(df, ctx, a1, clause)?))),
                    DateFunc::DateAdd(part, a1, a2) => AE::Func(DateFunc::DateAdd(part.clone(), Box::new(qualify_arith_ctx(df, ctx, a1, clause)?), Box::new(qualify_arith_ctx(df, ctx, a2, clause)?))),
                    DateFunc::DateDiff(part, a1, a2) => AE::Func(DateFunc::DateDiff(part.clone(), Box::new(qualify_arith_ctx(df, ctx, a1, clause)?), Box::new(qualify_arith_ctx(df, ctx, a2, clause)?))),
                }
            }
            AE::Slice { base, start, stop, step } => {                
                let qbase = Box::new(qualify_arith_ctx(df, ctx, base, clause)?);
                let qstart = match start {
                    Some(StrSliceBound::Pattern { expr, include }) => Some(StrSliceBound::Pattern { expr: Box::new(qualify_arith_ctx(df, ctx, expr, clause)?), include: *include }),
                    Some(other) => Some(other.clone()),
                    None => None,
                };
                let qstop = match stop {
                    Some(StrSliceBound::Pattern { expr, include }) => Some(StrSliceBound::Pattern { expr: Box::new(qualify_arith_ctx(df, ctx, expr, clause)?), include: *include }),
                    Some(other) => Some(other.clone()),
                    None => None,
                };
                AE::Slice { base: qbase, start: qstart, stop: qstop, step: *step }
            }
            AE::Predicate(w) => AE::Predicate(Box::new(qualify_where_ctx(df, ctx, w, clause)?)),
            AE::Case { when_clauses, else_expr } => {
                let qualified_when = when_clauses.iter().map(|(cond, val)| {
                    Ok((qualify_where_ctx(df, ctx, cond, clause)?, qualify_arith_ctx(df, ctx, val, clause)?))
                }).collect::<anyhow::Result<Vec<_>>>()?;
                let qualified_else = else_expr.as_ref().map(|e| qualify_arith_ctx(df, ctx, e, clause)).transpose()?;
                AE::Case { when_clauses: qualified_when, else_expr: qualified_else.map(Box::new) }
            }
        })
    }
    fn qualify_where_ctx(df: &DataFrame, ctx: &DataContext, w: &WhereExpr, clause: &str) -> anyhow::Result<WhereExpr> {        
        Ok(match w {
            WE::Comp { left, op, right } => WE::Comp { left: qualify_arith_ctx(df, ctx, left, clause)?, op: op.clone(), right: qualify_arith_ctx(df, ctx, right, clause)? },
            WE::And(a, b) => WE::And(Box::new(qualify_where_ctx(df, ctx, a, clause)?), Box::new(qualify_where_ctx(df, ctx, b, clause)?)),
            WE::Or(a, b) => WE::Or(Box::new(qualify_where_ctx(df, ctx, a, clause)?), Box::new(qualify_where_ctx(df, ctx, b, clause)?)),
            WE::IsNull { expr, negated } => WE::IsNull { expr: qualify_arith_ctx(df, ctx, expr, clause)?, negated: *negated },
            WE::Exists { negated, subquery } => WE::Exists { negated: *negated, subquery: subquery.clone() },
            WE::All { left, op, subquery, negated } => WE::All { left: qualify_arith_ctx(df, ctx, left, clause)?, op: op.clone(), subquery: subquery.clone(), negated: *negated },
            WE::Any { left, op, subquery, negated } => WE::Any { left: qualify_arith_ctx(df, ctx, left, clause)?, op: op.clone(), subquery: subquery.clone(), negated: *negated },
        })
    }

    // If BY/GROUP BY/SLICE or ROLLING already computed aggregations, don't recompute.
    // Instead, apply aliases to existing aggregate columns (function-form names) and passthrough.
    if q.by_window_ms.is_some() || q.group_by_cols.is_some() || q.rolling_window_ms.is_some() || q.by_slices.is_some() {
        // Build a mutable copy to apply renames for aliases
        let mut out = df.clone();
        for item in &q.select {
            // 1) Built-in aggregate functions: apply alias to function-form name
            if let Some(func) = &item.func {
                if let Some(alias) = &item.alias {
                    // Expected existing name in BY/GROUP stage uses function-form over item.column
                    let src_name = match func {
                        AggFunc::Avg => format!("AVG({})", item.column),
                        AggFunc::Max => format!("MAX({})", item.column),
                        AggFunc::Min => format!("MIN({})", item.column),
                        AggFunc::Sum => format!("SUM({})", item.column),
                        AggFunc::Count => if item.column == "*" { "COUNT(*)".to_string() } else { format!("COUNT({})", item.column) },
                        AggFunc::First => format!("FIRST({})", item.column),
                        AggFunc::Last => format!("LAST({})", item.column),
                        AggFunc::Stdev => format!("STDEV({})", item.column),
                        AggFunc::Delta => format!("DELTA({})", item.column),
                        AggFunc::Height => format!("HEIGHT({})", item.column),
                        AggFunc::Gradient => format!("GRADIENT({})", item.column),
                        AggFunc::Quantile(qp) => format!("_{}_QUANTILE({})", qp, item.column),
                        AggFunc::ArrayAgg => format!("ARRAY_AGG({})", item.column),
                    };
                    if out.get_column_names().iter().any(|c| c.as_str() == src_name) {
                        // Attempt to rename; if DataFrame::rename is unavailable, rebuild column
                        if let Err(_e) = out.rename(&src_name, alias.clone().into()) {
                            // Fallback: replace by taking the series and renaming it, then updating df
                            if let Ok(s) = out.column(&src_name).cloned() {
                                let mut ser = s.clone();
                                ser.rename(alias.clone().into());
                                // Remove old and add new
                                let cols = out.get_column_names();
                                let mut new_cols: Vec<Column> = Vec::with_capacity(cols.len());
                                for cname in cols {
                                    if cname.as_str() == src_name { new_cols.push(ser.clone()); }
                                    else { new_cols.push(out.column(cname).unwrap().clone()); }
                                }
                                out = DataFrame::new(new_cols)?;
                            }
                        }
                    }
                }
            }
            // 2) Aggregate UDFs: if aliased and outputs exist under function-name base, rename to alias base
            if let Some(ex) = &item.expr {
                if let ArithExpr::Call { name, .. } = ex {
                    if let Some(alias) = &item.alias {
                        if let Some(reg) = get_script_registry() {
                            if let Some(meta) = reg.get_meta(name) {
                                if matches!(meta.kind, crate::scripts::ScriptKind::Aggregate) {
                                    if meta.returns.len() <= 1 {
                                        // Single-return: function may have produced '<funcname>' base; attempt rename to alias
                                        let _ = out.rename(name.as_str(), alias.clone().into());
                                    } else {
                                        // Multi-return: rename '<funcname>_i' to '<alias>_i' when present
                                        for i in 0..meta.returns.len() {
                                            let from = format!("{}_{}", name, i);
                                            let to = format!("{}_{}", alias, i);
                                            let _ = out.rename(from.as_str(), to.into());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        ctx.register_df_columns_for_stage(SelectStage::ProjectSelect, &out);
        return Ok(out);
    }

    // Validate simple column items and expression references (using resolver)
    for item in &q.select {
        // Skip wildcard items here: '*' and qualified wildcards like 't.*' are handled by
        // the expansion logic below. Trying to resolve them as plain columns leads to
        // incorrect "Column not found" errors (e.g., for 't.*').
        let is_bare_wildcard = item.func.is_none()
            && item.str_func.is_none()
            && item.window_func.is_none()
            && item.expr.is_none()
            && item.column == "*";
        let is_qualified_wildcard = item.func.is_none()
            && item.str_func.is_none()
            && item.window_func.is_none()
            && item.expr.is_none()
            && item.column.ends_with(".*")
            && item.column != "*";

        if !(is_bare_wildcard || is_qualified_wildcard) {
            if item.func.is_none()
                && item.str_func.is_none()
                && item.window_func.is_none()
                && item.expr.is_none()
            {
                let needs_resolution = true;
                // If resolution fails but a wildcard in the SELECT can provide this column,
                // skip hard-failing at validation time. This covers cases like selecting
                // both 't.oid' and 't.*' where resolution ordering can be ambiguous.
                let mut provided_by_wildcard = false;
                // Detect if any bare '*' is present
                let has_bare = q.select.iter().any(|it| it.func.is_none() && it.str_func.is_none() && it.window_func.is_none() && it.expr.is_none() && it.column == "*");
                if has_bare { provided_by_wildcard = true; }
                // Detect if a qualified wildcard matching this qualifier exists
                if !provided_by_wildcard {
                    if let Some(dot_pos) = item.column.find('.') {
                        let qual = &item.column[..dot_pos];
                        let has_qual_wc = q.select.iter().any(|it| it.func.is_none() && it.str_func.is_none() && it.window_func.is_none() && it.expr.is_none() && it.column == format!("{}.*", qual));
                        if has_qual_wc { provided_by_wildcard = true; }
                    }
                }

                if needs_resolution {
                    match resolve_col_name_ctx(&df, ctx, &item.column) {
                        Ok(_) => { /* ok */ }
                        Err(_) if provided_by_wildcard => {
                            // Defer to wildcard expansion instead of erroring here
                            tracing::debug!(target: "clarium::exec", "SELECT validation: deferring resolution of '{}' due to wildcard presence", item.column);
                        }
                        Err(_) => {
                            return Err(crate::server::data_context::DataContext::column_not_found_error(
                                &item.column,
                                "SELECT",
                                &df,
                            ));
                        }
                    }
                }
            }
        }
        if let Some(ex) = &item.expr {
            let mut cols: Vec<String> = Vec::new();
            collect_cols_arith(ex, &mut cols);
            for c in cols { if resolve_col_name_ctx(&df, ctx, &c).is_err() { return Err(crate::server::data_context::DataContext::column_not_found_error(&c, "SELECT", &df)); } }
            // Do not hard-fail on missing UDFs during SELECT validation.
            // UDF existence and execution errors will be handled at evaluation time.
            if let Some(_reg) = get_script_registry() {
                let mut _udf_names: Vec<String> = Vec::new();
                collect_udf_names_arith(ex, &mut _udf_names);
                // Intentionally no eager bail-out here.
            }
        }
    }
    // If BY/GROUP BY or ROLLING already computed aggregations, pass-through
    if q.by_window_ms.is_some() || q.group_by_cols.is_some() || q.rolling_window_ms.is_some() {
        ctx.register_df_columns_for_stage(SelectStage::ProjectSelect, &df);
        return Ok(df);
    }

    let has_aggs = q.select.iter().any(|i| i.func.is_some());
    let _has_str = q.select.iter().any(|i| i.str_func.is_some());

    if has_aggs {
        // Aggregated projection without GROUP BY
        let lf = df.clone().lazy();
        let mut agg_cols: Vec<Expr> = Vec::new();
        for item in &q.select {
            if let Some(func) = &item.func {
                let base = if let Some(ex) = &item.expr { build_arith_expr(&qualify_arith_ctx(&df, ctx, ex, "SELECT")?, ctx) } else if matches!(func, AggFunc::Count) && item.column == "*" { lit(1) } else {
                    let qn = resolve_col_name_ctx(&df, ctx, &item.column).unwrap_or_else(|_| item.column.clone());
                    col(&qn)
                };
                let mut e = match func {
                    AggFunc::Avg => base.mean().alias(format!("AVG({})", item.column)),
                    AggFunc::Max => base.max().alias(format!("MAX({})", item.column)),
                    AggFunc::Min => base.min().alias(format!("MIN({})", item.column)),
                    AggFunc::Sum => base.sum().alias(format!("SUM({})", item.column)),
                    AggFunc::Count => {
                        if item.column == "*" { base.count().cast(DataType::Int64).alias("COUNT(*)") } else { base.count().cast(DataType::Int64).alias(format!("COUNT({})", item.column)) }
                    },
                    AggFunc::First => base.first().alias(format!("FIRST({})", item.column)),
                    AggFunc::Last => base.last().alias(format!("LAST({})", item.column)),
                    AggFunc::Stdev => base.std(1).alias(format!("STDEV({})", item.column)),
                    AggFunc::Delta => (base.clone().last() - base.first()).alias(format!("DELTA({})", item.column)),
                    AggFunc::Height => (base.clone().max() - base.min()).alias(format!("HEIGHT({})", item.column)),
                    AggFunc::Gradient => {
                        let num = base.clone().last() - base.first();
                        let time_col = resolve_col_name_ctx(&df, ctx, "_time").unwrap_or_else(|_| "_time".to_string());
                        let den = col(&time_col).max() - col(&time_col).min();
                        (num.cast(DataType::Float64) / den.cast(DataType::Float64)).alias(format!("GRADIENT({})", item.column))
                    }
                    AggFunc::Quantile(cutoff) => {
                        let p = (*cutoff as f64) / 100.0;
                        let alias = format!("_{}_QUANTILE({})", cutoff, item.column);
                        base.quantile(lit(p), QuantileMethod::Nearest).alias(&alias)
                    }
                    AggFunc::ArrayAgg => {
                        // Collect values into PostgreSQL array format
                        base.cast(DataType::String).implode().alias(format!("ARRAY_AGG({})", item.column))
                    }
                };
                if let Some(a) = &item.alias { e = e.alias(a); }
                agg_cols.push(e);
            }
        }
        let out = lf.select(&agg_cols).collect()?;
        ctx.register_df_columns_for_stage(SelectStage::ProjectSelect, &out);
        return Ok(out);
    }

    // Non-aggregate projection (including string funcs and arithmetic/UDFs)
    let mut out_cols: Vec<Column> = Vec::new();
    let mut user_generated: Vec<String> = Vec::new();
    let mut df = df; // Make df mutable so window functions can replace it with sorted_df
    let mut unnamed_counter: usize = 1;


    // Helper to allocate an Unnamed_N label
    let mut next_unnamed = || {
        let name = format!("Unnamed_{}", unnamed_counter);
        unnamed_counter += 1;
        name
    };

    // First pass: process window functions to sort DataFrame
    // This ensures df is in the correct sorted state before extracting other columns
    for item in &q.select {
        if item.window_func.is_some() {
            let wspec = item.window_spec.as_ref().ok_or_else(|| anyhow::anyhow!("Window function requires OVER clause"))?;
            
            // Sort DataFrame according to PARTITION BY + ORDER BY
            let mut sort_cols: Vec<String> = Vec::new();
            let mut sort_desc: Vec<bool> = Vec::new();
            
            // Add PARTITION BY columns first
            if let Some(partition_cols) = &wspec.partition_by {
                for col_name in partition_cols {
                    let qn = resolve_col_name_ctx(&df, ctx, col_name).unwrap_or_else(|_| col_name.clone());
                    sort_cols.push(qn);
                    sort_desc.push(false);
                }
            }
            
            // Add ORDER BY expressions
            let mut temp_col_counter = 0;
            if let Some(order_exprs) = &wspec.order_by {
                for (expr, asc) in order_exprs {                    
                    let col_name = if let AE::Term(AT::Col { name, previous: false }) = expr {
                        resolve_col_name_ctx(&df, ctx, name).unwrap_or_else(|_| name.clone())
                    } else {
                        let temp_name = format!("{}{}", WINDOW_ORDER_PREFIX, temp_col_counter);
                        temp_col_counter += 1;
                        let qualified_expr = qualify_arith_ctx(&df, ctx, expr, "WINDOW ORDER BY")?;
                        let order_expr = build_arith_expr(&qualified_expr, ctx);
                        let temp_lf = df.clone().lazy().with_column(order_expr.alias(&temp_name));
                        df = temp_lf.collect()?;
                        temp_name
                    };
                    sort_cols.push(col_name);
                    sort_desc.push(!asc);
                }
            }
            
            // Sort and clean up
            if !sort_cols.is_empty() {
                df = df.sort(sort_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                            SortMultipleOptions::default().with_order_descending_multi(sort_desc))?;
            }
            
            // Remove temporary columns
            for i in 0..temp_col_counter {
                let temp_name = format!("{}{}", WINDOW_ORDER_PREFIX, i);
                if df.get_column_names().iter().any(|n| n.as_str() == temp_name) {
                    df = df.drop(&temp_name)?;
                }
            }
        }
    }

    // Second pass: extract all SELECT items (now df is in correct sorted order)
    for item in &q.select {
        if item.func.is_none() && item.str_func.is_none() && item.expr.is_none() && item.column == "*" {
            // Expand '*' to all columns in incoming DF order.
            // Strategy:
            // - Always include the qualified column label as present in the DataFrame (source.effective_name prefix), to
            //   ensure alias-qualified references like 'vs.row_id' remain valid for downstream consumers/tests.
            // - Additionally include an unqualified alias (stripped suffix) when unique, to allow ergonomic access.
            // Build base-name frequency map across currently-visible qualified columns
            use std::collections::HashMap;
            let mut base_counts: HashMap<String, usize> = HashMap::new();
            for cname in df.get_column_names() {
                let base = cname.rsplit('.').next().unwrap_or(cname.as_str());
                let base_norm = if base == "_time" { "_time" } else { base };
                *base_counts.entry(base_norm.to_string()).or_insert(0) += 1;
            }
            // Detect if this is a time table selection (has a _time column present at all)
            let has_time = base_counts.contains_key("_time");
            // If time table, we normalize `_time` and map value→`_value` (unqualified) once
            let mut pushed_time = false;
            let mut pushed_value = false;
            for cname in df.get_column_names() {
                let cname_s = cname.as_str();
                let base = cname_s.rsplit('.').next().unwrap_or(cname_s);
                let base_norm = if base == "_time" { "_time" } else { base };
                if has_time {
                    // Special time-table handling
                    if base_norm == "_time" {
                        if !pushed_time {
                            let mut s = df.column(cname_s)?.clone();
                            s.rename("_time".into());
                            if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == "_time") { out_cols.remove(pos); }
                            out_cols.push(s);
                            pushed_time = true;
                        }
                        continue;
                    }
                    if base_norm.eq_ignore_ascii_case("_value") || base_norm.eq_ignore_ascii_case("value") {
                        if !pushed_value {
                            let mut s = df.column(cname_s)?.clone();
                            // Normalize to unqualified "_value"
                            s.rename("_value".into());
                            if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == "_value") { out_cols.remove(pos); }
                            out_cols.push(s);
                            pushed_value = true;
                        }
                        continue;
                    }
                    // For other columns (if any), keep prior behavior (qualified + possible unqualified when unique)
                }
                // Always include the qualified column label present in the DataFrame
                if !out_cols.iter().any(|c| c.name().as_str() == cname_s) {
                    let mut s = df.column(cname_s)?.clone();
                    s.rename(cname_s.into());
                    out_cols.push(s);
                }
                // Conditionally include an unqualified alias only if its base name is unique across sources
                if *base_counts.get(base_norm).unwrap_or(&0) == 1 {
                    if !out_cols.iter().any(|c| c.name().as_str() == base_norm) {
                        let mut s2 = df.column(cname_s)?.clone();
                        s2.rename(base_norm.into());
                        out_cols.push(s2);
                    }
                }
            }
            continue;
        }
        // Qualified wildcard expansion: e.g., t.*
        if item.func.is_none()
            && item.str_func.is_none()
            && item.expr.is_none()
            && item.column.ends_with(".*")
            && item.column != "*"
        {
            let qualifier = item.column[..item.column.len()-2].trim();
            if qualifier.is_empty() {
                anyhow::bail!("Syntax error: expected qualifier before .* in SELECT list");
            }

            // Determine the prefix used in the DataFrame columns for the qualifier.
            // Columns are prefixed with TableRef::effective_name() (alias if present, else table name).
            let mut prefix_match: Option<String> = None;
            let mut alias_suggestion: Option<String> = None;
            for src in &ctx.sources {
                let eff = src.effective_name().to_string();
                match src {
                    TableRef::Table { name, alias } => {
                        if eff == qualifier {
                            prefix_match = Some(eff);
                            break;
                        }
                        // If user referenced the raw table name but an alias was provided, capture suggestion
                        if alias.is_some() && name == qualifier {
                            alias_suggestion = Some(eff.clone());
                        }
                        // If no alias and name matches, accept it
                        if alias.is_none() && name == qualifier {
                            prefix_match = Some(eff.clone());
                            break;
                        }
                    }
                    TableRef::Subquery { alias, .. } => {
                        if alias == qualifier {
                            prefix_match = Some(eff);
                            break;
                        }
                    }
                    TableRef::Tvf { alias, .. } => {
                        if let Some(a) = alias {
                            if a == qualifier {
                                prefix_match = Some(eff);
                                break;
                            }
                        }
                    }
                }
            }

            let prefix = if let Some(p) = prefix_match {
                p
            } else if let Some(suggest) = alias_suggestion {
                anyhow::bail!(
                    "Qualified wildcard '{}.*' must use the table alias '{}' per PostgreSQL rules",
                    qualifier,
                    suggest
                );
            } else {
                // Build helpful error showing available qualifiers
                let mut avail: Vec<String> = ctx.sources
                    .iter()
                    .map(|s| s.effective_name().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                avail.sort();
                anyhow::bail!(
                    "Unknown qualifier '{}' for wildcard. Available sources: {:?}",
                    qualifier,
                    avail
                );
            };

            let want_prefix = format!("{}.", prefix);
            tracing::debug!(target: "clarium::exec", "SELECT: expanding qualified wildcard '{}.*' using prefix '{}'", qualifier, prefix);
            // Build uniqueness map for bases under this qualifier to decide whether to also add unqualified aliases
            use std::collections::HashMap;
            let mut base_counts: HashMap<String, usize> = HashMap::new();
            for cname in df.get_column_names() {
                let cname_s = cname.as_str();
                if cname_s.starts_with(&want_prefix) {
                    let base = cname_s.rsplit('.').next().unwrap_or(cname_s);
                    let base_norm = if base == "_time" { "_time" } else { base };
                    *base_counts.entry(base_norm.to_string()).or_insert(0) += 1;
                }
            }
            for cname in df.get_column_names() {
                let cname_s = cname.as_str();
                if cname_s.starts_with(&want_prefix) {
                    // Special-case time column: keep only a single unqualified `_time` and DO NOT keep `t._time`
                    let base = cname_s.rsplit('.').next().unwrap_or(cname_s);
                    let base_norm = if base == "_time" { "_time" } else { base };
                    if base_norm == "_time" {
                        if !out_cols.iter().any(|c| c.name().as_str() == "_time") {
                            let mut s2 = df.column(cname_s)?.clone();
                            s2.rename("_time".into());
                            out_cols.push(s2);
                        }
                        continue;
                    }
                    // For value columns on time tables: keep the qualified `t.value` (do not emit unqualified duplicate)
                    if base_norm.eq_ignore_ascii_case("_value") || base_norm.eq_ignore_ascii_case("value") {
                        if !out_cols.iter().any(|c| c.name().as_str() == cname_s) {
                            let mut s = df.column(cname_s)?.clone();
                            s.rename(cname_s.into());
                            out_cols.push(s);
                        }
                        continue;
                    }
                    // Default behavior for other columns: keep qualified and add unqualified when unique under qualifier
                    if !out_cols.iter().any(|c| c.name().as_str() == cname_s) {
                        let mut s = df.column(cname_s)?.clone();
                        s.rename(cname_s.into());
                        out_cols.push(s);
                    }
                    if *base_counts.get(base_norm).unwrap_or(&0) == 1 {
                        if !out_cols.iter().any(|c| c.name().as_str() == base_norm) {
                            let mut s2 = df.column(cname_s)?.clone();
                            s2.rename(base_norm.into());
                            out_cols.push(s2);
                        }
                    }
                }
            }
            continue;
        }
        if let Some(sf) = &item.str_func {
            let qn = resolve_col_name_ctx(&df, ctx, &item.column).unwrap_or_else(|_| item.column.clone());
            let base = col(&qn).cast(DataType::String);
            let mut e = match sf {
                StrFunc::Upper => base.str().to_uppercase().alias(format!("UPPER({})", item.column)),
                StrFunc::Lower => base.str().to_lowercase().alias(format!("LOWER({})", item.column)),
            };
            if let Some(a) = &item.alias { e = e.alias(a); }
            let tmp = df.clone().lazy().select([e]).collect()?;
            let name = item.alias.clone().unwrap_or_else(|| format!("{}({})", match sf { StrFunc::Upper => "UPPER", StrFunc::Lower => "LOWER" }, item.column.clone()));
            let s = tmp.column(&name)?.clone();
            if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == name.as_str()) { out_cols.remove(pos); }
            out_cols.push(s);
            user_generated.push(name);
        } else if item.window_func.is_some() {
            // Window function execution - DataFrame already sorted by first pass            
            let wfunc = item.window_func.as_ref().unwrap();
            let wspec = item.window_spec.as_ref().ok_or_else(|| anyhow::anyhow!("Window function requires OVER clause"))?;
            
            match wfunc {
                WindowFunc::RowNumber => {
                    // Compute row numbers: if PARTITION BY exists, reset numbering per partition
                    let row_numbers: Vec<i64> = if let Some(partition_cols) = &wspec.partition_by {
                        let mut nums: Vec<i64> = Vec::with_capacity(df.height());
                        let mut last_partition: Vec<String> = Vec::new();
                        let mut current_num: i64 = 1;
                        
                        for row_idx in 0..df.height() {
                            let mut current_partition: Vec<String> = Vec::new();
                            for col_name in partition_cols {
                                let qn = resolve_col_name_ctx(&df, ctx, col_name).unwrap_or_else(|_| col_name.clone());
                                let val = df.column(&qn)?.get(row_idx)?;
                                current_partition.push(format!("{:?}", val));
                            }
                            
                            if !last_partition.is_empty() && current_partition != last_partition {
                                current_num = 1; // Reset for new partition
                            }
                            nums.push(current_num);
                            current_num += 1;
                            last_partition = current_partition;
                        }
                        nums
                    } else {
                        // No partitioning - simple sequential numbering
                        (1..=df.height() as i64).collect()
                    };
                    
                    let result_name = item.alias.clone().unwrap_or_else(|| "rn".to_string());
                    let s = Series::new(result_name.clone().into(), row_numbers);
                    
                    if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == result_name.as_str()) { out_cols.remove(pos); }
                    out_cols.push(s.into());
                    user_generated.push(result_name);
                }
            }
        } else if item.column == "_time" && item.expr.is_none() {
            let time_col = resolve_col_name_ctx(&df, ctx, "_time").unwrap_or_else(|_| "_time".to_string());
            let mut s = df.column(&time_col)?.clone();
            // Normalize final output label for time to unqualified '_time'
            s.rename("_time".into());
            let cname = s.name().to_string();
            if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == cname.as_str()) { out_cols.remove(pos); }
            out_cols.push(s);
        } else if let Some(ex) = &item.expr {
            // General expression: handle multi-return scalar UDFs specially; otherwise evaluate normally
            // First, detect multi-return scalar UDF in projection and expand into multiple columns
            let mut handled_multi = false;
            if let ArithExpr::Call { name, args } = ex {
                if let Some(reg) = get_script_registry() {
                    if let Some(meta) = reg.get_meta(name) {
                        use crate::scripts::ScriptKind;
                        if matches!(meta.kind, ScriptKind::Scalar) && meta.returns.len() > 1 {
                            // Build argument expressions with qualification
                            let mut arg_exprs: Vec<Expr> = Vec::with_capacity(args.len());
                            for (i, a) in args.iter().enumerate() {
                                let qa = qualify_arith_ctx(&df, ctx, a, "SELECT")?;
                                arg_exprs.push(build_arith_expr(&qa, ctx).alias(format!("{}{}", ARG_PREFIX, i)));
                            }
                            let struct_expr = polars::lazy::dsl::as_struct(arg_exprs);
                            let base = item.alias.clone().unwrap_or_else(|| name.clone());
                            // For each return slot, create a mapped expression selecting that element
                            for (idx, dtype) in meta.returns.iter().enumerate() {
                                let colname = format!("{}_{}", base, idx);
                                let dtype_map = dtype.clone();
                                let dtype_field = dtype.clone();
                                let colname_for_series = colname.clone();
                                let colname_for_field = colname.clone();
                                let colname_for_alias = colname.clone();
                                let name_eval = name.clone();
                                // Capture a stable registry snapshot for use inside the map closure (rayon threads)
                                let reg_snapshot = get_script_registry().and_then(|r| r.snapshot().ok());
                                let mapped = struct_expr.clone().map(
                                    move |col: Column| {
                                        let s = col.as_materialized_series();
                                        let sc = s.struct_()?;
                                        let fields = sc.fields_as_series();
                                        let len = sc.len();
                                        if let Some(r) = &reg_snapshot {
                                            let out_col: Column = r
                                                .with_lua_function(&name_eval, |lua, func| {
                                                    use mlua::Value as LVal;
                                                    use mlua::MultiValue;
                                                    match dtype_map.clone() {
                                                        DataType::Boolean => {
                                                            let mut out: Vec<Option<bool>> = Vec::with_capacity(len);
                                                        for row_idx in 0..len {
                                                            let mut mvals = MultiValue::new();
                                                            for f in &fields {
                                                                let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                                                let lv = match av {
                                                                    polars::prelude::AnyValue::Null => LVal::Nil,
                                                                    polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                                    polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                                    polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                                    polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                                    polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                                    _ => LVal::Nil,
                                                                };
                                                                mvals.push_front(lv);
                                                            }
                                                            let outv: LVal = func.call(mvals)?;
                                                            // Expect array; pick idx-th (1-based) value
                                                            let b = match outv {
                                                                LVal::Table(t) => {
                                                                    let key = (idx as i64) + 1;
                                                                    match t.get::<i64, LVal>(key).unwrap_or(LVal::Nil) { LVal::Boolean(v) => Some(v), LVal::Nil => None, _ => None }
                                                                }
                                                                LVal::Boolean(v) => Some(v),
                                                                LVal::Nil => None,
                                                                _ => None,
                                                            };
                                                            out.push(b);
                                                        }
                                                        let s = Series::new(colname_for_series.as_str().into(), out);
                                                        Ok(s.into_column())
                                                    }
                                                    DataType::Int64 => {
                                                        let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
                                                        for row_idx in 0..len {
                                                            let mut mvals = MultiValue::new();
                                                            for f in &fields {
                                                                let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                                                let lv = match av {
                                                                    polars::prelude::AnyValue::Null => LVal::Nil,
                                                                    polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                                    polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                                    polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                                    polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                                    polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                                    _ => LVal::Nil,
                                                                };
                                                                mvals.push_front(lv);
                                                            }
                                                            let outv: LVal = func.call(mvals)?;
                                                            let v = match outv {
                                                                LVal::Table(t) => {
                                                                    let key = (idx as i64) + 1;
                                                                    match t.get::<i64, LVal>(key).unwrap_or(LVal::Nil) { LVal::Integer(i) => Some(i), LVal::Number(f) => Some(f as i64), LVal::Nil => None, _ => None }
                                                                }
                                                                LVal::Integer(i) => Some(i),
                                                                LVal::Number(f) => Some(f as i64),
                                                                LVal::Nil => None,
                                                                _ => None,
                                                            };
                                                            out.push(v);
                                                        }
                                                        let s = Series::new(colname_for_series.as_str().into(), out);
                                                        Ok(s.into_column())
                                                    }
                                                    DataType::Float64 => {
                                                        let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                                                        for row_idx in 0..len {
                                                            let mut mvals = MultiValue::new();
                                                            for f in &fields {
                                                                let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                                                let lv = match av {
                                                                    polars::prelude::AnyValue::Null => LVal::Nil,
                                                                    polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                                    polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                                    polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                                    polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                                    polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                                    _ => LVal::Nil,
                                                                };
                                                                mvals.push_front(lv);
                                                            }
                                                            let outv: LVal = func.call(mvals)?;
                                                            let v = match outv {
                                                                LVal::Table(t) => {
                                                                    let key = (idx as i64) + 1;
                                                                    match t.get::<i64, LVal>(key).unwrap_or(LVal::Nil) { LVal::Number(f) => Some(f), LVal::Integer(i) => Some(i as f64), LVal::Nil => None, _ => None }
                                                                }
                                                                LVal::Number(f) => Some(f),
                                                                LVal::Integer(i) => Some(i as f64),
                                                                LVal::Nil => None,
                                                                _ => None,
                                                            };
                                                            out.push(v);
                                                        }
                                                        let s = Series::new(colname_for_series.as_str().into(), out);
                                                        Ok(s.into_column())
                                                    }
                                                    _ => {
                                                        let mut out: Vec<Option<String>> = Vec::with_capacity(len);
                                                        for row_idx in 0..len {
                                                            let mut mvals = MultiValue::new();
                                                            for f in &fields {
                                                                let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                                                let lv = match av {
                                                                    polars::prelude::AnyValue::Null => LVal::Nil,
                                                                    polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                                    polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                                    polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                                    polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                                    polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                                    _ => LVal::Nil,
                                                                };
                                                                mvals.push_front(lv);
                                                            }
                                                            let outv: LVal = func.call(mvals)?;
                                                            let v = match outv {
                                                                LVal::Table(t) => {
                                                                    let key = (idx as i64) + 1;
                                                                    match t.get::<i64, LVal>(key).unwrap_or(LVal::Nil) { LVal::String(s) => Some(s.to_str()?.to_string()), LVal::Nil => None, _ => None }
                                                                }
                                                                LVal::String(s) => Some(s.to_str()?.to_string()),
                                                                LVal::Nil => None,
                                                                _ => None,
                                                            };
                                                            out.push(v);
                                                        }
                                                        let s = Series::new(colname_for_series.as_str().into(), out);
                                                        Ok(s.into_column())
                                                    }
                                                }
                                            })
                                            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;
                                        Ok(out_col)
                                    } else {
                                        Err(polars::error::PolarsError::ComputeError("Lua registry not initialized".into()))
                                    }
                                },
                                    move |_schema, _field| { Ok(Field::new(colname_for_field.clone().into(), dtype_field.clone())) }
                                );
                                let tmp = df.clone().lazy().select([mapped.alias(&colname_for_alias)]).collect()?;
                                let s = tmp.column(&colname_for_alias)?.clone();
                                if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == colname_for_alias.as_str()) { out_cols.remove(pos); }
                                out_cols.push(s);
                                user_generated.push(colname_for_alias);
                            }
                            handled_multi = true;
                        }
                    }
                }
            }
            if !handled_multi {
                // General expression: prefer alias, else derive; fallback to Unnamed_N
                let derived_name = match ex {
                    ArithExpr::Call { name, args: _ } => Some(name.clone()),
                    ArithExpr::BinOp { .. } => None,
                    ArithExpr::Concat(_) => None,
                    ArithExpr::Func(dfm) => {
                        // Derive a friendly label for date functions to aid tests and UX
                        match dfm {
                            DateFunc::DateAdd(_, _, _) => Some("DATEADD".to_string()),
                            DateFunc::DatePart(_, _) => Some("DATEPART".to_string()),
                            DateFunc::DateDiff(_, _, _) => Some("DATEDIFF".to_string()),
                        }
                    }
                    ArithExpr::Slice { .. } => None,
                    ArithExpr::Predicate(_) => None,
                    ArithExpr::Case { .. } => None,
                    ArithExpr::Cast { .. } => None,
                    ArithExpr::Term(_) => None,
                };
                let base_name = item.alias.clone().or(derived_name);
                let name = base_name.unwrap_or_else(&mut next_unnamed);
                let expr = build_arith_expr(&qualify_arith_ctx(&df, ctx, ex, "SELECT")?, ctx).alias(&name);
                // Use with_column instead of select to preserve df row count for constant expressions
                let tmp = df.clone().lazy().with_column(expr).select([col(&name)]).collect()?;
                let s = tmp.column(&name)?.clone();
                if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == name.as_str()) { out_cols.remove(pos); }
                out_cols.push(s);
                user_generated.push(name);
            }
        } else {
            // simple column
            let qn = resolve_col_name_ctx(&df, ctx, &item.column).unwrap_or_else(|_| item.column.clone());
            // Try to fetch the column; if missing, attempt intelligent fallbacks for qualified names
            let mut s = match df.column(&qn) {
                Ok(col) => col.clone(),
                Err(_) => {
                    // Fallbacks:
                    // - If qualified (a.b), try to locate by suffix '.b' or bare 'b'
                    // - If unqualified, try to match any column whose last segment matches
                    let mut found: Option<Column> = None;
                    if let Some((_qual, base)) = qn.rsplit_once('.') {
                        let suffix = format!(".{}", base);
                        for name in df.get_column_names() {
                            let ns = name.as_str();
                            if ns.ends_with(&suffix) || ns == base {
                                found = Some(df.column(ns).unwrap().clone());
                                break;
                            }
                        }
                    } else {
                        // unqualified: try exact first, then unique-suffix search
                        for name in df.get_column_names() {
                            let ns = name.as_str();
                            let last = ns.rsplit('.').next().unwrap_or(ns);
                            if last == qn {
                                found = Some(df.column(ns).unwrap().clone());
                                break;
                            }
                        }
                    }
                    if let Some(col) = found { col } else {
                        // Still not found: return a clear error now
                        return Err(crate::server::data_context::DataContext::column_not_found_error(&item.column, "SELECT", &df));
                    }
                }
            };
            // Use alias if provided, else use the column name as written in SELECT (preserving qualification)
            if let Some(a) = &item.alias {
                s.rename(a.clone().into());
            } else {
                // Preserve qualified names from SELECT clause: "a.id" stays "a.id"
                s.rename(item.column.clone().into());
            }
            let cname = s.name().to_string();
            if let Some(pos) = out_cols.iter().position(|c| c.name().as_str() == cname.as_str()) { out_cols.remove(pos); }
            out_cols.push(s);
        }
    }

    // Ensure ORDER BY columns exist in the projection to allow sorting (append if missing)
    // Always add missing ORDER BY columns, but track them so they can be removed later in strict mode
    if let Some(ob) = &q.order_by {
        if !ob.is_empty() {
            let mut existing: std::collections::HashSet<String> = out_cols.iter().map(|c| c.name().to_string()).collect();
            for (name, _asc) in ob.iter() {
                if existing.contains(name) { continue; }
                // resolve against incoming df (pre-projection) and append the column
                if let Ok(resolved) = resolve_col_name_ctx(&df, ctx, name) {
                    if let Ok(mut s) = df.column(&resolved).cloned() {
                        // Normalize label to unqualified form for known time column
                        let base = resolved.rsplit('.').next().unwrap_or(resolved.as_str()).to_string();
                        let target: String = if name == "_time" || base.as_str() == "_time" { "_time".to_string() } else { base };
                        s.rename(target.clone().into());
                        // avoid duplicate by name
                        if !out_cols.iter().any(|c| c.name().as_str() == target.as_str()) {
                            existing.insert(target.clone());
                            out_cols.push(s);
                            // Track this as a temporary ORDER BY column
                            ctx.temp_order_by_columns.insert(target);
                        }
                    }
                }
            }

            // Special handling: if ORDER BY has an ANN expression (vec_l2/cosine_sim) and the vector source column
            // is not in projection, append it so ANN/order stage can compute scores. Use order_by_raw[0].
            // Apply this both for explicit USING ANN and opportunistically when expression is present.
            if let Some(raw0) = q.order_by_raw.as_ref().and_then(|v| v.get(0)).map(|(s, _)| s.clone()) {
                    // Local parser for ANN ORDER BY expression: func(lhs, rhs) where lhs is table/col
                    fn parse_ann_lhs(expr: &str) -> Option<String> {
                        let txt = expr.trim();
                        let up = txt.to_ascii_lowercase();
                        let funcs = ["vec_l2", "cosine_sim"]; 
                        let func = funcs.iter().find(|f| up.starts_with(&format!("{}(", f))).cloned()?;
                        let open_pos = txt.find('(')?;
                        let inner = &txt[open_pos+1..].trim();
                        let mut depth = 1i32;
                        let bytes: Vec<char> = inner.chars().collect();
                        let mut comma_at: Option<usize> = None;
                        let mut i = 0usize;
                        while i < bytes.len() {
                            let ch = bytes[i];
                            if ch == '(' { depth += 1; }
                            else if ch == ')' { depth -= 1; if depth == 0 { break; } }
                            else if ch == ',' && depth == 1 && comma_at.is_none() { comma_at = Some(i); }
                            i += 1;
                        }
                        if depth != 0 { return None; }
                        let end_pos = i;
                        let body: String = bytes[..end_pos].iter().collect();
                        let (lhs, _rhs) = if let Some(cpos) = comma_at { (&body[..cpos], &body[cpos+1..]) } else { return None; };
                        let lhs = lhs.trim();
                        // extract last path segment as column name
                        let lhs_norm = lhs.trim_matches('"').trim_matches('`');
                        let parts: Vec<&str> = lhs_norm.split(|c| c == '.' || c == '/').collect();
                        if parts.len() < 1 { return None; }
                        Some(parts.last().unwrap().to_string())
                    }
                    if let Some(lhs_col) = parse_ann_lhs(&raw0) {
                        if !existing.contains(&lhs_col) {
                            // Resolve possibly-qualified column name against the incoming DF/context
                            if let Ok(resolved) = resolve_col_name_ctx(&df, ctx, &lhs_col) {
                                if let Ok(mut s) = df.column(&resolved).cloned() {
                                    let base = resolved.rsplit('.').next().unwrap_or(resolved.as_str()).to_string();
                                    let target = base.clone();
                                    s.rename(target.clone().into());
                                    if !out_cols.iter().any(|c| c.name().as_str() == target.as_str()) {
                                        let dt_dbg = s.dtype().clone();
                                        existing.insert(target.clone());
                                        out_cols.push(s);
                                        ctx.temp_order_by_columns.insert(target);
                                        crate::tprintln!("[PROJECT_SELECT] Added ANN source column '{}' for ORDER BY expression '{}' dtype={:?}", lhs_col, raw0, dt_dbg);
                                    }
                                }
                            }
                        }
                    }
            }
        }
    }

    let out = DataFrame::new(out_cols)?;
    // HAVING without aggregation is not supported (parity with legacy)
    if q.having_clause.is_some() { anyhow::bail!("HAVING is only supported with aggregate queries"); }

    ctx.register_df_columns_for_stage(SelectStage::ProjectSelect, &out);
    if !user_generated.is_empty() { ctx.register_user_columns_for_stage(SelectStage::ProjectSelect, user_generated); }
    Ok(out)
}
