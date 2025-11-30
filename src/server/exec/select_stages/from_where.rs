//! FROM/WHERE stage: load sources (WITH JOIN support), apply WHERE, and register columns.

use anyhow::Result;
use polars::prelude::*;

use crate::server::data_context::{DataContext, SelectStage};
use crate::query::{Query, WhereExpr, ArithExpr, ArithTerm, JoinType};
use crate::storage::SharedStore;
use crate::server::exec::exec_common::{build_where_expr};
use crate::server::exec::where_subquery::{eval_where_mask};
use crate::tprintln;

fn extract_simple_equi_with_remainder(on: &WhereExpr) -> Option<((String, String), Option<WhereExpr>)> {
    match on {
        WhereExpr::Comp { left, op, right } => {
            if *op != crate::query::CompOp::Eq { return None; }
            let l = match left { ArithExpr::Term(ArithTerm::Col { name, previous: false }) => Some(name.clone()), _ => None }?;
            let r = match right { ArithExpr::Term(ArithTerm::Col { name, previous: false }) => Some(name.clone()), _ => None }?;
            Some(((l, r), None))
        }
        WhereExpr::And(a, b) => {
            if let Some(((l, r), rem)) = extract_simple_equi_with_remainder(a) {
                let remainder = if let Some(rm) = rem { Some(WhereExpr::And(Box::new(rm), b.clone())) } else { Some((**b).clone()) };
                return Some(((l, r), remainder));
            }
            if let Some(((l, r), rem)) = extract_simple_equi_with_remainder(b) {
                let remainder = if let Some(rm) = rem { Some(WhereExpr::And(a.clone(), Box::new(rm))) } else { Some((**a).clone()) };
                return Some(((l, r), remainder));
            }
            None
        }
        WhereExpr::Or(_, _) | WhereExpr::IsNull { .. } | WhereExpr::Exists { .. } | WhereExpr::All { .. } | WhereExpr::Any { .. } => None,
    }
}

fn join_how(t: &JoinType) -> polars::prelude::JoinType {
    match t {
        JoinType::Inner => polars::prelude::JoinType::Inner,
        JoinType::Left => polars::prelude::JoinType::Left,
        JoinType::Right => polars::prelude::JoinType::Right,
        JoinType::Full => polars::prelude::JoinType::Full,
    }
}

pub fn from_where(store: &SharedStore, q: &Query, ctx: &mut DataContext) -> Result<DataFrame> {
    // Build base DataFrame
    let mut df = if let Some(tref) = &q.base_table {
        ctx.add_source(tref);
        tprintln!("Defaulting to {:?} dataframe", tref);
        ctx.load_source_df(store, tref)?
    } else {
        tprintln!("Defaulting to blank dataframe");
        // Support queries without a FROM source by starting with a single-row dummy DataFrame.
        // This allows SELECT constant expressions (including casts/UDF calls) to yield one row.
        let s = Series::new("__unit".into(), vec![1i32]);
        DataFrame::new(vec![s.into()])?   
    };

    // If there is no FROM source, skip JOINs and WHERE entirely and return the empty DataFrame
    if q.base_table.is_none() {
        ctx.register_df_columns_for_stage(SelectStage::FromWhere, &df);
        return Ok(df);
    }

    // Helpers to qualify columns in expressions using DataContext against a concrete DF
    fn qualify_arith_ctx(df: &DataFrame, ctx: &DataContext, a: &ArithExpr, clause: &str) -> anyhow::Result<ArithExpr> {
        use crate::query::{ArithExpr as AE, ArithTerm as AT};
        Ok(match a {
            AE::Term(AT::Col { name, previous: false }) => {
                let qn = ctx.resolve_column(df, name).map_err(|_| DataContext::column_not_found_error(name, clause, df))?;
                AE::Term(AT::Col { name: qn, previous: false })
            }
            AE::Term(_) => a.clone(),
            AE::Cast { expr, ty } => {
                AE::Cast { expr: Box::new(qualify_arith_ctx(df, ctx, expr, clause)?), ty: ty.clone() }
            }
            AE::BinOp { left, op, right } => AE::BinOp {
                left: Box::new(qualify_arith_ctx(df, ctx, left, clause)?),
                op: op.clone(),
                right: Box::new(qualify_arith_ctx(df, ctx, right, clause)?),
            },
            AE::Concat(parts) => AE::Concat(parts.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()?),
            AE::Call { name, args } => AE::Call { name: name.clone(), args: args.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()? },
            AE::Func(dfm) => {
                use crate::query::DateFunc;
                match dfm {
                    DateFunc::DatePart(part, a1) => AE::Func(DateFunc::DatePart(part.clone(), Box::new(qualify_arith_ctx(df, ctx, a1, clause)?))),
                    DateFunc::DateAdd(part, a1, a2) => AE::Func(DateFunc::DateAdd(part.clone(), Box::new(qualify_arith_ctx(df, ctx, a1, clause)?), Box::new(qualify_arith_ctx(df, ctx, a2, clause)?))),
                    DateFunc::DateDiff(part, a1, a2) => AE::Func(DateFunc::DateDiff(part.clone(), Box::new(qualify_arith_ctx(df, ctx, a1, clause)?), Box::new(qualify_arith_ctx(df, ctx, a2, clause)?))),
                }
            }
            AE::Slice { base, start, stop, step } => {
                use crate::query::StrSliceBound;
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
        use crate::query::WhereExpr as WE;
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

    // Apply JOINs (left-associative) if present
    if let Some(joins) = &q.joins {
        for jc in joins {
            // Load right side with alias-prefixed columns
            ctx.add_source(&jc.right);
            let right_df = ctx.load_source_df(store, &jc.right)?;
            
            // Try to extract equi-join condition with remainder
            let joined = if let Some(((left_key, right_key), remainder_opt)) = extract_simple_equi_with_remainder(&jc.on) {
                // Equi-join path: use hash join
                // The extracted keys are from the comparison expression, but they may reference either table.
                // Try to resolve each key against both tables to determine which belongs where.
                let (lk, rk) = if ctx.resolve_column(&df, &left_key).is_ok() && ctx.resolve_column(&right_df, &right_key).is_ok() {
                    // left_key is in left table, right_key is in right table (normal case)
                    let lk_resolved = ctx.resolve_column(&df, &left_key).map_err(|_| DataContext::column_not_found_error(&left_key, "JOIN ON", &df))?;
                    let rk_resolved = ctx.resolve_column(&right_df, &right_key).map_err(|_| DataContext::column_not_found_error(&right_key, "JOIN ON", &right_df))?;
                    (lk_resolved, rk_resolved)
                } else if ctx.resolve_column(&df, &right_key).is_ok() && ctx.resolve_column(&right_df, &left_key).is_ok() {
                    // Keys are swapped: right_key is in left table, left_key is in right table
                    let lk_resolved = ctx.resolve_column(&df, &right_key).map_err(|_| DataContext::column_not_found_error(&right_key, "JOIN ON", &df))?;
                    let rk_resolved = ctx.resolve_column(&right_df, &left_key).map_err(|_| DataContext::column_not_found_error(&left_key, "JOIN ON", &right_df))?;
                    (lk_resolved, rk_resolved)
                } else {
                    // Cannot resolve keys properly – include helpful diagnostics
                    let left_cols = df.get_column_names();
                    let right_cols = right_df.get_column_names();
                    let max_show = 12usize;
                    let left_preview = if left_cols.len() > max_show { format!("{:?} ... (+{} more)", &left_cols[..max_show], left_cols.len()-max_show) } else { format!("{:?}", left_cols) };
                    let right_preview = if right_cols.len() > max_show { format!("{:?} ... (+{} more)", &right_cols[..max_show], right_cols.len()-max_show) } else { format!("{:?}", right_cols) };
                    return Err(anyhow::anyhow!(
                        "JOIN ON: cannot resolve join keys '{}' and '{}' against tables. Available left cols: {} ; available right cols: {}",
                        left_key, right_key, left_preview, right_preview
                    ));
                };
                tracing::debug!(target: "clarium::exec", "JOIN: left cols before={:?}, right cols before={:?}", df.get_column_names(), right_df.get_column_names());
                tracing::debug!(target: "clarium::exec", "JOIN: left_key='{}', right_key='{}'", lk, rk);
                let how = join_how(&jc.join_type);
                let mut joined = df.join(&right_df, vec![lk.as_str()], vec![rk.as_str()], how.into(), None)?;
                // Preserve both join key columns when they have different qualified names.
                // Some backends (and clients like DBeaver) reference the right-side key (e.g., c.oid)
                // in subsequent JOINs. If Polars dropped the right key during the join, recreate it
                // from the left key column (values are equal for equi-joins).
                if lk != rk {
                    let has_rk = joined.get_column_names().iter().any(|c| c.as_str() == rk.as_str());
                    if !has_rk {
                        let s_left = joined.column(lk.as_str())?.clone();
                        let mut s_as_rk = s_left.clone();
                        s_as_rk.rename(rk.clone().into());
                        joined.with_column(s_as_rk)?;
                    }
                }
                tracing::debug!(target: "clarium::exec", "JOIN: result cols={:?}", joined.get_column_names());
                // Apply remainder predicate as filter if present
                if let Some(rem) = remainder_opt {
                    let qualified_rem = qualify_where_ctx(&joined, ctx, &rem, "JOIN ON")?;
                    let mask = build_where_expr(&qualified_rem, ctx);
                    joined = joined.lazy().filter(mask).collect()?;
                }
                joined
            } else {
                // Pure non-equi join: use cross join + filter
                // Note: only INNER and LEFT joins are supported for non-equi conditions
                match jc.join_type {
                    JoinType::Inner => {
                        // Cross join followed by filter: manual cartesian product
                        let left_height = df.height();
                        let right_height = right_df.height();
                        let total_rows = left_height * right_height;
                        
                        // Build crossed DataFrame manually
                        let mut crossed_cols: Vec<Column> = Vec::new();
                        
                        // Repeat each left row right_height times
                        for col_name in df.get_column_names() {
                            let col = df.column(col_name.as_str())?;
                            let mut values = Vec::with_capacity(total_rows);
                            for i in 0..left_height {
                                let val = col.get(i)?;
                                for _ in 0..right_height {
                                    values.push(val.clone());
                                }
                            }
                            let repeated_series = Series::from_any_values(col_name.clone(), &values, false)?;
                            crossed_cols.push(repeated_series.into());
                        }
                        
                        // Tile the entire right DataFrame left_height times
                        for col_name in right_df.get_column_names() {
                            let col = right_df.column(col_name.as_str())?;
                            let mut values = Vec::with_capacity(total_rows);
                            for _ in 0..left_height {
                                for j in 0..right_height {
                                    values.push(col.get(j)?);
                                }
                            }
                            let tiled_series = Series::from_any_values(col_name.clone(), &values, false)?;
                            crossed_cols.push(tiled_series.into());
                        }
                        
                        let crossed = DataFrame::new(crossed_cols)?;
                        let qualified_on = qualify_where_ctx(&crossed, ctx, &jc.on, "JOIN ON")?;
                        let mask = build_where_expr(&qualified_on, ctx);
                        crossed.lazy().filter(mask).collect()?
                    }
                    JoinType::Left => {
                        // LEFT join with non-equi: manual cartesian product + filter for matches, then append unmatched left rows
                        let left_height = df.height();
                        let right_height = right_df.height();
                        
                        // Manual cross join with row tracking
                        let left_row_ids: Vec<i64> = (0..left_height as i64).collect();
                        let mut df_with_id = df.clone();
                        df_with_id.with_column(Series::new("__left_row_id".into(), left_row_ids))?;
                        
                        let total_rows = left_height * right_height;
                        let mut crossed_cols: Vec<Column> = Vec::new();
                        
                        // Repeat each left row (with ID) right_height times
                        for col_name in df_with_id.get_column_names() {
                            let col = df_with_id.column(col_name.as_str())?;
                            let mut values = Vec::with_capacity(total_rows);
                            for i in 0..left_height {
                                let val = col.get(i)?;
                                for _ in 0..right_height {
                                    values.push(val.clone());
                                }
                            }
                            let repeated_series = Series::from_any_values(col_name.clone(), &values, false)?;
                            crossed_cols.push(repeated_series.into());
                        }
                        
                        // Tile the entire right DataFrame left_height times
                        for col_name in right_df.get_column_names() {
                            let col = right_df.column(col_name.as_str())?;
                            let mut values = Vec::with_capacity(total_rows);
                            for _ in 0..left_height {
                                for j in 0..right_height {
                                    values.push(col.get(j)?);
                                }
                            }
                            let tiled_series = Series::from_any_values(col_name.clone(), &values, false)?;
                            crossed_cols.push(tiled_series.into());
                        }
                        
                        let crossed_with_id = DataFrame::new(crossed_cols)?;
                        let qualified_on_id = qualify_where_ctx(&crossed_with_id, ctx, &jc.on, "JOIN ON")?;
                        let mask_id = build_where_expr(&qualified_on_id, ctx);
                        let matched_with_id = crossed_with_id.lazy().filter(mask_id).collect()?;
                        
                        // Create matched DataFrame without tracking column
                        let matched = matched_with_id.drop("__left_row_id")?;
                        
                        // Extract matched left row IDs
                        let matched_ids: std::collections::HashSet<i64> = if matched_with_id.height() > 0 {
                            let id_col = matched_with_id.column("__left_row_id")?.i64()?;
                            id_col.into_iter().flatten().collect()
                        } else {
                            std::collections::HashSet::new()
                        };
                        
                        // Filter to unmatched left rows
                        let unmatched_mask: Vec<bool> = (0..left_height as i64).map(|i| !matched_ids.contains(&i)).collect();
                        let unmatched_left = df.filter(&BooleanChunked::from_slice("".into(), &unmatched_mask))?;
                        
                        // Create null columns for right side
                        let right_cols = right_df.get_column_names();
                        let right_cols_len = right_cols.len();
                        let mut unmatched_cols: Vec<Column> = Vec::new();
                        
                        // Add left columns
                        for col_name in df.get_column_names() {
                            unmatched_cols.push(unmatched_left.column(col_name.as_str())?.clone());
                        }
                        
                        // Add null right columns
                        for right_col_name in &right_cols {
                            let right_col = right_df.column(right_col_name.as_str())?;
                            let dtype = right_col.dtype();
                            let null_series = Series::new_null((*right_col_name).clone(), unmatched_left.height());
                            let null_series_casted = null_series.cast(dtype)?;
                            unmatched_cols.push(null_series_casted.into());
                        }
                        
                        let unmatched_df = DataFrame::new(unmatched_cols)?;
                        // Diagnostics: compare schemas/dtypes before vstack which may fail if dtypes differ
                        tracing::debug!(target: "clarium::exec", "JOIN unmatched build: left_unmatched rows={} right_cols_added={}", unmatched_left.height(), right_cols_len);
                        if cfg!(debug_assertions) {
                            let mut md: Vec<String> = Vec::new();
                            for cname in matched.get_column_names() {
                                if let Ok(c) = matched.column(cname.as_str()) { md.push(format!("{}:{:?}", cname, c.dtype())); }
                            }
                            tracing::debug!(target: "clarium::exec", "JOIN matched dtypes: [{}]", md.join(", "));
                            let mut ud: Vec<String> = Vec::new();
                            for cname in unmatched_df.get_column_names() {
                                if let Ok(c) = unmatched_df.column(cname.as_str()) { ud.push(format!("{}:{:?}", cname, c.dtype())); }
                            }
                            tracing::debug!(target: "clarium::exec", "JOIN unmatched dtypes: [{}]", ud.join(", "));
                            // Highlight first dtype mismatch by column name if present
                            for cname in matched.get_column_names() {
                                if let (Ok(mc), Ok(uc)) = (matched.column(cname.as_str()), unmatched_df.column(cname.as_str())) {
                                    if mc.dtype() != uc.dtype() {
                                        tracing::debug!(target: "clarium::exec", "JOIN vstack potential mismatch: column='{}' matched={:?} unmatched={:?}", cname, mc.dtype(), uc.dtype());
                                    }
                                }
                            }
                        }
                        
                        // Vertically concatenate matched and unmatched (vstack)
                        // Special case: if matched is empty (e.g., right table was empty), use unmatched schema
                        if matched.height() == 0 && unmatched_df.height() > 0 {
                            unmatched_df
                        } else if unmatched_df.height() > 0 {
                            matched.vstack(&unmatched_df)?
                        } else {
                            matched
                        }
                    }
                    _ => anyhow::bail!("RIGHT/FULL JOIN with pure non-equi conditions requires at least one equality in ON clause"),
                }
            };
            df = joined;
        }
    }

    // Apply WHERE filter if present with clause-aware validation (columns + UDFs)
    if let Some(w) = &q.where_clause {
        eprintln!("[FROM/WHERE dbg] where_clause present: true, before rows={}", df.height());
        // Validate UDF presence in WHERE expressions
        fn collect_udf_names_where(w: &crate::query::WhereExpr, out: &mut Vec<String>) {
            use crate::query::WhereExpr as WE;
            use crate::query::ArithExpr as AE;
            match w {
                WE::Comp { left, right, .. } => {
                    fn collect_from_arith(a: &AE, out: &mut Vec<String>) {
                        match a {
                            AE::Call { name, args } => { out.push(name.clone()); for x in args { collect_from_arith(x, out); } },
                            AE::BinOp { left, right, .. } => { collect_from_arith(left, out); collect_from_arith(right, out); },
                            AE::Concat(parts) => { for p in parts { collect_from_arith(p, out); } },
                            _ => {}
                        }
                    }
                    collect_from_arith(left, out);
                    collect_from_arith(right, out);
                }
                WE::And(a, b) | WE::Or(a, b) => { collect_udf_names_where(a, out); collect_udf_names_where(b, out); }
                WE::IsNull { expr, .. } => {
                    fn collect_from_arith(a: &AE, out: &mut Vec<String>) {
                        match a {
                            AE::Call { name, args } => { out.push(name.clone()); for x in args { collect_from_arith(x, out); } },
                            AE::BinOp { left, right, .. } => { collect_from_arith(left, out); collect_from_arith(right, out); },
                            AE::Concat(parts) => { for p in parts { collect_from_arith(p, out); } },
                            _ => {}
                        }
                    }
                    collect_from_arith(expr, out);
                }
                WE::Exists { .. } | WE::Any { .. } | WE::All { .. } => {
                    // Subqueries are validated separately during their execution
                }
            }
        }
        if let Some(reg) = crate::scripts::get_script_registry() {
            let mut udf_names: Vec<String> = Vec::new();
            collect_udf_names_where(w, &mut udf_names);
            for n in udf_names { 
                if !reg.has_function(&n) { 
                    anyhow::bail!("UDF '{}' not found in WHERE clause", n); 
                } 
            }
        }
        eprintln!("[FROM/WHERE dbg] before WHERE: rows={}", df.height());
        // Qualify and apply filter using DataContext so unqualified/suffix columns resolve
        let qw = qualify_where_ctx(&df, ctx, w, "WHERE")?;
        // Always evaluate WHERE via eval_where_mask so correlated subqueries are supported.
        // eval_where_mask delegates to Polars for simple predicates, so this is safe.
        let mask = eval_where_mask(&df, ctx, store, &qw)?;
        let kept = mask.len() - mask.into_iter().filter(|v| !v.unwrap_or(false)).count();
        eprintln!("[FROM/WHERE dbg] mask computed; keeping {} rows", kept);
        let mask2 = eval_where_mask(&df, ctx, store, &qw)?;
        df = df.filter(&mask2)?;
        eprintln!("[FROM/WHERE dbg] after WHERE: rows={}", df.height());
    } else {
        eprintln!("[FROM/WHERE dbg] where_clause present: false, rows={}", df.height());
    }

    // Register visible columns for this stage
    ctx.register_df_columns_for_stage(SelectStage::FromWhere, &df);

    Ok(df)
}
