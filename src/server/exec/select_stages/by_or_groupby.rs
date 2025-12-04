//! BY or GROUP BY stage
//! Ported logic from exec_select.rs for BY window and GROUP BY paths (parity-focused).

use anyhow::Result;
use polars::prelude::*;
use polars::prelude::AnyValue;
use tracing::debug;
use crate::tprintln;

use crate::server::data_context::{DataContext, SelectStage};
use crate::server::query::query_common::Query;
use crate::server::query::query_common::AggFunc;
use crate::server::query::query_common::DateFunc;
use crate::server::query::query_common::ArithExpr;
use crate::server::query::query_common::StrSliceBound;
use crate::server::query::query_common::ArithExpr as AE;
use crate::server::query::query_common::ArithTerm as AT;
use crate::server::exec::exec_common::build_arith_expr;
use crate::scripts::get_script_registry;
use crate::server::exec::select_stages::having::apply_having_with_validation;
use crate::storage::SharedStore;

pub fn by_or_groupby(store: &SharedStore, df: DataFrame, q: &Query, ctx: &mut DataContext) -> Result<DataFrame> {
    debug!("[BY_OR_GROUPBY] Entering: by_window={:?}, group_by_cols={:?}, by_slices={:?}", q.by_window_ms.is_some(), q.group_by_cols.as_ref().map(|v| v.len()), q.by_slices.is_some());
    // If no BY/GROUP BY/SLICE requested, passthrough
    if q.by_window_ms.is_none() && q.group_by_cols.is_none() && q.by_slices.is_none() {
        debug!("[BY_OR_GROUPBY] Passthrough: no BY/GROUP BY/SLICE");
        ctx.register_df_columns_for_stage(SelectStage::ByOrGroupBy, &df);
        return Ok(df);
    }

    // BY SLICE path (manual or table-driven slices with optional labels)
    if let Some(plan) = &q.by_slices {
        tprintln!("BY SLICE path");
        // Compute slice intervals (and optional labels) using exec_slice
        let slice_df = crate::server::exec::exec_slice::run_slice(store, plan, ctx)?;
        // Resolve _time column name in base DF
        let time_col = ctx.resolve_column(&df, "_time").unwrap_or_else(|_| "_time".to_string());
        // Prepare output column builders
        let mut out_time: Vec<i64> = Vec::new();
        // Prepare map for agg column vectors keyed by output name
        use std::collections::BTreeMap;
        let mut agg_columns: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
        // Label columns if present in slice_df (any string columns excluding _start_date/_end_date)
        let label_names: Vec<String> = slice_df
            .get_column_names()
            .into_iter()
            .filter(|n| n.as_str() != "_start_date" && n.as_str() != "_end_date")
            .map(|s| s.to_string())
            .collect();
        // Maintain label value vectors
        let mut label_values: BTreeMap<String, Vec<Option<String>>> = BTreeMap::new();
        for ln in &label_names { label_values.insert(ln.clone(), Vec::new()); }

        // Aggregation columns will be created on demand using entry API; no separate closure is needed.
        // Iterate slices
        let s_start = slice_df.column("_start_date")?.i64()?;
        let s_end = slice_df.column("_end_date")?.i64()?;
        tprintln!("BY SLICE path");
        for i in 0..slice_df.height() {
            let start_t = s_start.get(i).unwrap();
            let end_t = s_end.get(i).unwrap();
            // Filter base df rows where start_t <= _time < end_t
            let t_ca = df.column(&time_col)?.i64()?;
            let mask = t_ca.gt_eq(start_t) & t_ca.lt(end_t);
            let part = df.filter(&mask)?;
            // Record _time for the slice as start
            out_time.push(start_t);
            // Capture labels for this slice row
            for ln in &label_names {
                let av = slice_df.column(ln)?.get(i).ok();
                let val_opt = match av {
                    Some(AnyValue::String(s)) => Some(s.to_string()),
                    Some(AnyValue::StringOwned(s)) => Some(s.to_string()),
                    Some(AnyValue::Null) => None,
                    Some(other) => Some(other.to_string()),
                    None => None,
                };
                if let Some(v) = label_values.get_mut(ln) { v.push(val_opt); }
            }
            // Compute aggregations for each select item
            for item in &q.select {
                if let Some(func) = &item.func {
                    // Determine base series expression/column
                    let series_opt: Option<Series> = if (matches!(func, AggFunc::Count) && item.column == "*") {
                        // COUNT(*) doesn't reference a series
                        None
                    } else if let Some(ex) = &item.expr {
                        // Build expression against qualified arithmetic (BY clause context)
                        let qa = {
                            // Qualify using current part df context; if fails, fallback to original df for resolution
                            
                            fn qualify(df: &DataFrame, ctx: &DataContext, a: &ArithExpr) -> anyhow::Result<ArithExpr> {                                
                                Ok(match a {
                                    AE::Term(AT::Col { name, previous: false }) => {
                                        let qn = ctx.resolve_column(df, name).map_err(|_| crate::server::data_context::DataContext::column_not_found_error(name, "BY SLICE", df))?;
                                        AE::Term(AT::Col { name: qn, previous: false })
                                    }
                                    AE::Term(_) => a.clone(),
                                    AE::Cast { expr, ty } => {
                                        AE::Cast { expr: Box::new(qualify(df, ctx, expr)?), ty: ty.clone() }
                                    }
                                    AE::BinOp { left, op, right } => AE::BinOp { left: Box::new(qualify(df, ctx, left)?), op: op.clone(), right: Box::new(qualify(df, ctx, right)?) },
                                    AE::Concat(parts) => AE::Concat(parts.iter().map(|p| qualify(df, ctx, p)).collect::<anyhow::Result<Vec<_>>>()?),
                                    AE::Call { name, args } => AE::Call { name: name.clone(), args: args.iter().map(|p| qualify(df, ctx, p)).collect::<anyhow::Result<Vec<_>>>()? },
                                    AE::Func(dfm) => {                                        
                                        match dfm {
                                            DateFunc::DatePart(part, a1) => AE::Func(DateFunc::DatePart(part.clone(), Box::new(qualify(df, ctx, a1)?))),
                                            DateFunc::DateAdd(part, a1, a2) => AE::Func(DateFunc::DateAdd(part.clone(), Box::new(qualify(df, ctx, a1)?), Box::new(qualify(df, ctx, a2)?))),
                                            DateFunc::DateDiff(part, a1, a2) => AE::Func(DateFunc::DateDiff(part.clone(), Box::new(qualify(df, ctx, a1)?), Box::new(qualify(df, ctx, a2)?))),
                                        }
                                    }
                                    AE::Slice { base, start, stop, step } => AE::Slice { base: Box::new(qualify(df, ctx, base)?), start: start.clone(), stop: stop.clone(), step: *step },
                                    AE::Predicate(_) => a.clone(),
                                    AE::Case { when_clauses, else_expr } => {
                                        let qualified_when = when_clauses.iter().map(|(cond, val)| {
                                            Ok((cond.clone(), qualify(df, ctx, val)?))
                                        }).collect::<anyhow::Result<Vec<_>>>()?;
                                        let qualified_else = else_expr.as_ref().map(|e| qualify(df, ctx, e)).transpose()?;
                                        AE::Case { when_clauses: qualified_when, else_expr: qualified_else.map(Box::new) }
                                    }
                                })
                            }
                            qualify(&part, ctx, ex)?
                        };
                        let expr = build_arith_expr(&qa, ctx);
                        let out = part.clone().lazy().select([expr.alias("__tmp__")]).collect()?;
                        out.column("__tmp__")?.as_series().cloned()
                    } else {
                        // Plain column
                        let qn = ctx.resolve_column(&part, &item.column).unwrap_or_else(|_| item.column.clone());
                        part.column(&qn).ok().and_then(|c| c.as_series()).cloned()
                    };
                    let out_name = match func {
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
                    let vec = agg_columns.entry(out_name.clone()).or_default();
                    // Compute aggregation value (as f64 where applicable)
                    // Support Int64 inputs by casting to Float64 on the fly
                    let f64ca_opt: Option<polars::chunked_array::ChunkedArray<polars::datatypes::Float64Type>> = series_opt.as_ref().and_then(|s| {
                        if s.dtype() == &DataType::Float64 {
                            s.f64().ok().cloned()
                        } else if s.dtype() == &DataType::Int64 {
                            s.cast(&DataType::Float64).ok().and_then(|cs| cs.f64().ok().cloned())
                        } else { None }
                    });
                    let val: Option<f64> = match func {
                        AggFunc::Avg => f64ca_opt.as_ref().and_then(|ca| ca.mean()),
                        AggFunc::Max => f64ca_opt.as_ref().and_then(|ca| ca.max()),
                        AggFunc::Min => f64ca_opt.as_ref().and_then(|ca| ca.min()),
                        AggFunc::Sum => f64ca_opt.as_ref().and_then(|ca| ca.sum()),
                        AggFunc::Count => {
                            debug!("[BY SLICE COUNT] Processing COUNT for column='{}', is_star={}, part.height()={}", item.column, item.column == "*", part.height());
                            let count_val = if item.column == "*" { 
                                part.height() as f64 
                            } else { 
                                (series_opt.as_ref().map(|s| s.len() as f64)).unwrap_or(0.0) 
                            };
                            debug!("[BY SLICE COUNT] COUNT result={} (as f64)", count_val);
                            Some(count_val)
                        },
                        AggFunc::First => f64ca_opt.as_ref().and_then(|ca| ca.get(0)),
                        AggFunc::Last => f64ca_opt.as_ref().and_then(|ca| ca.get(ca.len()-1)),
                        AggFunc::Stdev => f64ca_opt.as_ref().and_then(|ca| ca.std(1)),
                        AggFunc::Delta => {
                            if let Some(s) = f64ca_opt.as_ref() {
                                if s.len() >= 2 {
                                    match (s.get(0), s.get(s.len() - 1)) { (Some(a), Some(b)) => Some(b - a), _ => None }
                                } else { None }
                            } else { None }
                        }
                        AggFunc::Height => {
                            if let Some(s) = f64ca_opt.as_ref() {
                                match (s.min(), s.max()) { (Some(minv), Some(maxv)) => Some(maxv - minv), _ => None }
                            } else { None }
                        }
                        AggFunc::Gradient => {
                            if let Some(vals) = f64ca_opt.as_ref() {
                                if vals.len() >= 2 {
                                    let dt = (end_t - start_t) as f64;
                                    if dt != 0.0 {
                                        let first = vals.get(0);
                                        let last = vals.get(vals.len() - 1);
                                        match (first, last) { (Some(a), Some(b)) => Some((b - a) / dt), _ => None }
                                    } else { None }
                                } else { None }
                            } else { None }
                        }
                        AggFunc::Quantile(qp) => {
                            let qf = (*qp as f64) / 100.0;
                            f64ca_opt.as_ref().and_then(|ca| {
                                let mut v: Vec<f64> = ca.into_iter().flatten().collect();
                                if v.is_empty() { return None; }
                                v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                                let pos = (qf * ((v.len() as f64) - 1.0)).round() as usize;
                                v.get(pos).cloned()
                            })
                        }
                        AggFunc::ArrayAgg => {
                            // ArrayAgg collects values into PostgreSQL array format: {val1,val2,val3}
                            // For now, store as NaN as placeholder (will handle string output separately)
                            None
                        }
                    };
                    vec.push(val);
                }
            }
        }
        // Build output DataFrame
        let mut cols: Vec<Column> = Vec::new();
        cols.push(Series::new("_time".into(), out_time).into());
        // Append agg columns in deterministic order
        for (name, vals) in agg_columns.into_iter() {
            cols.push(Series::new(name.into(), vals).into());
        }
        // Append label columns
        for ln in label_names.into_iter() {
            if let Some(vs) = label_values.remove(&ln) {
                cols.push(Series::new(ln.into(), vs).into());
            }
        }
        let out_df = DataFrame::new(cols)?;
        ctx.register_df_columns_for_stage(SelectStage::ByOrGroupBy, &out_df);
        return Ok(out_df);
    }

    // BY window path
    if let Some(win) = q.by_window_ms {
        // Disallow string functions with windowed aggregation for now
        if q.select.iter().any(|i| i.str_func.is_some()) {
            anyhow::bail!("String functions are not supported with BY window");
        }
        // Column resolver helpers
        fn resolve_col_name_ctx(df: &DataFrame, ctx: &DataContext, name: &str) -> anyhow::Result<String> { ctx.resolve_column_at_stage(df, name, SelectStage::ByOrGroupBy) }
        fn qualify_arith_ctx(df: &DataFrame, ctx: &DataContext, a: &ArithExpr, clause: &str) -> anyhow::Result<ArithExpr> {            
            Ok(match a {
                AE::Term(AT::Col { name, previous: false }) => {
                    let qn = resolve_col_name_ctx(df, ctx, name).map_err(|_| crate::server::data_context::DataContext::column_not_found_error(name, clause, df))?;
                    AE::Term(AT::Col { name: qn, previous: false })
                }
                AE::Term(_) => a.clone(),
                AE::Cast { expr, ty } => {
                    AE::Cast { expr: Box::new(qualify_arith_ctx(df, ctx, expr, clause)?), ty: ty.clone() }
                }
                AE::BinOp { left, op, right } => AE::BinOp { left: Box::new(qualify_arith_ctx(df, ctx, left, clause)?), op: op.clone(), right: Box::new(qualify_arith_ctx(df, ctx, right, clause)?) },
                AE::Concat(parts) => AE::Concat(parts.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()?),
                AE::Call { name, args } => AE::Call { name: name.clone(), args: args.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()? },
                AE::Func(dfm) => {                    
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
                AE::Predicate(_) => a.clone(),
                AE::Case { when_clauses, else_expr } => {
                    let qualified_when = when_clauses.iter().map(|(cond, val)| {
                        Ok((cond.clone(), qualify_arith_ctx(df, ctx, val, clause)?))
                    }).collect::<anyhow::Result<Vec<_>>>()?;
                    let qualified_else = else_expr.as_ref().map(|e| qualify_arith_ctx(df, ctx, e, clause)).transpose()?;
                    AE::Case { when_clauses: qualified_when, else_expr: qualified_else.map(Box::new) }
                }
            })
        }
        // Clause-aware validation: ensure referenced columns/UDFs exist at this stage
        for item in &q.select {
            if item.func.is_some() && item.expr.is_none() {
                // COUNT(*) does not reference a real column
                if !(matches!(item.func, Some(AggFunc::Count)) && item.column == "*")
                    && resolve_col_name_ctx(&df, ctx, &item.column).is_err() {
                        return Err(crate::server::data_context::DataContext::column_not_found_error(&item.column, "BY", &df));
                    }
            }
        }
        // Validate UDF presence in BY expressions
        fn collect_udf_names_arith(a: &ArithExpr, out: &mut Vec<String>) {            
            match a {
                AE::Call { name, args } => { out.push(name.clone()); for x in args { collect_udf_names_arith(x, out); } },
                AE::BinOp { left, right, .. } => { collect_udf_names_arith(left, out); collect_udf_names_arith(right, out); },
                AE::Concat(parts) => { for p in parts { collect_udf_names_arith(p, out); } },
                _ => {}
            }
        }
        if let Some(reg) = get_script_registry() {
            let mut udf_names: Vec<String> = Vec::new();
            for item in &q.select { if let Some(ex) = &item.expr { collect_udf_names_arith(ex, &mut udf_names); } }
            for n in udf_names { if !reg.has_function(&n) { anyhow::bail!(format!("UDF '{}' not found in BY clause", n)); } }
        }
        // bucket column using resolved _time
        let time_col = resolve_col_name_ctx(&df, ctx, "_time").unwrap_or_else(|_| "_time".to_string());
        let t = df.column(&time_col)?.i64()?;
        let buckets: Vec<i64> = t.into_iter().map(|opt| opt.map(|v| (v / win) * win).unwrap_or_default()).collect();
        let bucket_s = Series::new("_bucket".into(), buckets);
        let df = df.hstack(&[bucket_s.into()])?;

        // build groupby via lazy API for compatibility
        let mut agg_cols: Vec<Expr> = Vec::new();
        for item in &q.select {
            if let Some(func) = &item.func {
                let base = if let Some(ex) = &item.expr { build_arith_expr(&qualify_arith_ctx(&df, ctx, ex, "BY")?, ctx) } else if matches!(func, AggFunc::Count) && item.column == "*" { lit(1) } else {
                    let qn = resolve_col_name_ctx(&df, ctx, &item.column).unwrap_or_else(|_| item.column.clone());
                    col(&qn)
                };
                let mut e = match func {
                    AggFunc::Avg => base.mean().alias(format!("AVG({})", item.column)),
                    AggFunc::Max => base.max().alias(format!("MAX({})", item.column)),
                    AggFunc::Min => base.min().alias(format!("MIN({})", item.column)),
                    AggFunc::Sum => base.sum().alias(format!("SUM({})", item.column)),
                    AggFunc::Count => {
                        // COUNT(*) should count rows in the group; avoid aggregating a literal which Polars disallows.
                        debug!("[BY COUNT] Processing COUNT aggregate for column='{}', is_star={}", item.column, item.column == "*");
                        if item.column == "*" {
                            // Use a guaranteed non-null column to count rows per group
                            debug!("[BY COUNT] COUNT(*) using time_col='{}', casting to Int64", time_col);
                            col(&time_col).count().cast(DataType::Int64).alias("COUNT(*)")
                        } else {
                            debug!("[BY COUNT] COUNT(column) for '{}', casting to Int64", item.column);
                            base.count().cast(DataType::Int64).alias(format!("COUNT({})", item.column))
                        }
                    },
                    AggFunc::First => base.first().alias(format!("FIRST({})", item.column)),
                    AggFunc::Last => base.last().alias(format!("LAST({})", item.column)),
                    AggFunc::Stdev => base.std(1).alias(format!("STDEV({})", item.column)),
                    AggFunc::Delta => (base.clone().last() - base.first()).alias(format!("DELTA({})", item.column)),
                    AggFunc::Height => (base.clone().max() - base.min()).alias(format!("HEIGHT({})", item.column)),
                    AggFunc::Gradient => {
                        let num = base.clone().last() - base.first();
                        let den = col(&time_col).max() - col(&time_col).min();
                        (num.cast(DataType::Float64) / den.cast(DataType::Float64)).alias(format!("GRADIENT({})", item.column))
                    }
                    AggFunc::Quantile(cutoff) => {
                        let p = (*cutoff as f64) / 100.0;
                        let alias = format!("_{}_QUANTILE({})", cutoff, item.column);
                        base.quantile(lit(p), QuantileMethod::Nearest).alias(&alias)
                    }
                    AggFunc::ArrayAgg => {
                        // Collect values into PostgreSQL array format: {val1,val2,val3}
                        base.cast(DataType::String).implode().alias(format!("ARRAY_AGG({})", item.column))
                    }
                };
                if let Some(a) = &item.alias { e = e.alias(a); }
                agg_cols.push(e);
            } else if item.str_func.is_none() && item.expr.is_none() && item.column != "_time" {
                // Preserve non-aggregate projection columns by taking the first value in each bucket
                let qn = resolve_col_name_ctx(&df, ctx, &item.column).unwrap_or_else(|_| item.column.clone());
                let mut e = col(&qn).first().alias(&item.column);
                if let Some(a) = &item.alias { e = e.alias(a); }
                agg_cols.push(e);
            }
        }
        // ensure at least one aggregation
        let mut agg_cols = agg_cols;
        if agg_cols.is_empty() {
            for c in df.get_column_names() {
                if c.as_str() != time_col.as_str() {
                    agg_cols.push(col(c.as_str()).mean().alias(format!("AVG({})", c.as_str())));
                }
            }
        }
        let mut out = df
            .lazy()
            .group_by([col("_bucket")])
            .agg(agg_cols)
            .collect()?;
        // Rename bucket key to _time
        if out.get_column_names().iter().any(|c| c.as_str()=="_bucket") {
            let s = out.column("_bucket")?.clone();
            out = out.drop("_bucket")?;
            let mut s2 = s.clone();
            s2.rename("_time".into());
            out = out.hstack(&[s2])?;
        }
        // Apply HAVING on BY results if provided; ensure validation against current aggregated output
        let mut out = out.sort(["_time"], polars::prelude::SortMultipleOptions::default())?;
        if let Some(h) = &q.having_clause { out = apply_having_with_validation(out, h, ctx)?; }
        ctx.register_df_columns_for_stage(SelectStage::ByOrGroupBy, &out);
        return Ok(out);
    }

    // GROUP BY path
    if let Some(group_cols) = &q.group_by_cols {
        if q.select.iter().any(|i| i.str_func.is_some()) {
            anyhow::bail!("String functions are not supported with GROUP BY");
        }
        // Register current DF columns for stage-aware resolution prior to validations
        ctx.register_df_columns_for_stage(SelectStage::ByOrGroupBy, &df);
        // Clause-aware validation: referenced columns must exist at this stage; UDFs must be known
        // Validate non-aggregate selections are group-by columns or agg UDFs
        for item in &q.select {
            if item.func.is_none() && item.str_func.is_none() {
                let mut is_udf_agg = false;
                if let Some(ex) = &item.expr {
                    if let ArithExpr::Call { name: _name, .. } = ex {
                        // Treat function call expressions as aggregate UDFs; defer validation until execution.
                        is_udf_agg = true;
                    }
                }
                if let Some(_ex) = &item.expr {
                    if !is_udf_agg && item.column != "_time" {
                        anyhow::bail!("Non-aggregate expressions are not supported with GROUP BY; select group-by columns or aggregates");
                    }
                }
                if !is_udf_agg {
                    // Resolve the item column and group-by list for comparison
                    let item_resolved = ctx.resolve_column_at_stage(&df, &item.column, SelectStage::ByOrGroupBy).unwrap_or_else(|_| item.column.clone());
                    let group_cols_resolved: Vec<String> = group_cols.iter().map(|c| ctx.resolve_column_at_stage(&df, c, SelectStage::ByOrGroupBy).unwrap_or_else(|_| c.clone())).collect();
                    if item.column != "_time" && !group_cols_resolved.iter().any(|c| c.as_str().ends_with(item_resolved.as_str()) || c == &item_resolved) {
                        anyhow::bail!(format!("Column '{}' must appear in GROUP BY or be aggregated", item.column));
                    }
                }
            }
        }
        // Validate aggregate inputs (non-expr) exist (resolve via context)
        // Special-case COUNT(*) which does not reference a real column
        for item in &q.select {
            if let Some(func) = &item.func {
                if item.expr.is_none() {
                    if matches!(func, AggFunc::Count) && item.column == "*" {
                        // skip validation for COUNT(*)
                    } else if ctx.resolve_column_at_stage(&df, &item.column, SelectStage::ByOrGroupBy).is_err() {
                        return Err(crate::server::data_context::DataContext::column_not_found_error(&item.column, "GROUP BY", &df));
                    }
                }
            }
        }
        // Prepare NOTNULL set and optional pre-filter (resolved via context)
        let notnull_set: std::collections::HashSet<String> = q
            .group_by_notnull_cols
            .as_ref()
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();
        let mut lf = df.clone().lazy();
        if !notnull_set.is_empty() {
            let mut filter_expr: Option<Expr> = None;
            for c in &notnull_set {
                let rc = ctx.resolve_column_at_stage(&df, c, SelectStage::ByOrGroupBy).unwrap_or_else(|_| c.clone());
                let e = col(rc.as_str()).is_not_null();
                filter_expr = Some(match filter_expr { None => e, Some(acc) => acc.and(e) });
            }
            if let Some(f) = filter_expr { lf = lf.filter(f); }
        }
        // Resolve group-by columns using DataContext and build expressions
        tracing::debug!(target: "clarium::groupby", "GROUPBY input: rows={} cols={:?}", df.height(), df.get_column_names());
        if cfg!(debug_assertions) {
            // Log name -> dtype mapping to aid type mismatch investigations
            let mut dts: Vec<String> = Vec::new();
            for cname in df.get_column_names() {
                if let Ok(c) = df.column(cname.as_str()) { dts.push(format!("{}:{:?}", cname, c.dtype())); }
            }
            tracing::debug!(target: "clarium::groupby", "GROUPBY input dtypes: [{}]", dts.join(", "));
        }
        let resolved_group_cols: Vec<String> = group_cols
            .iter()
            .map(|c| ctx.resolve_column_at_stage(&df, c, SelectStage::ByOrGroupBy).unwrap_or_else(|_| c.clone()))
            .collect();
        let mut gb_exprs: Vec<Expr> = Vec::new();
        for c in &resolved_group_cols { gb_exprs.push(col(c.as_str())); }
        // Aggregations
        let mut agg_cols: Vec<Expr> = Vec::new();
        // Track aggregate UDF items to evaluate post-aggregation
        struct UdfAggPlan { base_name: String, func_name: String, ret_types: Vec<DataType>, args: Vec<ArithExpr> }
        let mut udf_plans: Vec<UdfAggPlan> = Vec::new();
        let time_col = ctx.resolve_column_at_stage(&df, "_time", SelectStage::ByOrGroupBy).unwrap_or_else(|_| "_time".to_string());
        // Helper: qualify arithmetic expressions against this stage
        fn qualify_arith_ctx(df: &DataFrame, ctx: &DataContext, a: &ArithExpr, clause: &str) -> anyhow::Result<ArithExpr> {            
            Ok(match a {
                AE::Term(AT::Col { name, previous: false }) => {
                    let qn = ctx.resolve_column_at_stage(df, name, SelectStage::ByOrGroupBy)
                        .map_err(|_| crate::server::data_context::DataContext::column_not_found_error(name, clause, df))?;
                    AE::Term(AT::Col { name: qn, previous: false })
                }
                AE::Term(_) => a.clone(),
                AE::Cast { expr, ty } => {
                    AE::Cast { expr: Box::new(qualify_arith_ctx(df, ctx, expr, clause)?), ty: ty.clone() }
                }
                AE::BinOp { left, op, right } => AE::BinOp { left: Box::new(qualify_arith_ctx(df, ctx, left, clause)?), op: op.clone(), right: Box::new(qualify_arith_ctx(df, ctx, right, clause)?) },
                AE::Concat(parts) => AE::Concat(parts.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()?),
                AE::Call { name, args } => AE::Call { name: name.clone(), args: args.iter().map(|p| qualify_arith_ctx(df, ctx, p, clause)).collect::<anyhow::Result<Vec<_>>>()? },
                AE::Func(dfm) => {                    
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
                AE::Predicate(_) => a.clone(),
                AE::Case { when_clauses, else_expr } => {
                    let qualified_when = when_clauses.iter().map(|(cond, val)| {
                        Ok((cond.clone(), qualify_arith_ctx(df, ctx, val, clause)?))
                    }).collect::<anyhow::Result<Vec<_>>>()?;
                    let qualified_else = else_expr.as_ref().map(|e| qualify_arith_ctx(df, ctx, e, clause)).transpose()?;
                    AE::Case { when_clauses: qualified_when, else_expr: qualified_else.map(Box::new) }
                }
            })
        }
        for item in &q.select {
            if let Some(func) = &item.func {
                // Build base expression only when needed; avoid creating literal expressions for COUNT(*)
                let base = if let Some(ex) = &item.expr {
                    debug!("[GROUPBY] Building aggregate with expr: func={:?}, expr={:?}", func, ex);
                    build_arith_expr(&qualify_arith_ctx(&df, ctx, ex, "GROUP BY")?, ctx)
                } else if matches!(func, AggFunc::Count) && item.column == "*" {
                    // Use a concrete column (time) as a safe base for count; we won't aggregate it directly.
                    col(&time_col)
                } else {
                    let qn = ctx
                        .resolve_column_at_stage(&df, &item.column, SelectStage::ByOrGroupBy)
                        .unwrap_or_else(|_| item.column.clone());
                    col(&qn)
                };
                tracing::debug!(target: "clarium::groupby", "GROUPBY agg build: func={:?} col='{}' alias={:?}", func, item.column, item.alias);
                let mut e = match func {
                    AggFunc::Avg => base.mean().alias(format!("AVG({})", item.column)),
                    AggFunc::Max => base.max().alias(format!("MAX({})", item.column)),
                    AggFunc::Min => base.min().alias(format!("MIN({})", item.column)),
                    AggFunc::Sum => base.sum().alias(format!("SUM({})", item.column)),
                    AggFunc::Count => {
                        debug!("[GROUPBY COUNT] Processing COUNT aggregate for column='{}', is_star={}", item.column, item.column == "*");
                        if item.column == "*" { 
                            debug!("[GROUPBY COUNT] COUNT(*) using time_col='{}', casting to Int64", time_col);
                            base.count().cast(DataType::Int64).alias("COUNT(*)") 
                        } else { 
                            debug!("[GROUPBY COUNT] COUNT(column) for '{}', casting to Int64", item.column);
                            base.count().cast(DataType::Int64).alias(format!("COUNT({})", item.column)) 
                        }
                    },
                    AggFunc::First => base.first().alias(format!("FIRST({})", item.column)),
                    AggFunc::Last => base.last().alias(format!("LAST({})", item.column)),
                    AggFunc::Stdev => base.std(1).alias(format!("STDEV({})", item.column)),
                    AggFunc::Delta => (base.clone().last() - base.first()).alias(format!("DELTA({})", item.column)),
                    AggFunc::Height => (base.clone().max() - base.min()).alias(format!("HEIGHT({})", item.column)),
                    AggFunc::Gradient => {
                        let num = base.clone().last() - base.first();
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
            } else if let Some(ex) = &item.expr {
                // Aggregate UDFs are handled post-aggregation. Detect and record plan.
                if let ArithExpr::Call { name, args } = ex {
                    let mut consider_as_aggregate = false;
                    let mut ret_types_hint: Vec<DataType> = Vec::new();
                    if let Some(reg) = get_script_registry() {
                        if let Some(meta) = reg.get_meta(name) {
                            if matches!(meta.kind, crate::scripts::ScriptKind::Aggregate) {
                                consider_as_aggregate = true;
                                ret_types_hint = meta.returns.clone();
                            }
                        } else {
                            // UDF not found in registry
                            anyhow::bail!("UDF '{}' not found in GROUP BY clause", name);
                        }
                    } else {
                        // Registry unavailable in this context; still attempt to treat as aggregate so tests relying
                        // on session-local snapshot work uniformly.
                        consider_as_aggregate = true;
                    }
                    if consider_as_aggregate {
                        // qualify args against this stage
                        let qargs: Vec<ArithExpr> = args.iter().map(|a| qualify_arith_ctx(&df, ctx, a, "GROUP BY")).collect::<anyhow::Result<Vec<_>>>().unwrap_or_else(|_| args.clone());
                        let base_name = item.alias.clone().unwrap_or_else(|| name.clone());
                        udf_plans.push(UdfAggPlan { base_name, func_name: name.clone(), ret_types: ret_types_hint, args: qargs });
                    }
                }
            }
        }
        // Always include group time bounds
        agg_cols.push(col(&time_col).min().alias("_start_time"));
        agg_cols.push(col(&time_col).max().alias("_end_time"));
        tracing::debug!(target: "clarium::groupby", "GROUPBY executing: gb_keys={:?}", resolved_group_cols);
        let mut out = lf.group_by(gb_exprs).agg(agg_cols).collect()?;
        tracing::debug!(target: "clarium::groupby", "GROUPBY raw out: rows={} cols={:?}", out.height(), out.get_column_names());
        debug!("[GROUPBY COUNT] After aggregation, checking schema for COUNT columns");
        if cfg!(debug_assertions) {
            let mut dts: Vec<String> = Vec::new();
            for cname in out.get_column_names() {
                if let Ok(c) = out.column(cname.as_str()) { 
                    let dtype_str = format!("{:?}", c.dtype());
                    dts.push(format!("{}:{}", cname, dtype_str));
                    if cname.as_str().starts_with("COUNT(") {
                        debug!("[GROUPBY COUNT] Column '{}' has dtype: {}", cname, dtype_str);
                    }
                }
            }
            tracing::debug!(target: "clarium::groupby", "GROUPBY raw out dtypes: [{}]", dts.join(", "));
        }
        // After aggregation, rename group-by key columns to their unqualified suffixes
        // Support both dotted (db.schema.table.col) and path-like (db/schema/table.col) qualifications.
        for gc in &resolved_group_cols {
            // Determine suffix after the last qualifier separator ('.', '/', '\\')
            let suffix = if let Some((_, suf)) = gc.rsplit_once('.') {
                Some(suf.to_string())
            } else if let Some((_, suf)) = gc.rsplit_once('/') {
                Some(suf.to_string())
            } else if let Some((_, suf)) = gc.rsplit_once('\\') {
                Some(suf.to_string())
            } else { None };
            if let Some(suf) = suffix {
                // Attempt a direct rename from the fully-resolved name to suffix
                let _ = out.rename(gc.as_str(), suf.clone().into());
                // If the direct rename didn't take (because the output used a different qualified
                // spelling), scan columns for a single match that ends with the suffix and rename it.
                let cols_now = out.get_column_names();
                let has_suffix = cols_now.iter().any(|c| c.as_str() == suf.as_str());
                if !has_suffix {
                    // Find candidates that end with .suffix or /suffix or \\suffix
                    let dot = format!(".{}", suf);
                    let fwd = format!("/{}", suf);
                    let bwd = format!("\\{}", suf);
                    let mut cand: Option<String> = None;
                    for c in cols_now {
                        let cs = c.as_str();
                        if cs.ends_with(dot.as_str()) || cs.ends_with(fwd.as_str()) || cs.ends_with(bwd.as_str()) {
                            cand = Some(cs.to_string());
                            break;
                        }
                    }
                    if let Some(from) = cand {
                        let _ = out.rename(from.as_str(), suf.into());
                    }
                }
            }
        }
        // Defer HAVING until after aggregate UDFs are evaluated so HAVING can reference their outputs
        // Sort for stable output; if NOTNULL used, sort by _start_time else by resolved group cols
        let mut out = if !notnull_set.is_empty() {
            out.sort(["_start_time"], polars::prelude::SortMultipleOptions::default())?
        } else if !resolved_group_cols.is_empty() {
            // sort by the (potentially renamed) columns
            let sort_refs: Vec<String> = resolved_group_cols
                .iter()
                .map(|s| {
                    if let Some((_, suf)) = s.rsplit_once('.') { suf.to_string() }
                    else if let Some((_, suf)) = s.rsplit_once('/') { suf.to_string() }
                    else if let Some((_, suf)) = s.rsplit_once('\\') { suf.to_string() }
                    else { s.clone() }
                })
                .collect();
            let refs: Vec<&str> = sort_refs.iter().map(|s| s.as_str()).collect();
            out.sort(refs, polars::prelude::SortMultipleOptions::default())?
        } else { out };
        tracing::debug!(target: "clarium::groupby", "GROUPBY out (post-rename/sort) rows={} cols={:?}", out.height(), out.get_column_names());
        if cfg!(debug_assertions) {
            let mut dts: Vec<String> = Vec::new();
            for cname in out.get_column_names() {
                if let Ok(c) = out.column(cname.as_str()) { dts.push(format!("{}:{:?}", cname, c.dtype())); }
            }
            tracing::debug!(target: "clarium::groupby", "GROUPBY out dtypes: [{}]", dts.join(", "));
        }
        // Evaluate aggregate UDF plans, if any
        if !udf_plans.is_empty() {
            // Track names of columns we append to register as user-generated columns for this stage
            let mut appended_cols: Vec<String> = Vec::new();
            // Build group membership map from original df
            let nrows = df.height();
            // Prepare resolved group columns on original df
            let mut grp_cols_series: Vec<Column> = Vec::new();
            for c in &resolved_group_cols { grp_cols_series.push(df.column(c.as_str())?.clone()); }
            let mut group_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
            for i in 0..nrows {
                // stringify group key as joined with '\u{1F}' separator
                let mut parts: Vec<String> = Vec::with_capacity(grp_cols_series.len());
                for s in &grp_cols_series {
                    let v = s.get(i).unwrap_or(polars::prelude::AnyValue::Null);
                    parts.push(match v {
                        polars::prelude::AnyValue::Null => "<NULL>".to_string(),
                        polars::prelude::AnyValue::Int64(v) => v.to_string(),
                        polars::prelude::AnyValue::Float64(v) => v.to_string(),
                        polars::prelude::AnyValue::String(s) => s.to_string(),
                        polars::prelude::AnyValue::StringOwned(ref s) => s.to_string(),
                        _ => v.to_string(),
                    });
                }
                let key = parts.join("\u{1F}");
                group_map.entry(key).or_default().push(i);
            }
            // Evaluate arg expressions over the original df once
            use crate::server::exec::exec_common::build_arith_expr;
            let mut arg_eval_cache: std::collections::HashMap<usize, DataFrame> = std::collections::HashMap::new();
            for (pi, plan) in udf_plans.iter().enumerate() {
                // Build a df with each arg as a column __argN
                let mut exprs: Vec<Expr> = Vec::with_capacity(plan.args.len());
                for (ai, a) in plan.args.iter().enumerate() { exprs.push(build_arith_expr(a, ctx).alias(format!("__arg{}", ai))); }
                let arg_df = df.clone().lazy().select(exprs).collect()?;
                arg_eval_cache.insert(pi, arg_df);
            }
            // Build mapping from out rows to group key
            let mut out_keys: Vec<String> = Vec::with_capacity(out.height());
            if !resolved_group_cols.is_empty() {
                // columns in out are renamed to suffixes; compute them (handle '.', '/', '\\')
                let out_key_cols: Vec<String> = resolved_group_cols
                    .iter()
                    .map(|s| {
                        if let Some((_, suf)) = s.rsplit_once('.') { suf.to_string() }
                        else if let Some((_, suf)) = s.rsplit_once('/') { suf.to_string() }
                        else if let Some((_, suf)) = s.rsplit_once('\\') { suf.to_string() }
                        else { s.clone() }
                    })
                    .collect();
                for i in 0..out.height() {
                    let mut parts: Vec<String> = Vec::with_capacity(out_key_cols.len());
                    for oc in &out_key_cols {
                        let v = out.column(oc.as_str())?.get(i).unwrap_or(polars::prelude::AnyValue::Null);
                        parts.push(match v {
                            polars::prelude::AnyValue::Null => "<NULL>".to_string(),
                            polars::prelude::AnyValue::Int64(v) => v.to_string(),
                            polars::prelude::AnyValue::Float64(v) => v.to_string(),
                            polars::prelude::AnyValue::String(s) => s.to_string(),
                            polars::prelude::AnyValue::StringOwned(ref s) => s.to_string(),
                            _ => v.to_string(),
                        });
                    }
                    out_keys.push(parts.join("\u{1F}"));
                }
            } else {
                // No group columns: single group with all rows
                out_keys.resize(out.height(), "__ALL__".to_string());
                group_map.insert("__ALL__".to_string(), (0..nrows).collect());
            }
            // For each UDF plan, compute per-group outputs and append
            for (pi, plan) in udf_plans.iter().enumerate() {
                let arg_df = arg_eval_cache.get(&pi).unwrap();
                // Prepare result vectors per return. When metadata is missing (empty ret_types),
                // we will lazily initialize on the first successful call by inspecting the Lua result.
                let mut dyn_ret_types: Vec<DataType> = plan.ret_types.clone();
                let mut res_cols_bool: Vec<Vec<Option<bool>>> = Vec::new();
                let mut res_cols_i64: Vec<Vec<Option<i64>>> = Vec::new();
                let mut res_cols_f64: Vec<Vec<Option<f64>>> = Vec::new();
                let mut res_cols_str: Vec<Vec<Option<String>>> = Vec::new();
                let mut vectors_inited: bool = false;
                let null_on_err = crate::system::get_null_on_error();
                for (row_idx, gk) in out_keys.iter().enumerate() {
                    let mut rows = group_map.get(gk).cloned().unwrap_or_else(Vec::new);
                    // Fallback: if no rows found due to key formatting differences, derive rows by matching values from 'out' to original df
                    if rows.is_empty() && !resolved_group_cols.is_empty() {
                        // Compute suffix names used in 'out' for group columns
                        let out_key_cols: Vec<String> = resolved_group_cols
                            .iter()
                            .map(|s| {
                                if let Some((_, suf)) = s.rsplit_once('.') { suf.to_string() }
                                else if let Some((_, suf)) = s.rsplit_once('/') { suf.to_string() }
                                else if let Some((_, suf)) = s.rsplit_once('\\') { suf.to_string() }
                                else { s.clone() }
                            })
                            .collect();
                        // Read target values for this group from 'out'
                        let mut target_vals: Vec<String> = Vec::with_capacity(out_key_cols.len());
                        for oc in &out_key_cols {
                            let v = out.column(oc.as_str()).ok().and_then(|c| c.get(row_idx).ok()).unwrap_or(AnyValue::Null);
                            let s = match v {
                                AnyValue::Null => "<NULL>".to_string(),
                                AnyValue::Int64(v) => v.to_string(),
                                AnyValue::Float64(v) => v.to_string(),
                                AnyValue::String(s) => s.to_string(),
                                AnyValue::StringOwned(ref s) => s.to_string(),
                                _ => v.to_string(),
                            };
                            target_vals.push(s);
                        }
                        // Scan original df rows and collect those matching all group column values
                        'scan: for i in 0..nrows {
                            for (gi, gc) in resolved_group_cols.iter().enumerate() {
                                let v = df.column(gc.as_str()).ok().and_then(|c| c.get(i).ok()).unwrap_or(AnyValue::Null);
                                let s = match v {
                                    AnyValue::Null => "<NULL>".to_string(),
                                    AnyValue::Int64(v) => v.to_string(),
                                    AnyValue::Float64(v) => v.to_string(),
                                    AnyValue::String(st) => st.to_string(),
                                    AnyValue::StringOwned(ref st) => st.to_string(),
                                    _ => v.to_string(),
                                };
                                if s != target_vals[gi] { continue 'scan; }
                            }
                            rows.push(i);
                        }
                    }
                    // Build JSON arrays for args
                    let mut jargs: Vec<serde_json::Value> = Vec::with_capacity(plan.args.len());
                    for (ai, _aexpr) in plan.args.iter().enumerate() {
                        let s = arg_df.column(&format!("__arg{}", ai)).unwrap();
                        let mut arr: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
                        for &r in &rows {
                            let av = s.get(r).unwrap_or(polars::prelude::AnyValue::Null);
                            let jv = match av {
                                polars::prelude::AnyValue::Null => serde_json::Value::Null,
                                polars::prelude::AnyValue::Boolean(b) => serde_json::json!(b),
                                polars::prelude::AnyValue::Int64(v) => serde_json::json!(v),
                                polars::prelude::AnyValue::Float64(v) => serde_json::json!(v),
                                polars::prelude::AnyValue::String(s) => serde_json::json!(s),
                                polars::prelude::AnyValue::StringOwned(ref s) => serde_json::json!(s),
                                _ => serde_json::Value::Null,
                            };
                            arr.push(jv);
                        }
                        jargs.push(serde_json::Value::Array(arr));
                    }
                    // Capture a stable registry snapshot to avoid TLS loss on rayon worker threads
                    let reg_snapshot = crate::scripts::get_script_registry().and_then(|r| r.snapshot().ok());
                    // Call Lua (aggregate variant: map NULLs to real Lua nil) using the captured snapshot
                    let call = if let Some(reg) = &reg_snapshot { reg.call_function_json_aggregate(&plan.func_name, &jargs) } else { Err(anyhow::anyhow!("Lua registry not initialized")) };
                    match call {
                        Ok(v) => {
                            // If return types unknown, infer from first successful value
                            if dyn_ret_types.is_empty() {
                                // If array, infer per element; else single value
                                if let Some(arr) = v.as_array() {
                                    let mut inferred: Vec<DataType> = Vec::with_capacity(arr.len());
                                    for elem in arr.iter() {
                                        let dt = if elem.is_boolean() { DataType::Boolean }
                                                 else if elem.is_i64() { DataType::Int64 }
                                                 else if elem.is_f64() { DataType::Float64 }
                                                 else { DataType::String };
                                        inferred.push(dt);
                                    }
                                    if inferred.is_empty() { inferred.push(DataType::Null); }
                                    dyn_ret_types = inferred;
                                } else {
                                    let dt = if v.is_boolean() { DataType::Boolean }
                                             else if v.is_i64() { DataType::Int64 }
                                             else if v.is_f64() { DataType::Float64 }
                                             else { DataType::String };
                                    dyn_ret_types = vec![dt];
                                }
                                // Initialize vectors now that we know types
                                if !vectors_inited {
                                    for dt in &dyn_ret_types {
                                        match dt {
                                            DataType::Boolean => res_cols_bool.push(Vec::with_capacity(out.height())),
                                            DataType::Int64 => res_cols_i64.push(Vec::with_capacity(out.height())),
                                            DataType::Float64 => res_cols_f64.push(Vec::with_capacity(out.height())),
                                            _ => res_cols_str.push(Vec::with_capacity(out.height())),
                                        }
                                    }
                                    vectors_inited = true;
                                }
                            } else if !vectors_inited {
                                for dt in &dyn_ret_types {
                                    match dt {
                                        DataType::Boolean => res_cols_bool.push(Vec::with_capacity(out.height())),
                                        DataType::Int64 => res_cols_i64.push(Vec::with_capacity(out.height())),
                                        DataType::Float64 => res_cols_f64.push(Vec::with_capacity(out.height())),
                                        _ => res_cols_str.push(Vec::with_capacity(out.height())),
                                    }
                                }
                                vectors_inited = true;
                            }
                            if dyn_ret_types.len() <= 1 {
                                let dt = dyn_ret_types.first().cloned().unwrap_or(DataType::String);
                                match dt {
                                    DataType::Boolean => if let Some(b) = v.as_bool() { res_cols_bool[0].push(Some(b)); } else { res_cols_bool[0].push(None); },
                                    DataType::Int64 => {
                                        if let Some(i) = v.as_i64() { res_cols_i64[0].push(Some(i)); }
                                        else if let Some(f) = v.as_f64() { res_cols_i64[0].push(Some(f as i64)); }
                                        else { res_cols_i64[0].push(None); }
                                    },
                                    DataType::Float64 => if let Some(f) = v.as_f64() { res_cols_f64[0].push(Some(f)); } else if let Some(i) = v.as_i64() { res_cols_f64[0].push(Some(i as f64)); } else { res_cols_f64[0].push(None); },
                                    _ => if let Some(s) = v.as_str() { res_cols_str[0].push(Some(s.to_string())); } else { res_cols_str[0].push(None); },
                                }
                            } else {
                                for (ri, dt) in dyn_ret_types.iter().enumerate() {
                                    let elem = v.get(ri);
                                    match dt {
                                        DataType::Boolean => res_cols_bool[ri].push(elem.and_then(|x| x.as_bool())),
                                        DataType::Int64 => {
                                            if let Some(i) = elem.and_then(|x| x.as_i64()) { res_cols_i64[ri].push(Some(i)); }
                                            else if let Some(f) = elem.and_then(|x| x.as_f64()) { res_cols_i64[ri].push(Some(f as i64)); }
                                            else { res_cols_i64[ri].push(None); }
                                        },
                                        DataType::Float64 => {
                                            if let Some(f) = elem.and_then(|x| x.as_f64()) { res_cols_f64[ri].push(Some(f)); }
                                            else if let Some(i) = elem.and_then(|x| x.as_i64()) { res_cols_f64[ri].push(Some(i as f64)); }
                                            else { res_cols_f64[ri].push(None); }
                                        },
                                        _ => res_cols_str[ri].push(elem.and_then(|x| x.as_str().map(|s| s.to_string()))),
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            if !null_on_err { anyhow::bail!(format!("UDF '{}' error", plan.func_name)); }
                            // Ensure result vectors are initialized before pushing nulls.
                            // Case A: return types unknown yet — assume single String column for alignment
                            if dyn_ret_types.is_empty() {
                                dyn_ret_types = vec![DataType::String];
                            }
                            // If vectors not inited (e.g., all prior rows errored), initialize now based on dyn_ret_types
                            if !vectors_inited {
                                for dt in &dyn_ret_types {
                                    match dt {
                                        DataType::Boolean => res_cols_bool.push(Vec::with_capacity(out.height())),
                                        DataType::Int64 => res_cols_i64.push(Vec::with_capacity(out.height())),
                                        DataType::Float64 => res_cols_f64.push(Vec::with_capacity(out.height())),
                                        _ => res_cols_str.push(Vec::with_capacity(out.height())),
                                    }
                                }
                                vectors_inited = true;
                            }
                            // Push nulls matching current arity
                            for v in res_cols_bool.iter_mut() { v.push(None); }
                            for v in res_cols_i64.iter_mut() { v.push(None); }
                            for v in res_cols_f64.iter_mut() { v.push(None); }
                            for v in res_cols_str.iter_mut() { v.push(None); }
                        }
                    }
                }
                // Build Series and append
                if dyn_ret_types.len() <= 1 {
                    let dt = dyn_ret_types.first().cloned().unwrap_or(DataType::String);
                    let name = plan.base_name.clone();
                    let col: Column = match dt {
                        DataType::Boolean => Series::new(name.clone().into(), res_cols_bool.remove(0)).into(),
                        DataType::Int64 => Series::new(name.clone().into(), res_cols_i64.remove(0)).into(),
                        DataType::Float64 => Series::new(name.clone().into(), res_cols_f64.remove(0)).into(),
                        _ => Series::new(name.clone().into(), res_cols_str.remove(0)).into(),
                    };
                    out = out.hstack(&[col])?;
                    appended_cols.push(name);
                } else {
                    for (ri, dt) in dyn_ret_types.iter().enumerate() {
                        let name = format!("{}_{}", plan.base_name, ri);
                        let col: Column = match dt {
                            DataType::Boolean => Series::new(name.clone().into(), res_cols_bool.remove(0)).into(),
                            DataType::Int64 => Series::new(name.clone().into(), res_cols_i64.remove(0)).into(),
                            DataType::Float64 => Series::new(name.clone().into(), res_cols_f64.remove(0)).into(),
                            _ => Series::new(name.clone().into(), res_cols_str.remove(0)).into(),
                        };
                        out = out.hstack(&[col])?;
                        appended_cols.push(name);
                    }
                }
            }
            // Register appended UDF columns so subsequent stages can resolve them via DataContext
            if !appended_cols.is_empty() {
                ctx.register_user_columns_for_stage(SelectStage::ByOrGroupBy, appended_cols);
            }
        }
        // HAVING is applied by run_select after GROUP BY stage; do not apply here.
        ctx.register_df_columns_for_stage(SelectStage::ByOrGroupBy, &out);
        return Ok(out);
    }

    // Fallback: passthrough
    ctx.register_df_columns_for_stage(SelectStage::ByOrGroupBy, &df);
    Ok(df)
}
