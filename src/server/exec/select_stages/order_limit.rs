//! ORDER BY / LIMIT stage

use anyhow::Result;
use polars::prelude::*;
use crate::tprintln;

use crate::query::Query;
use crate::server::data_context::{DataContext, SelectStage};
use crate::system; // for strict_projection flag

pub fn order_limit(mut df: DataFrame, q: &Query, ctx: &mut DataContext) -> Result<DataFrame> {
    // Resolve ORDER BY columns. When strict projection is disabled, tolerate
    // ORDER BY on columns that are not part of the final projection by
    // skipping those sort keys instead of erroring. This preserves historical
    // behavior expected by tests which allow ORDER BY on non-projected columns.
    if let Some(ob) = &q.order_by {
        if !ob.is_empty() {
            let strict = system::get_strict_projection();
            // In strict mode, validate that no ORDER BY columns are temporary (not in original SELECT)
            if strict && !ctx.temp_order_by_columns.is_empty() {
                for (name, _asc) in ob.iter() {
                    if ctx.temp_order_by_columns.contains(name) {
                        return Err(
                            crate::server::data_context::DataContext::column_not_found_error(
                                name,
                                "ORDER BY",
                                &df,
                            ),
                        );
                    }
                }
            }
            let mut exprs: Vec<Expr> = Vec::new();
            let mut descending: Vec<bool> = Vec::new();
            for (name, asc) in ob.iter() {
                // Try to resolve the ORDER BY column against current DF/Context
                match ctx.resolve_column_at_stage(&df, name, SelectStage::OrderLimit) {
                    Ok(resolved) => {
                        exprs.push(col(resolved.as_str()));
                        descending.push(!asc);
                    }
                    Err(_) => {
                        // Column not found by resolver. In loose (non-strict) mode, we skip
                        // unknown ORDER BY keys. In strict mode, surface a clause-specific error.
                        if strict {
                            return Err(
                                crate::server::data_context::DataContext::column_not_found_error(
                                    name,
                                    "ORDER BY",
                                    &df,
                                ),
                            );
                        } else {
                            // Best-effort diagnostic to help track behavior during tests
                            tprintln!(
                                "[ORDER_LIMIT] Skipping unknown ORDER BY key '{}' (strict_projection=false)",
                                name
                            );
                            continue;
                        }
                    }
                }
            }
            if !exprs.is_empty() {
                let nulls_last: Vec<bool> = vec![true; exprs.len()];
                let opts = polars::prelude::SortMultipleOptions { descending, nulls_last, maintain_order: true, multithreaded: true, limit: None };
                df = df.lazy().sort_by_exprs(exprs, opts).collect()?;
            }
            // In loose mode, drop temporary ORDER BY columns that were added for sorting
            if !strict && !ctx.temp_order_by_columns.is_empty() {
                let cols_to_keep: Vec<String> = df.get_column_names()
                    .iter()
                    .filter(|c| !ctx.temp_order_by_columns.contains(c.as_str()))
                    .map(|c| c.to_string())
                    .collect();
                if !cols_to_keep.is_empty() {
                    df = df.select(&cols_to_keep)?;
                }
            }
        }
    }
    // Apply LIMIT locally (mirror df_utils::apply_order_and_limit)
    if let Some(n) = q.limit {
        let h = df.height();
        if n > 0 {
            let m = n as usize;
            if m < h { df = df.slice(0, m); }
        } else if n < 0 {
            let m = (-n) as usize;
            if m < h {
                let start = (h - m) as i64;
                df = df.slice(start, m);
            }
        } else {
            df = df.slice(0, 0);
        }
    }
    ctx.register_df_columns_for_stage(SelectStage::OrderLimit, &df);
    Ok(df)
}
