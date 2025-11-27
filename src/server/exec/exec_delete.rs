//! exec_delete
//! -----------
//! DELETE COLUMNS implementation extracted from exec.rs. Keeps dispatcher thin.

use anyhow::Result;
use polars::prelude::*;

use crate::server::exec::{where_subquery::{eval_where_mask, where_contains_subquery}, exec_common::build_where_expr, df_utils::read_df_or_kv};
use crate::storage::SharedStore;

pub fn handle_delete_columns(store: &SharedStore, database: String, mut columns: Vec<String>, where_clause: Option<crate::query::WhereExpr>) -> Result<serde_json::Value> {
    // Load full dataframe
    let mut df_all = read_df_or_kv(store, &database)?;
    if df_all.width() == 0 { return Ok(serde_json::json!({"status":"ok"})); }

    // Normalize column list (dedup)
    columns.sort();
    columns.dedup();

    let new_df = if let Some(w) = &where_clause {
        // Build mask (with subquery support)
        let registry_snapshot = crate::scripts::get_script_registry().and_then(|r| r.snapshot().ok());
        let mut ctx = crate::server::data_context::DataContext::with_defaults("clarium", "public");
        if let Some(reg) = registry_snapshot { ctx.script_registry = Some(reg); }
        let mask = if where_contains_subquery(w) {
            eval_where_mask(&df_all, &ctx, store, w)?
        } else {
            let mask_df = df_all.clone().lazy().select([build_where_expr(w, &ctx).alias("__m__")]).collect()?;
            mask_df.column("__m__")?.bool()?.clone()
        };
        // For each requested column, set NULL where mask is true
        for col in &columns {
            if let Ok(s) = df_all.column(col.as_str()) {
                let len = s.len();
                let dtype = s.dtype().clone();
                let new_series: Series = match dtype {
                    DataType::Int64 => {
                        let ca = s.i64()?;
                        let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
                        for i in 0..len { if mask.get(i).unwrap_or(false) { out.push(None) } else { out.push(ca.get(i)); } }
                        Series::new(col.clone().into(), out)
                    }
                    DataType::Float64 => {
                        let ca = s.f64()?;
                        let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                        for i in 0..len { if mask.get(i).unwrap_or(false) { out.push(None) } else { out.push(ca.get(i)); } }
                        Series::new(col.clone().into(), out)
                    }
                    DataType::String => {
                        let mut out: Vec<Option<String>> = Vec::with_capacity(len);
                        for i in 0..len {
                            if mask.get(i).unwrap_or(false) { out.push(None) } else {
                                match s.get(i) {
                                    Ok(AnyValue::StringOwned(v)) => out.push(Some(v.to_string())),
                                    Ok(AnyValue::String(v)) => out.push(Some(v.to_string())),
                                    Ok(AnyValue::Null) => out.push(None),
                                    _ => out.push(None),
                                }
                            }
                        }
                        Series::new(col.clone().into(), out)
                    }
                    _ => {
                        // Fallback: set to NULLs where masked, keep as string otherwise
                        let mut out: Vec<Option<String>> = Vec::with_capacity(len);
                        for i in 0..len { if mask.get(i).unwrap_or(false) { out.push(None) } else { out.push(None) } }
                        Series::new(col.clone().into(), out)
                    }
                };
                df_all.replace_or_add(&new_series)?;
            }
        }
        df_all
    } else {
        // Drop columns entirely
        let mut keep_cols: Vec<Series> = Vec::new();
        for name in df_all.get_column_names() {
            if !columns.iter().any(|c| c.as_str() == name.as_str()) {
                keep_cols.push(df_all.column(name.as_str())?.clone());
            }
        }
        DataFrame::new(keep_cols.into_iter().map(|s| s.into()).collect())?
    };
    let guard = store.0.lock();
    guard.rewrite_table_df(&database, new_df)?;
    Ok(serde_json::json!({"status": "ok"}))
}
