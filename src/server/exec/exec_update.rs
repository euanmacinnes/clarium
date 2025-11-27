//! exec_update
//! -----------
//! SQL UPDATE implementation extracted from exec.rs. Handles partially-qualified
//! identifiers via normalization performed earlier, evaluates WHERE (with
//! subqueries), and applies constant assignments type-safely.

use anyhow::Result;
use polars::prelude::*;

use crate::{query, server::exec::{where_subquery::{eval_where_mask, where_contains_subquery}, exec_common::build_where_expr, df_utils::read_df_or_kv}};
use crate::storage::SharedStore;

pub fn handle_update(store: &SharedStore, table: String, assignments: Vec<(String, query::ArithTerm)>, where_clause: Option<query::WhereExpr>) -> Result<serde_json::Value> {
    // Load existing dataframe (works for regular and time tables)
    let mut df_all = read_df_or_kv(store, &table)?;
    let n = df_all.height();
    if n == 0 {
        return Ok(serde_json::json!({"status":"ok","updated":0}));
    }
    // Build mask: rows to update
    let mask_bool = if let Some(w) = &where_clause {
        let registry_snapshot = crate::scripts::get_script_registry().and_then(|r| r.snapshot().ok());
        let mut ctx = crate::server::data_context::DataContext::with_defaults("clarium", "public");
        if let Some(reg) = registry_snapshot { ctx.script_registry = Some(reg); }
        if where_contains_subquery(w) {
            eval_where_mask(&df_all, &ctx, store, w)?
        } else {
            let mask_df = df_all.clone().lazy().select([build_where_expr(w, &ctx).alias("__m__")]).collect()?;
            mask_df.column("__m__")?.bool()?.clone()
        }
    } else {
        // All true
        let mut v: Vec<bool> = Vec::with_capacity(n);
        v.resize(n, true);
        BooleanChunked::from_vec("__m__", v)
    };

    // Apply assignments one by one
    for (col, term) in assignments {
        // If column doesn't exist yet, add an all-null series with an inferred type
        let exists = df_all.get_column_names().iter().any(|c| c.as_str() == col);
        if !exists {
            let s: Series = match term {
                query::ArithTerm::Number(_) => Series::new_null(col.clone().into(), n).cast(&DataType::Float64)?,
                query::ArithTerm::Str(_) => Series::new_null(col.clone().into(), n).cast(&DataType::String)?,
                query::ArithTerm::Null => Series::new_null(col.clone().into(), n),
                _ => Series::new_null(col.clone().into(), n),
            };
            df_all.with_column(s)?;
        }
        let s = df_all.column(col.as_str())?;
        let dt = s.dtype().clone();
        let len = s.len();
        // Build updated series according to dtype
        let new_series: Series = match dt {
            DataType::Int64 => {
                let ca = s.i64()?;
                let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
                for i in 0..len {
                    let do_set = mask_bool.get(i).unwrap_or(false);
                    if do_set {
                        let v = match &term {
                            query::ArithTerm::Number(n) => Some(*n as i64),
                            query::ArithTerm::Str(st) => st.parse::<i64>().ok(),
                            query::ArithTerm::Null => None,
                            _ => None,
                        };
                        out.push(v);
                    } else {
                        out.push(ca.get(i));
                    }
                }
                Series::new(col.clone().into(), out)
            }
            DataType::Float64 => {
                let ca = s.f64()?;
                let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                for i in 0..len {
                    let do_set = mask_bool.get(i).unwrap_or(false);
                    if do_set {
                        let v = match &term {
                            query::ArithTerm::Number(n) => Some(*n),
                            query::ArithTerm::Str(st) => st.parse::<f64>().ok(),
                            query::ArithTerm::Null => None,
                            _ => None,
                        };
                        out.push(v);
                    } else {
                        out.push(ca.get(i));
                    }
                }
                Series::new(col.clone().into(), out)
            }
            DataType::String => {
                let mut out: Vec<Option<String>> = Vec::with_capacity(len);
                for i in 0..len {
                    let do_set = mask_bool.get(i).unwrap_or(false);
                    if do_set {
                        let v = match &term {
                            query::ArithTerm::Number(n) => Some(n.to_string()),
                            query::ArithTerm::Str(st) => Some(st.clone()),
                            query::ArithTerm::Null => None,
                            _ => None,
                        };
                        out.push(v);
                    } else {
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
                // Fallback: treat as string
                let mut out: Vec<Option<String>> = Vec::with_capacity(len);
                for i in 0..len {
                    let do_set = mask_bool.get(i).unwrap_or(false);
                    if do_set {
                        let v = match &term {
                            query::ArithTerm::Number(n) => Some(n.to_string()),
                            query::ArithTerm::Str(st) => Some(st.clone()),
                            query::ArithTerm::Null => None,
                        _ => None,
                        };
                        out.push(v);
                    } else {
                        out.push(None);
                    }
                }
                Series::new(col.clone().into(), out)
            }
        };
        // Replace/insert column
        df_all.replace(col.as_str(), new_series)?;
    }
    let guard = store.0.lock();
    guard.rewrite_table_df(&table, df_all)?;
    Ok(serde_json::json!({"status":"ok"}))
}
