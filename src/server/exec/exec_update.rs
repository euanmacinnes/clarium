//! exec_update
//! -----------
//! SQL UPDATE implementation extracted from exec.rs. Handles partially-qualified
//! identifiers via normalization performed earlier, evaluates WHERE (with
//! subqueries), and applies constant assignments type-safely.

use anyhow::Result;
use polars::prelude::*;

use crate::{server::query, server::exec::{where_subquery::{eval_where_mask, where_contains_subquery}, exec_common::build_where_expr, df_utils::read_df_or_kv}};
use crate::storage::SharedStore;

pub fn handle_update(store: &SharedStore, table: String, assignments: Vec<(String, query::ArithTerm)>, where_clause: Option<query::WhereExpr>) -> Result<serde_json::Value> {
    let __t0 = std::time::Instant::now();
    // Load existing dataframe (works for regular and time tables)
    let __t_read = std::time::Instant::now();
    let mut df_all = read_df_or_kv(store, &table)?;
    crate::tprintln!("[EXEC_UPDATE] read_df rows={} cols={} took={:?}", df_all.height(), df_all.width(), __t_read.elapsed());
    let n = df_all.height();
    if n == 0 {
        return Ok(serde_json::json!({"status":"ok","updated":0}));
    }
    // Fetch primary key and partitions metadata (for regular tables)
    let (pk_cols_opt, partitions_cols): (Option<Vec<String>>, Vec<String>) = {
        let g = store.0.lock();
        (g.get_primary_key(&table), g.get_partitions(&table))
    };
    // Build mask: rows to update
    let __t_mask = std::time::Instant::now();
    let mask_bool = if let Some(w) = &where_clause {
        let registry_snapshot = crate::scripts::get_script_registry().and_then(|r| r.snapshot().ok());
        let mut ctx = crate::server::data_context::DataContext::with_defaults(
            crate::ident::DEFAULT_DB,
            crate::ident::DEFAULT_SCHEMA,
        );
        if let Some(reg) = registry_snapshot { ctx.script_registry = Some(reg); }
        if where_contains_subquery(w) {
            eval_where_mask(&df_all, &ctx, store, w)?
        } else {
            let mask_df = df_all.clone().lazy().select([build_where_expr(w, &ctx).alias("__m__")]).collect()?;
            mask_df.column("__m__")?.bool()?.clone()
        }
    } else {
        // All true
        let v: Vec<bool> = vec![true; n];
        BooleanChunked::from_slice("__m__".into(), &v)
    };
    crate::tprintln!("[EXEC_UPDATE] build_mask rows={} took={:?}", n, __t_mask.elapsed());

    // Determine whether assignments touch primary key columns or partition columns
    let mut pk_touched = false;
    let mut partitions_touched = false;
    for (col, _term) in &assignments {
        if let Some(pk_cols) = &pk_cols_opt { if pk_cols.iter().any(|c| c == col) { pk_touched = true; } }
        if !partitions_cols.is_empty() && partitions_cols.iter().any(|c| c == col) { partitions_touched = true; }
    }

    // Apply assignments one by one
    let __t_assign = std::time::Instant::now();
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
    crate::tprintln!("[EXEC_UPDATE] apply_assignments rows={} took={:?}", n, __t_assign.elapsed());
    // If PK columns were touched, validate non-null and uniqueness across all rows
    let __t_pk = std::time::Instant::now();
    if let Some(pk_cols) = &pk_cols_opt {
        if pk_touched && !pk_cols.is_empty() {
            use std::collections::HashSet;
            let mut seen: HashSet<String> = HashSet::with_capacity(df_all.height());
            let mut key_buf = String::new();
            for i in 0..df_all.height() {
                key_buf.clear();
                let mut first = true;
                for c in pk_cols {
                    if !df_all.get_column_names().iter().any(|n| n.as_str() == c) {
                        anyhow::bail!(format!("UPDATE references missing primary key column '{}'", c));
                    }
                    let av = df_all.column(c.as_str())?.get(i).ok();
                    if matches!(av, Some(AnyValue::Null) | None) {
                        anyhow::bail!("PRIMARY KEY cannot be NULL");
                    }
                    let sval = match av.unwrap() {
                        AnyValue::String(s) => s.to_string(),
                        AnyValue::StringOwned(s) => s.to_string(),
                        AnyValue::Int64(v) => v.to_string(),
                        AnyValue::Float64(f) => {
                            let mut s = format!("{}", f);
                            if s.contains('.') { s = s.trim_end_matches('0').trim_end_matches('.').to_string(); }
                            s
                        }
                        v => v.to_string(),
                    };
                    if !first { key_buf.push(','); }
                    first = false;
                    key_buf.push_str(c);
                    key_buf.push('=');
                    key_buf.push_str(&sval);
                }
                if !seen.insert(key_buf.clone()) {
                    anyhow::bail!("PRIMARY KEY violation: duplicate key after UPDATE");
                }
            }
            crate::tprintln!("[EXEC_UPDATE] pk_validate rows={} took={:?}", df_all.height(), __t_pk.elapsed());
        }
    }
    let guard = store.0.lock();
    // rewrite_table_df for regular tables is partition-aware now; time tables path is unchanged
    let __t_rewrite = std::time::Instant::now();
    guard.rewrite_table_df(&table, df_all)?;
    crate::tprintln!("[EXEC_UPDATE] rewrite_table took={:?} total={:?}", __t_rewrite.elapsed(), __t0.elapsed());
    Ok(serde_json::json!({"status":"ok"}))
}
