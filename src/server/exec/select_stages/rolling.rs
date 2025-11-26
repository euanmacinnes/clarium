//! ROLLING stage
//! Ported core logic from `run_select_rolling`, adapted to operate on the incoming DataFrame.

use anyhow::Result;
use polars::prelude::*;

use crate::server::data_context::{DataContext, SelectStage};
use crate::query::{Query, AggFunc};

pub fn rolling(mut df: DataFrame, q: &Query, ctx: &mut DataContext) -> Result<DataFrame> {
    let win = q.rolling_window_ms.ok_or_else(|| anyhow::anyhow!("ROLLING BY requires a window"))?;
    if q.group_by_cols.is_some() { anyhow::bail!("ROLLING BY cannot be used with GROUP BY"); }
    if q.select.iter().any(|i| i.str_func.is_some()) {
        anyhow::bail!("String functions are not supported with ROLLING BY window");
    }
    if q.select.iter().any(|i| match &i.expr { Some(crate::query::ArithExpr::Term(crate::query::ArithTerm::Col { .. })) => false, Some(_) => true, None => false }) {
        anyhow::bail!("ROLLING BY currently supports only simple columns inside aggregate functions");
    }

    // Resolve time column and ensure sorted by it
    let time_col = ctx.resolve_column(&df, "_time").unwrap_or_else(|_| "_time".to_string());
    df = df.sort([time_col.as_str()], polars::prelude::SortMultipleOptions::default())?;

    // Prepare output columns starting with _time
    let time_i64 = df.column(&time_col)?.i64()?.clone();
    let time_vals: Vec<Option<i64>> = time_i64.into_iter().collect();
    let mut out_cols: Vec<Column> = vec![Series::new("_time".into(), time_vals.clone()).into()];

    // Implement rolling aggregates per selected aggregate column (support subset as in original)
    for item in &q.select {
        if let Some(func) = &item.func {
            // Clause-aware validation: ensure input column exists (resolve via context)
            let t_col = df.column(&time_col)?.i64()?;
            let times: Vec<i64> = t_col.into_no_null_iter().collect();
            // COUNT(*) does not reference a real column; treat every row as 1
            let val_opt: Vec<Option<f64>> = if matches!(func, AggFunc::Count) && item.column == "*" {
                vec![Some(1.0); times.len()]
            } else {
                let val_col = ctx.resolve_column(&df, &item.column).unwrap_or_else(|_| item.column.clone());
                let val_s = df.column(&val_col)?;
                // Prepare numeric view for value column as Float64 (required for numeric aggregates)
                match val_s.dtype() {
                    DataType::Float64 => val_s.f64()?.into_iter().collect(),
                    DataType::Int64 => val_s.i64()?.into_iter().map(|o| o.map(|v| v as f64)).collect(),
                    DataType::String => { anyhow::bail!("ROLLING BY supports only numeric columns for aggregations"); },
                    _ => val_s.cast(&DataType::Float64)?.f64()?.into_iter().collect(),
                }
            };

            let n = times.len();
            let mut res: Vec<Option<f64>> = Vec::with_capacity(n);
            let mut j: usize = 0; // window start index (inclusive)
            let mut sum: f64 = 0.0;
            let mut cnt: usize = 0;
            let mut sumsq: f64 = 0.0; // for STDEV

            for i in 0..n {
                let cur_t = times[i];
                let cutoff = cur_t - win + 1;
                // slide window start forward
                while j < i {
                    if times[j] >= cutoff { break; }
                    if let Some(v) = val_opt[j] { sum -= v; sumsq -= v * v; cnt -= 1; }
                    j += 1;
                }
                // include current i
                if let Some(v) = val_opt[i] { sum += v; sumsq += v * v; cnt += 1; }

                // compute aggregate for this i
                let v_opt = match func {
                    AggFunc::Avg => { if cnt > 0 { Some(sum / cnt as f64) } else { None } }
                    AggFunc::Sum => { if cnt > 0 { Some(sum) } else { None } }
                    AggFunc::Count => { Some(cnt as f64) }
                    AggFunc::Stdev => {
                        if cnt >= 2 {
                            let mean = sum / cnt as f64;
                            let var = (sumsq - mean * mean * cnt as f64) / (cnt as f64 - 1.0);
                            Some(var.sqrt())
                        } else { None }
                    }
                    AggFunc::Max | AggFunc::Min | AggFunc::First | AggFunc::Last | AggFunc::Delta | AggFunc::Height | AggFunc::Gradient | AggFunc::Quantile(_) | AggFunc::ArrayAgg => {
                        anyhow::bail!("ROLLING BY currently supports AVG, SUM, COUNT, and STDEV only");
                    }
                };
                res.push(v_opt);
            }

            let name = match func {
                AggFunc::Avg => format!("AVG({})", item.column),
                AggFunc::Sum => format!("SUM({})", item.column),
                AggFunc::Count => if item.column == "*" { "COUNT(*)".to_string() } else { format!("COUNT({})", item.column) },
                AggFunc::Stdev => format!("STDEV({})", item.column),
                AggFunc::Max => format!("MAX({})", item.column),
                AggFunc::Min => format!("MIN({})", item.column),
                AggFunc::First => format!("FIRST({})", item.column),
                AggFunc::Last => format!("LAST({})", item.column),
                AggFunc::Delta => format!("DELTA({})", item.column),
                AggFunc::Height => format!("HEIGHT({})", item.column),
                AggFunc::Gradient => format!("GRADIENT({})", item.column),
                AggFunc::Quantile(cutoff) => format!("_{}_QUANTILE({})", cutoff, item.column),
                AggFunc::ArrayAgg => format!("ARRAY_AGG({})", item.column),
            };
            out_cols.push(Series::new((&name).into(), res).into());
        }
    }

    let cols: Vec<Column> = out_cols.into_iter().collect();
    let out = DataFrame::new(cols)?;

    // Do not apply HAVING here; HAVING must run as the final stage on the final projection
    ctx.register_df_columns_for_stage(SelectStage::Rolling, &out);
    Ok(out)
}
