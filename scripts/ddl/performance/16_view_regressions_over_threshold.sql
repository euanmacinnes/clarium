-- Regressions where lower_is_better and delta > threshold (default 5%)
-- delta = (latest - baseline)/baseline

CREATE VIEW performance.regressions_over_5pct AS
SELECT 
  lvb.bench_id,
  lvb.metric_id,
  lvb.param_fingerprint,
  lvb.latest_value,
  lvb.baseline_value,
  lvb.delta,
  md.name AS metric_name,
  md.unit,
  md.aggregation
FROM performance.latest_vs_baseline lvb
JOIN performance.metric_def md ON md.metric_id = lvb.metric_id
WHERE md.aggregation = 'lower_is_better'
  AND lvb.delta IS NOT NULL
  AND lvb.delta > 0.05
ORDER BY lvb.delta DESC;
