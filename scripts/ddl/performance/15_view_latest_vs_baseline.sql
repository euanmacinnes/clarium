-- Latest vs baseline comparison per metric/bench/params

CREATE VIEW performance.latest_vs_baseline AS
WITH latest_run AS (
  SELECT run_id FROM performance.latest_main_run
), baseline_run AS (
  SELECT run_id FROM performance.baseline_main_run
)
SELECT 
  l.bench_id,
  l.metric_id,
  l.param_fingerprint,
  l.value AS latest_value,
  b.value AS baseline_value,
  CASE WHEN b.value = 0 THEN NULL ELSE (l.value - b.value) / b.value END AS delta
FROM performance.metric_ts l
JOIN latest_run lr ON lr.run_id = l.run_id
JOIN performance.metric_ts b
  ON b.bench_id = l.bench_id
 AND b.metric_id = l.metric_id
 AND b.param_fingerprint = l.param_fingerprint
JOIN baseline_run br ON br.run_id = b.run_id;
