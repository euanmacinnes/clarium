-- Supporting indexes for common access paths

CREATE INDEX IF NOT EXISTS ix_metric_ts_bench_time
  ON performance.metric_ts (bench_id, ts);

CREATE INDEX IF NOT EXISTS ix_metric_ts_run
  ON performance.metric_ts (run_id);

CREATE INDEX IF NOT EXISTS ix_metric_ts_metric
  ON performance.metric_ts (metric_id);

CREATE INDEX IF NOT EXISTS ix_metric_ts_param
  ON performance.metric_ts (param_fingerprint);
