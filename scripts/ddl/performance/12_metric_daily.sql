-- Daily rollup table for metric_ts
-- Aggregates raw metrics into 1-day buckets for long-term trends

CREATE TABLE IF NOT EXISTS performance.metric_daily (
  ts_day             TIMESTAMP NOT NULL, -- bucket start (ms since epoch at 00:00)
  bench_id           BIGINT NOT NULL,
  metric_id          BIGINT NOT NULL,
  param_fingerprint  TEXT NOT NULL,
  cnt                INT NOT NULL,
  avg_value          DOUBLE NOT NULL,
  min_value          DOUBLE NOT NULL,
  max_value          DOUBLE NOT NULL,
  PRIMARY KEY (ts_day, bench_id, metric_id, param_fingerprint)
);

CREATE INDEX IF NOT EXISTS ix_metric_daily_bench_time
  ON performance.metric_daily (bench_id, ts_day);
