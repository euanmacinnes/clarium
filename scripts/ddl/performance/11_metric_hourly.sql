-- Hourly rollup table for metric_ts
-- Aggregates raw metrics into 1-hour buckets for faster trend queries

CREATE TABLE IF NOT EXISTS performance.metric_hourly (
  ts_hour            TIMESTAMP NOT NULL, -- bucket start (ms since epoch)
  bench_id           BIGINT NOT NULL,
  metric_id          BIGINT NOT NULL,
  param_fingerprint  TEXT NOT NULL,
  cnt                INT NOT NULL,
  avg_value          DOUBLE NOT NULL,
  min_value          DOUBLE NOT NULL,
  max_value          DOUBLE NOT NULL,
  PRIMARY KEY (ts_hour, bench_id, metric_id, param_fingerprint)
);

CREATE INDEX IF NOT EXISTS ix_metric_hourly_bench_time
  ON performance.metric_hourly (bench_id, ts_hour);
