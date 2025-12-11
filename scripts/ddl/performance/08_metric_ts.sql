-- Tall time-series of measured metric values

CREATE TABLE IF NOT EXISTS performance.metric_ts (
  ts                TIMESTAMP NOT NULL,
  run_id            BIGINT NOT NULL REFERENCES performance.run(run_id),
  bench_id          BIGINT NOT NULL REFERENCES performance.bench(bench_id),
  metric_id         BIGINT NOT NULL REFERENCES performance.metric_def(metric_id),
  param_fingerprint TEXT NOT NULL,
  value             DOUBLE NOT NULL,
  samples           INT,
  ci_low            DOUBLE,
  ci_high           DOUBLE,
  PRIMARY KEY (ts, run_id, bench_id, metric_id, param_fingerprint)
);
