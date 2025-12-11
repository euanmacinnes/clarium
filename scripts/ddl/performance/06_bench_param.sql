-- Normalized parameter key/values for each benchmark

CREATE TABLE IF NOT EXISTS performance.bench_param (
  param_id         BIGINT PRIMARY KEY,
  bench_id         BIGINT NOT NULL REFERENCES performance.bench(bench_id),
  key              TEXT NOT NULL,
  value            TEXT NOT NULL,
  UNIQUE(bench_id, key, value)
);
