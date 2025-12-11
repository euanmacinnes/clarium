-- Canonical benchmark identity

CREATE TABLE IF NOT EXISTS performance.bench (
  bench_id         BIGINT PRIMARY KEY,
  suite            TEXT NOT NULL,
  group_name       TEXT NOT NULL,
  bench_name       TEXT NOT NULL,
  description      TEXT,
  UNIQUE(suite, group_name, bench_name)
);
