-- A run represents one invocation of the benchmark suite

CREATE TABLE IF NOT EXISTS performance.run (
  run_id           BIGINT PRIMARY KEY,
  run_ts           TIMESTAMP NOT NULL,
  host_id          BIGINT NOT NULL REFERENCES performance.env_host(host_id),
  build_id         BIGINT NOT NULL REFERENCES performance.env_build(build_id),
  runner           TEXT,
  ci               BOOL DEFAULT FALSE,
  notes            TEXT
);
