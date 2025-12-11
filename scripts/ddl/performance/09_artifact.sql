-- Optional links to reports or external artifacts per run

CREATE TABLE IF NOT EXISTS performance.artifact (
  artifact_id      BIGINT PRIMARY KEY,
  run_id           BIGINT NOT NULL REFERENCES performance.run(run_id),
  kind             TEXT NOT NULL,
  uri              TEXT NOT NULL,
  label            TEXT
);
