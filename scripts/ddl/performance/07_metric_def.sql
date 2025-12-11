-- Metric catalog (names, units, semantics)

CREATE TABLE IF NOT EXISTS performance.metric_def (
  metric_id        BIGINT PRIMARY KEY,
  name             TEXT NOT NULL,
  unit             TEXT NOT NULL,
  kind             TEXT NOT NULL,
  aggregation      TEXT NOT NULL, -- lower_is_better | higher_is_better
  description      TEXT,
  UNIQUE(name, unit)
);
