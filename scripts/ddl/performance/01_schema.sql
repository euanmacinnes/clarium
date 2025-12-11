-- performance schema root
-- Purpose: isolate all benchmark/run/metric storage artifacts in a dedicated schema
-- Note: keep each DDL in a separate file per project guidelines

CREATE SCHEMA IF NOT EXISTS performance;
