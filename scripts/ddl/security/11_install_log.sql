-- Installation log for DDL execution
CREATE TABLE IF NOT EXISTS security.install_log (
  script_path TEXT,
  checksum TEXT,
  started_at BIGINT,
  finished_at BIGINT,
  status TEXT,          -- 'ok' | 'error'
  statements INT,
  error_text TEXT
);
