-- Published overlay namespaces
CREATE TABLE IF NOT EXISTS security.publications (
  publication TEXT PRIMARY KEY,
  description TEXT,
  created_at BIGINT,
  updated_at BIGINT
);
