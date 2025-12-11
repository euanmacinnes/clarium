-- Roles catalog
CREATE TABLE IF NOT EXISTS security.roles (
  role_id TEXT PRIMARY KEY,
  description TEXT,
  created_at BIGINT,
  updated_at BIGINT
);
