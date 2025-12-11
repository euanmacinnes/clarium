-- Epoch counters for cache invalidation
CREATE TABLE IF NOT EXISTS security.epochs (
  scope TEXT,          -- 'global' | 'filestore' | 'publication' | 'user' | 'db'
  id TEXT,             -- scope identifier (e.g., fs name, publication name, user id)
  value BIGINT,
  updated_at BIGINT,
  PRIMARY KEY (scope, id)
);
