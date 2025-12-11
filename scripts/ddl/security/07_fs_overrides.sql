-- Per-filestore/node explicit allow/deny overrides
CREATE TABLE IF NOT EXISTS security.fs_overrides (
  filestore TEXT,
  node_id TEXT,            -- path or stable node id
  role_id TEXT,            -- applies to this role (or user-specific via a special role)
  action TEXT,             -- one action or '*'
  effect TEXT,             -- 'allow' | 'deny'
  created_at BIGINT,
  updated_at BIGINT,
  PRIMARY KEY (filestore, node_id, role_id, action)
  -- FK(role_id) REFERENCES security.roles(role_id)
);
