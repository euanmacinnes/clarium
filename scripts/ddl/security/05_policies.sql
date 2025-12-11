-- RBAC policies
CREATE TABLE IF NOT EXISTS security.policies (
  policy_id TEXT PRIMARY KEY,
  role_id TEXT,
  actions TEXT,             -- comma-separated actions or "*"
  resource_selector TEXT,   -- e.g., res://db/schema/table/** or res://**
  predicate_json TEXT,      -- JSON expression for ABAC (optional)
  effect TEXT,              -- 'allow' | 'deny'
  priority INT,
  created_at BIGINT,
  updated_at BIGINT
  -- FK(role_id) REFERENCES security.roles(role_id)
);
