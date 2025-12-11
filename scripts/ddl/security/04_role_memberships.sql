-- User-to-role memberships with optional validity windows
CREATE TABLE IF NOT EXISTS security.role_memberships (
  user_id TEXT,
  role_id TEXT,
  valid_from BIGINT, -- epoch ms
  valid_to BIGINT,   -- epoch ms (nullable)
  created_at BIGINT,
  updated_at BIGINT,
  PRIMARY KEY (user_id, role_id)
  -- FK(user_id) REFERENCES security.users(user_id)
  -- FK(role_id) REFERENCES security.roles(role_id)
);
