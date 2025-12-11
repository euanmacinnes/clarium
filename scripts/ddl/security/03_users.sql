-- Users catalog (central identity). Passwords may be managed by the engine or external IdP.
CREATE TABLE IF NOT EXISTS security.users (
  user_id TEXT PRIMARY KEY,
  display_name TEXT,
  password_hash TEXT, -- optional when using external IdP
  attrs_json TEXT,    -- serialized attributes (org_id, tenant_id, ip, device_id, ...)
  created_at BIGINT,
  updated_at BIGINT
);
