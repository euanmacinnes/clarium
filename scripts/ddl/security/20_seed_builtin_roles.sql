-- Seed core built-in roles (idempotent)

-- admin (global)
INSERT INTO security.roles (role_id, description, created_at, updated_at)
SELECT 'admin', 'Global administrator role', CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
WHERE NOT EXISTS (
  SELECT 1 FROM security.roles WHERE LOWER(role_id) = 'admin'
);

-- tenancy roles
INSERT INTO security.roles (role_id, description, created_at, updated_at)
SELECT 'tenancy_admin', 'Tenant management role', CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
WHERE NOT EXISTS (
  SELECT 1 FROM security.roles WHERE LOWER(role_id) = 'tenancy_admin'
);

INSERT INTO security.roles (role_id, description, created_at, updated_at)
SELECT 'tenancy_ddl', 'Tenant DDL role', CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
WHERE NOT EXISTS (
  SELECT 1 FROM security.roles WHERE LOWER(role_id) = 'tenancy_ddl'
);

INSERT INTO security.roles (role_id, description, created_at, updated_at)
SELECT 'tenancy_read', 'Tenant read-only role', CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
WHERE NOT EXISTS (
  SELECT 1 FROM security.roles WHERE LOWER(role_id) = 'tenancy_read'
);

-- database pattern roles
INSERT INTO security.roles (role_id, description, created_at, updated_at)
SELECT 'db_reader', 'Database read permission set', CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
WHERE NOT EXISTS (
  SELECT 1 FROM security.roles WHERE LOWER(role_id) = 'db_reader'
);

INSERT INTO security.roles (role_id, description, created_at, updated_at)
SELECT 'db_writer', 'Database write permission set', CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
WHERE NOT EXISTS (
  SELECT 1 FROM security.roles WHERE LOWER(role_id) = 'db_writer'
);

INSERT INTO security.roles (role_id, description, created_at, updated_at)
SELECT 'db_owner', 'Database owner permission set', CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
WHERE NOT EXISTS (
  SELECT 1 FROM security.roles WHERE LOWER(role_id) = 'db_owner'
);
