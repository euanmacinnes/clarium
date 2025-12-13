-- Seed default global privileges for admin role (idempotent)

-- Ensure admin has global management privileges
INSERT INTO security.grants (scope_kind, privilege, role_id, grant_option, created_at, updated_at)
SELECT 'GLOBAL', p, 'admin', TRUE, CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
FROM (VALUES
    ('CREATE TENANT'),
    ('MANAGE TENANT'),
    ('CREATE DATABASE'),
    ('MANAGE USERS'),
    ('MANAGE ROLES')
) AS T(p)
WHERE NOT EXISTS (
  SELECT 1 FROM security.grants g
  WHERE g.scope_kind='GLOBAL' AND LOWER(g.privilege)=LOWER(T.p) AND LOWER(g.role_id)='admin'
);
