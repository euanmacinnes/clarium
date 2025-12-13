-- Seed comprehensive default privilege labels for admin role (idempotent)
-- This seeds a broad catalog of typical privileges (global, database, schema, and object-level)
-- as GLOBAL-scoped grants to the 'admin' role. These GLOBAL grants are primarily to:
--  1) Ensure the 'admin' role has management capabilities out of the box.
--  2) Establish a canonical list of common privilege names in the catalogs for tooling/UIs.
-- Note: Enforcement for DB/Schema/Object privileges occurs at their respective scopes.
--       Seeding them here as GLOBAL grants to 'admin' is harmless (admin is already all-powerful)
--       and makes the names discoverable.

-- Ensure admin has global management privileges
INSERT INTO security.grants (scope_kind, privilege, role_id, grant_option, created_at, updated_at)
SELECT 'GLOBAL', p, 'admin', TRUE, CAST(strftime('%s','now') AS BIGINT)*1000, CAST(strftime('%s','now') AS BIGINT)*1000
FROM (
  VALUES
    -- Global management
    ('CREATE TENANT'),
    ('MANAGE TENANT'),
    ('CREATE DATABASE'),
    ('MANAGE USERS'),
    ('MANAGE ROLES'),

    -- Database-scoped (typical)
    ('DB CONNECT'),
    ('DB USAGE'),
    ('DB READ'),
    ('DB WRITE'),
    ('DB OWNER'),

    -- Schema-scoped (typical)
    ('SCHEMA USAGE'),
    ('SCHEMA CREATE'),
    ('SCHEMA OWNER'),

    -- Object-scoped: TABLE
    ('SELECT'),
    ('INSERT'),
    ('UPDATE'),
    ('DELETE'),
    ('ALTER'),
    ('DROP'),
    ('TRUNCATE'),

    -- Object-scoped: VIEW
    ('VIEW SELECT'),
    ('VIEW ALTER'),
    ('VIEW DROP'),

    -- Object-scoped: FUNCTION / PROCEDURE
    ('EXECUTE'),
    ('FUNCTION ALTER'),
    ('FUNCTION DROP'),

    -- Object-scoped: GRAPH
    ('GRAPH READ'),
    ('GRAPH WRITE'),
    ('GRAPH ALTER'),
    ('GRAPH DROP'),

    -- Object-scoped: FILESTORE (files/buckets)
    ('FILE READ'),
    ('FILE WRITE'),
    ('FILE ALTER'),
    ('FILE DELETE'),

    -- Object-scoped: VECTOR (indexes/collections)
    ('VECTOR READ'),
    ('VECTOR WRITE'),
    ('VECTOR ALTER'),
    ('VECTOR DROP')
) AS T(p)
WHERE NOT EXISTS (
  SELECT 1 FROM security.grants g
  WHERE g.scope_kind='GLOBAL' AND LOWER(g.privilege)=LOWER(T.p) AND LOWER(g.role_id)='admin'
);
