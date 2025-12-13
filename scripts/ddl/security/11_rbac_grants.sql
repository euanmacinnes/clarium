-- RBAC privilege grants catalog (idempotent)
-- Scope precedence: OBJECT > SCHEMA > DATABASE > TENANT > GLOBAL
-- Notes:
--  - All identifier comparisons are case-insensitive by convention.
--  - object_kind: TABLE | VIEW | FUNCTION (extend as needed)
--  - privilege: free-text enum for now; engine validates known values.

CREATE TABLE IF NOT EXISTS security.grants (
  scope_kind    TEXT NOT NULL CHECK (scope_kind IN ('GLOBAL','TENANT','DATABASE','SCHEMA','OBJECT')),
  scope_tenant  TEXT NULL,
  db_name       TEXT NULL,
  schema_name   TEXT NULL,
  object_kind   TEXT NULL,
  object_name   TEXT NULL,
  privilege     TEXT NOT NULL,
  role_id       TEXT NOT NULL,
  grant_option  BOOLEAN NOT NULL DEFAULT FALSE,
  source        TEXT NOT NULL DEFAULT 'DDL',
  created_at    BIGINT,
  updated_at    BIGINT,
  PRIMARY KEY (
    scope_kind,
    COALESCE(LOWER(scope_tenant),'__global__'),
    COALESCE(LOWER(db_name),'__none__'),
    COALESCE(LOWER(schema_name),'__none__'),
    COALESCE(LOWER(object_kind),'__none__'),
    COALESCE(LOWER(object_name),'__none__'),
    LOWER(privilege),
    LOWER(role_id)
  )
);
