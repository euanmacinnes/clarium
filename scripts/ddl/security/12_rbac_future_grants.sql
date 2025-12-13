-- RBAC future grants templates (idempotent)
-- Applies to future objects created in a scope.

CREATE TABLE IF NOT EXISTS security.future_grants (
  scope_kind    TEXT NOT NULL CHECK (scope_kind IN ('DATABASE','SCHEMA')),
  scope_tenant  TEXT NULL,
  db_name       TEXT NULL,
  schema_name   TEXT NULL,
  object_kind   TEXT NOT NULL,   -- TABLES | VIEWS | FUNCTIONS
  privilege     TEXT NOT NULL,
  role_id       TEXT NOT NULL,
  created_at    BIGINT,
  updated_at    BIGINT,
  PRIMARY KEY (
    scope_kind,
    COALESCE(LOWER(scope_tenant),'__global__'),
    COALESCE(LOWER(db_name),'__none__'),
    COALESCE(LOWER(schema_name),'__none__'),
    LOWER(object_kind),
    LOWER(privilege),
    LOWER(role_id)
  )
);
