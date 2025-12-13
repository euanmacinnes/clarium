### Overview
Below is a comprehensive RBAC DDL proposal suitable for enterprise-grade, multi-tenant deployments. It is designed to be future-proof, composable, and auditable, while remaining compatible with your current legacy authorizer (as seen in `src/identity/authorizer.rs`). Each DDL is defined as an independent command to align with your guideline of keeping each DDL in separate files and to keep parsing/execution layered.

### Design goals
- Clear separation between identities (users), roles, and privileges.
- Database-, schema-, and object-level scoping, with future-grant templates.
- Tenant-aware scoping for multi-tenant deployments.
- First-class role hierarchies and default roles.
- Auditable changes with explicit, reversible commands.
- Graceful error handling (no panics/bails) and permanent debug logging gates.

### Core entities
- User: authenticating principal (maps to `identity::Principal`).
- Role: collection of privileges; may have parent roles (inheritance).
- Privilege: operation on a scope (global | tenant | database | schema | object).
- Tenant: abstract boundary for multi-tenant isolation.

### Privilege taxonomy (examples)
- Global: `CREATE TENANT`, `MANAGE TENANT`, `CREATE DATABASE`, `MANAGE USERS`, `MANAGE ROLES`.
- Tenant-scoped: `TENANT DDL`, `TENANT READ`, `TENANT WRITE`.
- Database-scoped: `DB CONNECT`, `DB USAGE`, `DB READ`, `DB WRITE`, `DB OWNER`.
- Schema-scoped: `SCHEMA USAGE`, `SCHEMA CREATE`, `SCHEMA OWNER`.
- Object-scoped:
  - Table: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP`, `TRUNCATE`
  - View: `SELECT`, `ALTER`, `DROP`
  - Function/Procedure: `EXECUTE`, `ALTER`, `DROP`

### DDL Syntax
Note: Identifiers are case-insensitive unless quoted. Optional clauses shown in brackets.

#### Users
```sql
CREATE USER user_name
    [WITH PASSWORD 'secret']
    [IN ROLE role_name [, ...]]
    [DEFAULT ROLE role_name | ALL | NONE]
    [TENANT tenant_name]
    [DISABLED];

ALTER USER user_name
    [WITH PASSWORD 'new_secret']
    [RENAME TO new_user_name]
    [SET DEFAULT ROLE role_name | ALL | NONE]
    [SET TENANT tenant_name | UNSET TENANT]
    [ENABLE | DISABLE];

DROP USER user_name [IF EXISTS] [CASCADE | RESTRICT];
```

#### Roles
```sql
CREATE ROLE role_name
    [INHERIT]                -- role inherits from granted roles
    [NOINHERIT]
    [TENANT tenant_name];

ALTER ROLE role_name
    [RENAME TO new_role_name]
    [SET TENANT tenant_name | UNSET TENANT]
    [INHERIT | NOINHERIT];

DROP ROLE role_name [IF EXISTS] [CASCADE | RESTRICT];
```

#### Role membership (users or roles as members)
```sql
GRANT ROLE role_name [, ...] TO principal_name [, ...]
    [WITH ADMIN OPTION]
    [TENANT tenant_name];

REVOKE ROLE role_name [, ...] FROM principal_name [, ...]
    [CASCADE | RESTRICT]
    [TENANT tenant_name];
```

#### Privilege grants
Scope precedence: object > schema > database > tenant > global. Grants are additive; deny is modeled by absence of grant (keep it simple initially).
```sql
-- Global
GRANT privilege [, ...] ON GLOBAL TO role_name [, ...] [WITH GRANT OPTION];
REVOKE privilege [, ...] ON GLOBAL FROM role_name [, ...] [CASCADE | RESTRICT];

-- Tenant
GRANT privilege [, ...] ON TENANT tenant_name TO role_name [, ...] [WITH GRANT OPTION];
REVOKE privilege [, ...] ON TENANT tenant_name FROM role_name [, ...] [CASCADE | RESTRICT];

-- Database
GRANT privilege [, ...] ON DATABASE db_name TO role_name [, ...] [WITH GRANT OPTION];
REVOKE privilege [, ...] ON DATABASE db_name FROM role_name [, ...] [CASCADE | RESTRICT];

-- Schema
GRANT privilege [, ...] ON SCHEMA db_name.schema_name TO role_name [, ...] [WITH GRANT OPTION];
REVOKE privilege [, ...] ON SCHEMA db_name.schema_name FROM role_name [, ...] [CASCADE | RESTRICT];

-- Object (TABLE | VIEW | FUNCTION)
GRANT privilege [, ...] ON { TABLE | VIEW | FUNCTION } db.schema.obj TO role_name [, ...] [WITH GRANT OPTION];
REVOKE privilege [, ...] ON { TABLE | VIEW | FUNCTION } db.schema.obj FROM role_name [, ...] [CASCADE | RESTRICT];
```

#### Default and active role controls
```sql
-- Default role(s) for user session start
ALTER USER user_name SET DEFAULT ROLE role_name | ALL | NONE;

-- Switch current active roles for this session
SET ROLE role_name | NONE | DEFAULT;
SHOW CURRENT ROLES;    -- list active roles in this session
```

#### Future grants (templates)
Apply to future objects created in the scope.
```sql
-- Database-level template
GRANT privilege [, ...] ON FUTURE TABLES IN DATABASE db_name TO role_name [, ...];
REVOKE privilege [, ...] ON FUTURE TABLES IN DATABASE db_name FROM role_name [, ...];

-- Schema-level template
GRANT privilege [, ...] ON FUTURE { TABLES | VIEWS | FUNCTIONS } IN SCHEMA db.schema TO role_name [, ...];
REVOKE privilege [, ...] ON FUTURE { TABLES | VIEWS | FUNCTIONS } IN SCHEMA db.schema FROM role_name [, ...];
```

#### Row-Level Security (RLS) policies
Optional now; reserved for enterprise scenarios.
```sql
CREATE POLICY policy_name ON TABLE db.schema.tbl
    AS { PERMISSIVE | RESTRICTIVE }
    FOR { SELECT | INSERT | UPDATE | DELETE }
    TO role_name [, ...]
    USING (predicate_sql_expr)
    [WITH CHECK (predicate_sql_expr)];

ALTER POLICY policy_name ON TABLE db.schema.tbl ...;
DROP POLICY policy_name ON TABLE db.schema.tbl [IF EXISTS];
```

#### Introspection & audit DDL
```sql
SHOW USERS [LIKE pattern];
SHOW ROLES [LIKE pattern];
SHOW ROLE GRANTS [FOR principal_name];
SHOW PRIVILEGES [ON { GLOBAL | TENANT t | DATABASE d | SCHEMA d.s | OBJECT d.s.o }];
SHOW FUTURE GRANTS [IN { DATABASE d | SCHEMA d.s }];
SHOW POLICIES [ON TABLE d.s.t];

-- Audit stream controls
SHOW AUDIT TRAIL [SINCE timestamp];
```

### Examples
```sql
-- 1) Create a tenant admin role and grant to a user
CREATE ROLE tenant_admin TENANT acme INHERIT;
CREATE USER alice WITH PASSWORD '***' TENANT acme;
GRANT ROLE tenant_admin TO alice WITH ADMIN OPTION TENANT acme;

-- 2) DB-level read/write split
CREATE ROLE sales_reader;
CREATE ROLE sales_writer;
GRANT DB CONNECT, DB USAGE, DB READ ON DATABASE sales TO sales_reader;
GRANT DB CONNECT, DB USAGE, DB WRITE ON DATABASE sales TO sales_writer;
GRANT ROLE sales_reader TO alice;

-- 3) Future grants for new tables in a schema
GRANT SELECT ON FUTURE TABLES IN SCHEMA sales.public TO sales_reader;

-- 4) Object-level override
GRANT UPDATE, DELETE ON TABLE sales.public.orders TO sales_writer;
```

### Legacy compatibility and migration
Your current `identity::authorizer` infers roles from legacy permissions:
- Legacy `Schema`/`Database` privileges imply `Admin` (maps to global/tenant admins here).
- `Select` → `DbReader`, `Insert` → `DbWriter`, `Calculate` → `Compute`, `DeleteRows` → `DbDeleter`.

Migration plan:
- Phase 1 (compat): keep legacy ACL as the source of truth; RBAC DDL is accepted and stored but evaluated by translating to the legacy checks where possible.
- Phase 2: dual-write—RBAC becomes primary; generate equivalent legacy entries for backward compatibility.
- Phase 3: deprecate legacy paths; `check_command_allowed` evaluates RBAC directly.

### Error handling and debug logging
- All DDL commands must return structured errors: `code`, `message`, `hint`, `scope`, `principal`.
- Common errors: `ROLE_NOT_FOUND`, `USER_NOT_FOUND`, `PRIVILEGE_NOT_APPLICABLE_TO_SCOPE`, `TENANT_MISMATCH`, `CYCLIC_ROLE_INHERITANCE`, `DUPLICATE_NAME`.
- No panics; use result-propagation and convert to user-visible diagnostics.
- Add permanent debug `tprintln!` lines at entry/exit of DDL handlers with: command, principal, tenant, scope, success/failure, elapsed.

### System catalogs (for SHOW queries)
- `pg_roles_ext(role_id, role_name, tenant, inherit, created_at, created_by)`
- `pg_users_ext(user_id, user_name, tenant, default_role, disabled, created_at, created_by)`
- `pg_grants_ext(scope_kind, scope_id, object_fqn, privilege, grantee_role_id, grantable, source)`
- `pg_role_members(role_id, member_principal_id, admin_option)`
- `pg_future_grants(scope_kind, scope_id, object_kind, privilege, grantee_role_id)`
- `pg_policies(table_oid, policy_name, permissive, cmd, expr_using, expr_check, to_roles)`

### Parser/engine notes
- Keep parsing and execution separate; each DDL is its own AST node and executor.
- Avoid giant matches: dispatch each DDL kind to its own function/module (e.g., `ddl/users.rs`, `ddl/roles.rs`, `ddl/grants.rs`, ...).
- Keep primary interfaces thin; convert to internal canonical model (principal_id, role_id, scope, privilege_enum, flags).

### Security defaults and semantics
- Default-deny: users have no privileges until granted via roles.
- Ownership model: creators are implicit owners with `WITH GRANT OPTION` over created objects (configurable).
- Inheritance is enabled by default (`INHERIT`), allowing transitive privileges.
- Active roles are the intersection of: granted roles ∩ session `SET ROLE` selection ∩ tenant context.

### Testing strategy (post-implementation)
- Unit tests per DDL file: parse → plan → execute → catalog verify.
- Cross-scope tests: global vs tenant vs db vs schema vs object grants precedence.
- Negative tests for every error code.
- Concurrency tests for DDL race conditions (create/drop, grant/revoke).
- Migration tests: legacy ↔ RBAC translations.

### Next steps
- Confirm syntax choices (especially tenant clauses and future grants terminology).
- Lock privilege enums and scope model.
- Define system catalog schemas and wire `SHOW` commands.
- Implement in stages, keeping each DDL isolated and thin. 

If you want, I can tailor the privilege enum to your current `security::CommandKind` and propose a 1:1 mapping table for the first implementation phase.