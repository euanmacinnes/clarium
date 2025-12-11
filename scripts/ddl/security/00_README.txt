Security schema DDLs
---------------------

This directory contains discrete DDL files to create the security catalog.
Apply them in numeric order. Each file is idempotent (uses IF NOT EXISTS).

Order:
  01_create_schema.sql
  02_roles.sql
  03_users.sql
  04_role_memberships.sql
  05_policies.sql
  06_resources.sql
  07_fs_overrides.sql
  08_publications.sql
  09_pub_graph.sql
  10_epochs.sql
  99_seed_data.sql (optional initial seed)

Notes:
- Timestamps are stored as BIGINT epoch milliseconds for engine portability.
- JSON-like payloads (predicates, tags) are TEXT (stringified JSON) for now.
- Foreign-key relationships are documented in comments; enforce at the application layer until engine FK support is added.
