Storage Format
==============

Clarium stores all objects on the local filesystem using a simple, transparent
layout. This section documents the on‑disk representation so you can reason
about durability, backups, and tooling.

Directory layout
----------------
```
<root>/
  <database>/
    <schema>/
      <table>/                 # regular table directory
        schema.json            # logical column type map and metadata
        data-<min>-<max>-<ts>.parquet  # one or more parquet chunks (optional)
        ...
      <table>.time/            # time table directory (ends with .time)
        schema.json
        data-*.parquet
      <view>.view              # view JSON file (not a directory)
```

schema.json (regular and time tables)
-------------------------------------
- JSON object mapping `columnName -> typeKey` plus optional markers:
  - Keys for user columns map to simplified type keys: `string|int64|float64|bool`.
  - A `PRIMARY` marker may be present to indicate a table‑level primary key.
  - Nested object `__clarium_oids__` persists stable OIDs: `{ "class_oid": <int> }`.

Parquet chunks
--------------
- Files named `data-<min>-<max>-<ts>.parquet` hold appended data.
- For time tables, `_time` is always encoded as Int64 epoch milliseconds.
- Rewrites (e.g., `INTO ... REPLACE`) may consolidate into a single chunk file.

View files (`.view`)
--------------------
- JSON object with fields:
  - `name`: qualified view name (`db/schema/name`).
  - `columns`: array of `(name, typeKey)` pairs inferred from the definition.
  - `definition_sql`: original SELECT or SELECT UNION SQL text.
  - `__clarium_oids__`: nested object with `{ "class_oid": <int> }` for stable OID.

Backups and migration
---------------------
- A file‑level copy of the database root is sufficient for backups.
- Table schema changes occur via `schema.json` rewrites; parquet chunks are
  immutable once written by append operations.

Name uniqueness and files
-------------------------
- A view `<name>.view` must not collide with a directory `<name>` or `<name>.time`.
- The engine enforces these rules during DDL.
