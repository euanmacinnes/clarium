Views
=====

Clarium supports first‑class SQL views with full SELECT and SELECT UNION
definitions. Views are persisted as JSON files with a `.view` extension next to
tables and can be queried from any `FROM` clause.

Why views?
----------
- Encapsulate complex queries and reuse them.
- Compose analytics by joining views with tables or other views.
- Interoperate with tools via compatibility catalogs (e.g., `pg_catalog.pg_views`).

Creating and altering views
---------------------------
```
-- honors session defaults (USE DATABASE/SCHEMA)
CREATE VIEW hourly_hot AS
  SELECT BY 1h, AVG(temperature) AS avg_temp
  FROM sensors.time
  WHERE temperature >= 30;

-- re-create or update in place
CREATE OR ALTER VIEW hourly_hot AS
  SELECT BY 1h, MAX(temperature) AS max_temp
  FROM sensors.time;
```

Selecting from views
--------------------
```
SELECT * FROM hourly_hot ORDER BY _time LIMIT 10;

-- join a view to a table or another view
SELECT a._time, a.max_temp, meta.device_name
FROM hourly_hot a
JOIN device_meta meta ON a.device_id = meta.id;
```

UNION views
-----------
```
CREATE VIEW v_union AS
  SELECT x FROM u1.time
  UNION ALL
  SELECT x FROM u2.time;

SELECT a.x, b.x
FROM v_union a
JOIN v_union b ON a.x < b.x;
```

Inspecting a view’s definition
------------------------------
```
SHOW VIEW hourly_hot;           -- returns one row with name and definition

-- Or via compatibility function (through pg catalogs)
SELECT pg_get_viewdef(oid)
FROM pg_catalog.pg_class
WHERE relkind='v' AND relname='hourly_hot';
```

Name uniqueness and collisions
------------------------------
- A view name must not conflict with an existing table or time table of the same
  base name in the same schema.
- Attempting to create a view `foo` when `foo` (table) or `foo.time` exists will
  fail, and vice versa.

On-disk representation
----------------------
- Views are stored as `<db>/<schema>/<name>.view` JSON:
  - `name`: qualified name `db/schema/name`
  - `columns`: list of `(columnName, typeKey)` pairs (`string|int64|float64|bool`)
  - `definition_sql`: the original `SELECT` or `SELECT UNION` text

System catalog visibility
-------------------------
- `information_schema.views` lists `table_schema`, `table_name`, `view_definition`.
- `pg_catalog.pg_views` lists `schemaname`, `viewname`, `definition`.
- `pg_catalog.pg_class` includes views with `relkind='v'` and stable `oid`s.
