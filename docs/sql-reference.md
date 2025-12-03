SQL Reference
=============

This reference summarizes the SQL supported by Clarium with runnable examples.
Unless otherwise noted, unqualified names are resolved using current session
defaults from `USE DATABASE` and `USE SCHEMA`.

SELECT
------
Projection and expressions:
```
SELECT a, b, a + b AS s, LOWER(name) AS name_lc
FROM demo;  -- regular table
```

FROM sources
------------
- Regular tables and time tables: `schema/table` and `schema/table.time`.
- Views: reference by name, no extension.
- Subqueries with alias:
```
SELECT x.col
FROM (
  SELECT a AS col FROM t1
) x;
```

JOINs
-----
Inner/Left/Right/Full joins with `ON`:
```
SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id;
```

WHERE (including subqueries)
----------------------------
Comparisons, boolean logic, IS [NOT] NULL, EXISTS/ANY/ALL against subqueries:
```
SELECT * FROM orders o
WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)
  AND o.total > 100;
```

GROUP BY and aggregates
-----------------------
Built-in aggregates: AVG, MAX, MIN, SUM, COUNT, FIRST, LAST, STDEV, DELTA,
HEIGHT, GRADIENT, QUANTILE(p), ARRAY_AGG
```
SELECT customer_id, COUNT(*) AS n, SUM(total) AS revenue
FROM orders
GROUP BY customer_id
HAVING SUM(total) > 500;
```

Time windows with BY
--------------------
Fixed windows over `_time` for time tables:
```
SELECT COUNT(*) AS n, AVG(temp) AS avg_temp
FROM sensors.time
BY 5m
ORDER BY _time;
```

Rolling windows
---------------
```
SELECT ROLLING 1h, MAX(temp) AS max1h
FROM sensors.time
ORDER BY _time;
```

ORDER BY and LIMIT
------------------
```
SELECT * FROM demo ORDER BY id DESC LIMIT 25;
```

Common Table Expressions (WITH)
-------------------------------
```
WITH recent AS (
  SELECT * FROM sensors.time WHERE _time > 1700000000000
)
SELECT COUNT(*) FROM recent;
```

UNION and UNION ALL
-------------------
```
SELECT x FROM a
UNION ALL
SELECT x FROM b;
```
UNION outputs the schema-union of columns; non-ALL removes duplicates.

String features and f-strings
-----------------------------
```
SELECT f'id={id} temp={temp}' AS msg FROM sensors.time LIMIT 1;
-- slicing
SELECT f'num={v}'[0:3] AS prefix FROM demo;
```

INTO (persist SELECT results)
-----------------------------
```
-- Regular table (APPEND default)
SELECT a,b FROM src INTO results REPLACE;

-- Time table: must project exactly one _time column and unique timestamps
SELECT _time, value FROM src.time INTO cleaned.time APPEND;
```

Built-in functions
------------------
- `UPPER(text)`, `LOWER(text)`
- Window functions: `ROW_NUMBER()` with `OVER (...)`
- Date/time functions exposed via `EXTRACT(field FROM expr)` style helpers
- Compatibility: `pg_get_viewdef(oid)` returns stored view definition

User-defined functions (UDFs)
-----------------------------
Lua scalar and aggregate UDFs can be registered and used in expressions and
aggregations. See udf.md for details and examples.

DML
---
`UPDATE` on regular tables and time tables with type-safe assignments and WHERE.

DDL
---
- `CREATE/DROP/RENAME DATABASE`
- `CREATE/DROP/RENAME SCHEMA`
- `CREATE/DROP/RENAME TABLE` (regular)
- `CREATE/DROP/RENAME TIME TABLE`
- `CREATE [OR ALTER] VIEW`, `DROP VIEW`, `SHOW VIEW`
All DDL honors session defaults when names are unqualified.
