End‑to‑End Examples
===================

This page collects short, reproducible scenarios that demonstrate Clarium’s
features in realistic flows. You can copy/paste them into your SQL client (or
adapt paths if you use a non‑default storage root).

Conventions used below
----------------------
- We set session defaults so unqualified names resolve under `clarium/public`.
- Replace timestamps and column names with your actual data when needed.

0) Initialize session defaults
------------------------------
```
USE DATABASE clarium;
USE SCHEMA public;
```

1) Build a view over a time table and query it
----------------------------------------------
```
-- Create source time table and load a couple of rows (ingestion shown conceptually)
CREATE TIME TABLE readings.time;
-- ... insert rows with _time, temp, device_id ...

-- Create a view for hot readings
CREATE VIEW hot_reads AS
  SELECT _time, device_id, temp
  FROM readings.time
  WHERE temp >= 30;

-- Use the view
SELECT device_id, COUNT(*) AS n
FROM hot_reads
GROUP BY device_id
ORDER BY n DESC;
```

2) UNION view and self‑join
---------------------------
```
CREATE TIME TABLE a.time;  -- load rows with x
CREATE TIME TABLE b.time;  -- load rows with x

CREATE VIEW v_union AS
  SELECT x FROM a.time
  UNION ALL
  SELECT x FROM b.time;

SELECT a.x, b.x
FROM v_union a
JOIN v_union b ON a.x < b.x
ORDER BY a.x, b.x;
```

3) Session defaults affecting DDL and SELECT
--------------------------------------------
```
USE DATABASE analytics;
USE SCHEMA s1;

CREATE TIME TABLE src.time;
CREATE VIEW v1 AS SELECT v FROM src.time;

-- All resolve under analytics/s1
SHOW VIEW v1;
SELECT * FROM v1 LIMIT 5;
DROP VIEW v1;
```

4) Persist SELECT results (regular table and time table)
-------------------------------------------------------
```
-- Regular table
SELECT device_id, AVG(temp) AS avg_temp
FROM readings.time
GROUP BY device_id
INTO device_avgs REPLACE;

-- Time table (must project exactly one _time)
SELECT _time, temp AS temp_c
FROM readings.time
INTO readings_clean.time APPEND;
```

5) Lua UDF pipeline (scalar + aggregate)
----------------------------------------
```
-- Scalar UDF that formats a message
CREATE SCRIPT udf/hello AS 'function hello(x) return "hi:"..tostring(x) end';
-- Metadata: kind Scalar, returns [String]

SELECT hello(device_id) AS msg, COUNT(*) AS n
FROM readings.time
GROUP BY msg
ORDER BY n DESC;

-- Aggregate UDF that computes sum+1
CREATE SCRIPT udf/sum_plus AS [[
function sum_plus(arr)
  local s = 0
  for i=1,#arr do local v = arr[i]; if v ~= nil then s = s + v end end
  return s + 1
end
]];
-- Metadata: kind Aggregate, returns [Int64]

SELECT device_id, sum_plus(temp) AS s
FROM readings.time
GROUP BY device_id;
```

6) System catalog queries for tooling
-------------------------------------
```
-- List all views
SELECT viewname FROM pg_catalog.pg_views ORDER BY viewname;

-- Get a view definition using OID
SELECT pg_get_viewdef(oid)
FROM pg_catalog.pg_class
WHERE relkind='v' AND relname='hot_reads';

-- Introspect columns
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
ORDER BY table_schema, table_name, ordinal_position;
```
