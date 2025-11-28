Getting Started
===============

This quick start shows how to create a database, ingest some rows, query them,
and try a few distinctive features (time-series tables, views, UDFs).

Prerequisites
-------------
- Rust toolchain (for building from source) or use the provided Dockerfile.
- Clarium binary or server build.

Create a workspace
------------------
We'll use a temporary folder for storage in examples below. The logical object
paths use the form `database/schema/table` and time-series tables end with
`.time`.

Create a time-series table and insert data
------------------------------------------
```
-- Create a time table
CREATE TIME TABLE clarium/public/sensors.time;

-- Insert a few rows via HTTP or the embedded API; using SQL SELECT ... INTO is also supported (see below)
-- Each row must have a `_time` (epoch milliseconds) and any number of fields.
```

Query the table
---------------
```
SELECT _time, temperature, humidity
FROM clarium/public/sensors.time
WHERE temperature > 20
ORDER BY _time
LIMIT 10;
```

Set session defaults and use unqualified names
----------------------------------------------
```
USE DATABASE clarium;
USE SCHEMA public;

SELECT COUNT(*) AS n FROM sensors.time;
```

Create and use a view
---------------------
```
CREATE VIEW hot_reads AS
  SELECT _time, temperature
  FROM sensors.time
  WHERE temperature >= 30;

-- Query the view like a table
SELECT * FROM hot_reads ORDER BY _time LIMIT 5;

-- See the stored definition
SHOW VIEW hot_reads;
-- Or via pg function
SELECT pg_get_viewdef(oid)
FROM pg_catalog.pg_class
WHERE relkind='v' AND relname='hot_reads';
```

Use windows and rolling analytics
---------------------------------
```
-- Fixed-duration windowing with BY
SELECT AVG(temperature) AS avg_temp
FROM sensors.time
BY 5m
ORDER BY _time;

-- Rolling window
SELECT MAX(temperature) AS max15
FROM sensors.time
ROLLING 15m
ORDER BY _time;
```

Write query results back to storage
-----------------------------------
```
-- Into a regular table
SELECT device_id, AVG(temperature) AS avg_temp
FROM sensors.time
GROUP BY device_id
INTO clarium/public/device_avgs REPLACE;

-- Into a time table (must project exactly one _time column)
SELECT _time, temperature AS temp_c
FROM sensors.time
INTO clarium/public/cleaned.time APPEND;
```

Register and call a Lua UDF (scalar)
------------------------------------
```
-- Create a scalar UDF `is_pos(x)` that returns bool
CREATE SCRIPT udf/is_pos AS 'function is_pos(x) if x==nil then return false end return x>0 end';

-- Tell the engine about return types (e.g., through API or configuration)
-- Then use in SQL
SELECT _time, temperature, is_pos(temperature) AS is_hot
FROM sensors.time
LIMIT 3;
```

Next steps
----------
- Read concepts.md to understand objects and naming.
- See sql-reference.md for the query surface with examples.
- Explore time-series and views in depth in time-series.md and views.md.
