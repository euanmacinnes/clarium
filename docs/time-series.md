Time-Series Analytics
=====================

Clarium provides first-class support for time-series data using tables that end
with `.time`. Every time table has an implicit `_time` column (epoch
milliseconds, Int64) that drives windowing and ordering semantics.

Creating time tables
--------------------
```
-- Fully qualified
CREATE TIME TABLE clarium/public/metrics.time;

-- With session defaults
USE DATABASE clarium;
USE SCHEMA public;
CREATE TIME TABLE metrics.time;
```

Ingesting records
-----------------
Use the write API or INTO (see below). Each record requires `_time` and can have
any additional fields. Types are inferred and widened as needed (int -> float -> string).

Selecting from time tables
--------------------------
```
SELECT _time, temp, humidity
FROM metrics.time
WHERE temp IS NOT NULL
ORDER BY _time
LIMIT 100;
```

Windowed aggregation with BY
----------------------------
`BY` groups records into fixed windows by `_time` (ms, s, m, h, d):
```
SELECT COUNT(*) AS n, AVG(temp) AS avg_temp
FROM metrics.time
BY 5m
ORDER BY _time;
```

Rolling analytics
-----------------
`ROLLING` applies a moving window over `_time`:
```
SELECT ROLLING 1h, MAX(temp) AS max1h
FROM metrics.time
ORDER BY _time;
```

Persisting results back to storage
----------------------------------
Use `INTO` to write SELECT results. For time tables:
- The projection must contain exactly one `_time` column.
- `_time` values must be unique in the result.
```
SELECT _time, temp AS temp_c
FROM metrics.time
INTO cleaned.time APPEND;   -- or REPLACE for full rewrite
```

SLICE planning (advanced)
-------------------------
SLICE composes time ranges and manual intervals into a plan:
```
SLICE USING metrics.time WHERE temp>0
  UNION { 1700000000000..1700003600000 }
  LABELS(device, region)
```
See sql-reference.md for the full SLICE syntax supported by your build.

Joins and performance notes
---------------------------
- Joining time tables behaves like regular joins; ensure you join on meaningful
  keys and consider windowing or pre-aggregation to reduce data volume.
- ORDER BY on large time ranges may add temporary columns internally; these are
  dropped from the final projection.
