User‑Defined Functions (UDFs)
=============================

Clarium supports Lua‑based UDFs in two flavors:
- Scalar UDFs usable in any expression (SELECT list, WHERE, HAVING, etc.).
- Aggregate UDFs usable in grouped aggregations.

Overview
--------
- UDFs are registered by name and loaded from text sources or files.
- Metadata (kind and return types) should be provided so the engine can type
  expressions correctly.
- Within Lua, you can access execution context via a helper `get_context(key)`.

Register a scalar UDF
---------------------
```
-- Create a simple positive check
CREATE SCRIPT udf/is_pos AS 'function is_pos(x) if x==nil then return false end return x>0 end';

-- Provide metadata using the API (example) so SQL knows the return type is boolean
-- ScriptMeta { kind: Scalar, returns: [Boolean], nullable: true }

-- Use in SQL
SELECT value, is_pos(value) AS positive
FROM demo
LIMIT 5;
```

Register a scalar UDF with multiple return values
-------------------------------------------------
Functions that return multiple values can only be used in SELECT projections.
```
CREATE SCRIPT udf/split2 AS 'function split2(x) if x==nil then return {nil,nil} end return {x, x+1} end';

-- Metadata: returns [Int64, Int64]

SELECT x, split2(x) AS (a, b)
FROM demo;
```

Aggregate UDFs
--------------
Aggregate UDFs receive arrays of group values and return a single value (or a
tuple of values). Example:
```
CREATE SCRIPT udf/sum_plus AS [[
function sum_plus(arr)
  local s = 0
  for i=1,#arr do local v = arr[i]; if v ~= nil then s = s + v end end
  return s + 1
end
]];

-- Metadata: kind Aggregate, returns [Int64]

SELECT group_id, sum_plus(v) AS s
FROM demo
GROUP BY group_id;
```

Error handling and nulls
------------------------
- By default, UDF errors produce NULL results (configurable via engine flags).
- Provide `nullable: true` in metadata when appropriate.

Accessing execution context from Lua
------------------------------------
Inside a UDF you can call `get_context(key)` for values like:
- `current_database`, `current_schema`
- `current_user`, `session_user`
- `transaction_timestamp`, `statement_timestamp` (epoch seconds)

Compatibility helpers
---------------------
Clarium exposes a compatibility scalar `pg_get_viewdef(oid)` so SQL tools that
query PostgreSQL catalogs can retrieve view definitions.
