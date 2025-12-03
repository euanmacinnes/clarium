Vector indexes (HNSW)
======================

Clarium provides a lightweight VECTOR INDEX catalog with HNSW defaults, integrated with `ORDER BY ... USING ANN` for vector search. Indexes are stored as sidecar JSON files (`.vindex`).

Create an index
---------------
```
CREATE VECTOR INDEX idx_docs_body
ON docs(body_embed)
USING hnsw WITH (
  metric='cosine',   -- 'cosine' | 'l2' | 'ip'
  dim=3072           -- required; M/ef_* use config defaults if omitted
);
```

Inspect and drop
----------------
```
SHOW VECTOR INDEX idx_docs_body;
SHOW VECTOR INDEXES;
DROP VECTOR INDEX idx_docs_body;
```

ANN query hint
--------------
Use the `USING` hint on `ORDER BY` to select ANN vs EXACT. ANN requires the first ORDER BY item to be a vector-scoring function with the vector column as its first argument:

```
WITH q AS (SELECT to_vec('[0.1, 0.2, 0.3]') AS v)
SELECT id, cosine_sim(body_embed, (SELECT v FROM q)) AS score
FROM docs
ORDER BY vec_l2(docs.body_embed, (SELECT v FROM q)) USING ANN
LIMIT 10;
```

Notes
-----
- Supported functions for ANN detection: `vec_l2(col, q)` and `cosine_sim(col, q)`.
- RHS can be a literal string (vector) or a scalar subquery `(SELECT ...)` returning a single value.
- If a matching `.vindex` is not found or metadata mismatches, Clarium falls back to exact sort.

Defaults and configuration
--------------------------
HNSW/runtime defaults are controlled by session settings (thread-local) with initial values:
- `vector.hnsw.M = 32`
- `vector.hnsw.ef_build = 200`
- `vector.search.ef_search = 64`

You can override in-session via `SET`:
```
SET vector.hnsw.M = 48;
SET vector.hnsw.ef_build = 256;
SET vector.search.ef_search = 96;
```

On-disk catalog
---------------
Vector indexes live at `<db>/<schema>/<name>.vindex` with:
```
{
  "version": 1,
  "name": "<qualified>",
  "table": "<db>/<schema>/docs",
  "column": "body_embed",
  "algo": "hnsw",
  "metric": "cosine",
  "dim": 3072,
  "params": { "M": 32, "ef_build": 200, "ef_search": 64 },
  "status": { ... }
}
```

UDF helpers
-----------
The following scalar UDFs are available under `scripts/scalars/`:
- `to_vec(text) -> string`
- `cosine_sim(vec, vec) -> float64`
- `vec_l2(vec, vec) -> float64`
- `vec_ip(vec, vec) -> float64`
