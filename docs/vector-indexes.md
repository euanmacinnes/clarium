Vector indexes (ANN + EXACT)
=============================

Clarium provides a lightweight VECTOR INDEX catalog with HNSW defaults, integrated with `ORDER BY ... USING ANN` and table-valued functions (TVFs) for vector search. Index metadata is stored as sidecar JSON files (`.vindex`) and index data lives in companion binary files (`.vdata`).

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

Modes (freshness policy)
------------------------
Vector indexes support a configurable freshness policy recorded in `.vindex.mode`:

- IMMEDIATE | BATCHED | ASYNC | REBUILD_ONLY

Current runtime behavior: REBUILD_ONLY is fully supported; other modes are accepted in DDL and surfaced in `SHOW`, but incremental updates will return a friendly “not supported yet” message instead of panicking.

ANN query hint and semantics
----------------------------
Use the `USING` hint on `ORDER BY` to select ANN vs EXACT. ANN requires the first ORDER BY item to be a vector-scoring function with the vector column as its first argument. Behavior differs with and without `LIMIT`:

```
WITH q AS (SELECT to_vec('[0.1, 0.2, 0.3]') AS v)
SELECT id, cosine_sim(body_embed, (SELECT v FROM q)) AS score
FROM docs
ORDER BY vec_l2(docs.body_embed, (SELECT v FROM q)) USING ANN
LIMIT 10;
```

- With LIMIT present (top-k): the engine may directly use the ANN index to retrieve k candidates and return them after optional exact re-score for parity checks.
- Without LIMIT (full sort): a two‑phase path is used — ANN preselect W = alpha·k (configurable preselect width) or a default window, perform exact re‑score on W, then final sort across the full result respecting secondary keys.
- Secondary ORDER BY keys are applied after the primary vector score (or re‑scored value) to preserve full SQL semantics.
- Deterministic tie‑breaking: when scores tie, stable `row_id` or ordinal is used to keep results deterministic across runs.

Notes
-----
- Supported functions for ANN detection: `vec_l2(col, q)`, `cosine_sim(col, q)`, and `vec_ip(col, q)`.
- RHS can be a literal string (vector) or a scalar subquery `(SELECT ...)` returning a single value.
- If a matching `.vindex` is not found or metadata mismatches (metric/dim/table/column), Clarium falls back to EXACT.
- Metric ordering direction: L2 ascending (smaller is better), cosine/IP descending (larger is better).

Defaults and configuration
--------------------------
HNSW/runtime defaults are controlled by session settings (thread‑local) with initial values:
- `vector.hnsw.M = 32`
- `vector.hnsw.ef_build = 200`
- `vector.search.ef_search = 64`
- `vector.preselect_alpha = 6`   -- two‑phase ANN preselect multiplier (W = alpha·k)

You can override in‑session via `SET`:
```
SET vector.hnsw.M = 48;
SET vector.hnsw.ef_build = 256;
SET vector.search.ef_search = 96;
SET vector.preselect_alpha = 8;
```

On-disk catalog and data files (v2)
-----------------------------------
Metadata lives at `<db>/<schema>/<name>.vindex` and index data at `<db>/<schema>/<name>.vdata`.

`.vindex` example:
```
{
  "version": 2,
  "name": "<qualified>",
  "table": "<db>/<schema>/docs",
  "column": "body_embed",
  "algo": "hnsw",
  "metric": "cosine",
  "dim": 3072,
  "mode": "REBUILD_ONLY",
  "params": { "M": 32, "ef_build": 200, "ef_search": 64 },
  "status": { "state": "built", "rows_indexed": 12345, "bytes": 1048576, "build_time_ms": 4200 }
}
```

`.vdata` v2 layout stores ANN graph and a compact row‑ID map:
- `row_id` is a 64‑bit identifier used to join results back to table rows; preference is given to table primary keys. If PK is integral, it is stored as is; if PK is string or composite, a stable hashed u64 is stored.
- `flags` encode the `row_id` flavor: e.g., `0=ordinal`, `1=pk_u64`, `2=pk_hashed` (exact values are subject to change; consult `SHOW VECTOR INDEX STATUS`).
- Vector payloads are stored as `f32` internally even if the table column is `List(Float64)`; conversions occur at the boundary.

TVFs and UDF helpers
--------------------
Table‑valued functions expose vector search in a composable way:

- `nearest_neighbors(table, column, qvec, k [, metric, ef_search])`
  - Returns columns: `row_id: u64`, `score: float64`, optional `ord: i64` for stable ordering/joins.
  - Honors per‑call `metric` and `ef_search` when provided; otherwise uses index defaults and session knobs.

- `vector_search(index_name, qvec, k [, topk, engine])`
  - Directly searches a named index; `engine` can hint `ann` or `exact`.

Common UDF helpers available under `scripts/scalars/`:
- `to_vec(text) -> string`
- `cosine_sim(vec, vec) -> float64`
- `vec_l2(vec, vec) -> float64`
- `vec_ip(vec, vec) -> float64`

Examples
--------
Join TVF output back to the source table by `row_id`:
```
WITH q AS (SELECT to_vec('[0.1,0.2,0.3]') AS v)
SELECT d.id, nn.score
FROM nearest_neighbors('public.docs', 'body_embed', (SELECT v FROM q), 10, 'cosine', 96) AS nn
JOIN docs AS d ON d.__row_id.docs = nn.row_id
ORDER BY nn.score DESC, d.id
```

Diagnostics and observability
-----------------------------
- Permanent `tprintln!` breadcrumbs are emitted during planning and execution: chosen engine (EXACT/ANN), metric, ef_search, preselect alpha/W, final k, and explicit fallback reasons.
- `SHOW VECTOR INDEX STATUS` includes: `state, rows_indexed, bytes, dim, metric, engine, build_time_ms, ef_build, ef_search, mode`.
- `EXPLAIN` annotates whether EXACT or ANN path was chosen, the index used, metric, ef_search, preselect W, and any fallback notes.
