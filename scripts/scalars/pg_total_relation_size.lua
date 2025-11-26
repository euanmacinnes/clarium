--[[
{ "kind": "scalar", "returns": ["int64"], "nullable": true, "version": 1,
  "doc": "pg_total_relation_size(relation_oid): returns the total disk space used by the specified table and all its indexes" }
]]

-- pg_total_relation_size(relation_oid): returns total disk space in bytes
-- In PostgreSQL, this function returns the total disk space used by a table including indexes and TOAST data.
-- Timeline doesn't track detailed storage metrics per table, so this is a stub that returns 0.
function pg_total_relation_size(relation_oid)
    -- Return 0 to indicate minimal/unknown size
    -- A more sophisticated implementation could scan parquet files and sum their sizes
    return 0
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function pg_total_relation_size__meta()
    return { kind = "scalar", returns = { "int64" }, nullable = true, version = 1 }
end
