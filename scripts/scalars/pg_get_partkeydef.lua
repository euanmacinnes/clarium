--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "pg_get_partkeydef(relation_oid): returns the partition key definition for a partitioned table" }
]]

-- pg_get_partkeydef(relation_oid): returns partition key definition
-- In PostgreSQL, this function returns the partition key definition for a partitioned table.
-- Timeline doesn't currently support partitioned tables in the PostgreSQL sense,
-- so this is a minimal stub implementation that returns NULL.
function pg_get_partkeydef(relation_oid)
    -- Timeline tables don't have PostgreSQL-style partition keys
    -- Return NULL to indicate no partition key is defined
    return nil
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function pg_get_partkeydef__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
