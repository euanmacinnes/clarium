--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "pg_get_viewdef(view_oid): returns the CREATE VIEW statement for a view, or NULL if not a view or views not supported" }
]]

-- pg_get_viewdef(view_oid): returns view definition
-- Timeline doesn't currently support views, so always return NULL
function pg_get_viewdef(view_oid)
    -- In a full implementation, this would look up the view definition
    -- For now, Timeline doesn't support views, so return NULL
    return nil
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function pg_get_viewdef__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
