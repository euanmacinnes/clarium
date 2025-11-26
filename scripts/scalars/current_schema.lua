--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "current_schema(): returns the name of the current schema" }
]]

-- current_schema(): returns the current schema name
function current_schema()
    -- Call Rust-provided context accessor function
    local schema = get_context("current_schema")
    if schema then
        return schema
    end
    return "public"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function current_schema__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
