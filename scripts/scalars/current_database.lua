--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "current_database(): returns the name of the current database" }
]]

-- current_database(): returns the current database name
function current_database()
    -- Call Rust-provided context accessor function
    local db = get_context("current_database")
    if db then
        return db
    end
    return "timeline"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function current_database__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
