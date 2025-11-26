--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "current_user: returns the user name of the current execution context" }
]]

-- current_user: returns the current user name
function current_user()
    -- Call Rust-provided context accessor function
    local user = get_context("current_user")
    if user then
        return user
    end
    return "postgres"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function current_user__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
