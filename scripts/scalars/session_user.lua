--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "session_user: returns the session user name" }
]]

-- session_user: returns the session user name
function session_user()
    -- Call Rust-provided context accessor function
    local user = get_context("session_user")
    if user then
        return user
    end
    return "postgres"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function session_user__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
