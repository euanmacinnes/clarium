--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "version(): returns the PostgreSQL/TimelineDB version string" }
]]

-- version(): returns the server version string
function version()
    -- Return a PostgreSQL-compatible version string with TimelineDB identifier
    -- In production, this could read from a global set by Rust
    return "PostgreSQL 14.1 on x86_64-pc-linux-musl, compiled by gcc (Alpine 10.3.1_git20211027) 10.3.1 20211027, 64-bit TimelineDB"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function version__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
