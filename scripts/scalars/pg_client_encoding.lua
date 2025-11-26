--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "pg_client_encoding(): returns the current client encoding name" }
]]

-- pg_client_encoding(): returns client encoding
function pg_client_encoding()
    -- Return UTF8 as the default/assumed client encoding
    -- In a full implementation, this would query the actual client encoding setting
    return "UTF8"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function pg_client_encoding__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
