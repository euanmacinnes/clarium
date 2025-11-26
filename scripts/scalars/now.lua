--[[
{ "kind": "scalar", "returns": ["timestamp"], "nullable": false, "version": 1,
  "doc": "now(): returns the current date and time" }
]]

-- now(): returns the current date and time as Unix timestamp
function now()
    return os.time()
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function now__meta()
    return { kind = "scalar", returns = { "timestamp" }, nullable = false, version = 1 }
end
