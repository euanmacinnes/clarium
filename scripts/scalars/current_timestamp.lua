--[[
{ "kind": "scalar", "returns": ["timestamp"], "nullable": false, "version": 1,
  "doc": "current_timestamp(): returns the current date and time as a timestamp" }
]]

-- current_timestamp(): returns the current date and time as a formatted timestamp
function current_timestamp()
    return os.date("%Y-%m-%d %H:%M:%S")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function current_timestamp__meta()
    return { kind = "scalar", returns = { "timestamp" }, nullable = false, version = 1 }
end
