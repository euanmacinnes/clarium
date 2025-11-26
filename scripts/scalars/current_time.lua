--[[
{ "kind": "scalar", "returns": ["time"], "nullable": false, "version": 1,
  "aliases": ["curtime"],
  "doc": "current_time(): returns the current time" }
]]

-- current_time(): returns the current time (HH:MM:SS format)
function current_time()
    return os.date("%H:%M:%S")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function current_time__meta()
    return { kind = "scalar", returns = { "time" }, nullable = false, version = 1, aliases = { "curtime" } }
end
