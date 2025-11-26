--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "sin(x): returns the sine of x (x in radians)" }
]]

-- sin(x): returns the sine of x (x in radians)
function sin(x)
    if x == nil then return nil end
    return math.sin(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function sin__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
