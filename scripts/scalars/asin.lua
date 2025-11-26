--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "asin(x): returns the arc sine of x in radians" }
]]

-- asin(x): returns the arc sine of x in radians
function asin(x)
    if x == nil then return nil end
    return math.asin(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function asin__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
