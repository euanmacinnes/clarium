--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "atan(x): returns the arc tangent of x in radians" }
]]

-- atan(x): returns the arc tangent of x in radians
function atan(x)
    if x == nil then return nil end
    return math.atan(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function atan__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
