--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "atan2(y, x): returns the arc tangent of y/x in radians, using signs to determine quadrant" }
]]

-- atan2(y, x): returns the arc tangent of y/x in radians
function atan2(y, x)
    if y == nil or x == nil then return nil end
    return math.atan(y, x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function atan2__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
