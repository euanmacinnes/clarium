--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "tan(x): returns the tangent of x (x in radians)" }
]]

-- tan(x): returns the tangent of x (x in radians)
function tan(x)
    if x == nil then return nil end
    return math.tan(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function tan__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
