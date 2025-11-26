--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "acos(x): returns the arc cosine of x in radians" }
]]

-- acos(x): returns the arc cosine of x in radians
function acos(x)
    if x == nil then return nil end
    return math.acos(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function acos__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
