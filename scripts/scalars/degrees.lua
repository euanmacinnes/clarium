--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "degrees(x): converts radians to degrees" }
]]

-- degrees(x): converts radians to degrees
function degrees(x)
    if x == nil then return nil end
    return x * 180 / math.pi
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function degrees__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
