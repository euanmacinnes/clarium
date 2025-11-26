--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "acot(x): returns the inverse cotangent of x in radians" }
]]

-- acot(x): returns the inverse cotangent of x in radians
-- acot(x) = atan(1/x) for x != 0
-- acot(0) = π/2
function acot(x)
    if x == nil then return nil end
    if x == 0 then
        return math.pi / 2
    end
    return math.atan(1 / x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function acot__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
