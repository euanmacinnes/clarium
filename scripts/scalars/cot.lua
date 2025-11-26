--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "cot(x): returns the cotangent of x (x in radians), cot(x) = 1/tan(x)" }
]]

-- cot(x): returns the cotangent of x (x in radians)
-- cot(x) = cos(x) / sin(x) = 1 / tan(x)
function cot(x)
    if x == nil then return nil end
    local tan_x = math.tan(x)
    if tan_x == 0 then return nil end  -- Undefined when tan(x) = 0
    return 1 / tan_x
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function cot__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
