--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "acoth(x): returns the inverse hyperbolic cotangent of x (|x| > 1)" }
]]

-- acoth(x): returns the inverse hyperbolic cotangent of x
-- acoth(x) = 0.5 * ln((x + 1) / (x - 1)), valid for |x| > 1
function acoth(x)
    if x == nil then return nil end
    if x >= -1 and x <= 1 then return nil end  -- Domain error: acoth undefined for |x| <= 1
    return 0.5 * math.log((x + 1) / (x - 1))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function acoth__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
