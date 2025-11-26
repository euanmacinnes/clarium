--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "atanh(x): returns the inverse hyperbolic tangent of x (-1 < x < 1)" }
]]

-- atanh(x): returns the inverse hyperbolic tangent of x
-- atanh(x) = 0.5 * ln((1 + x) / (1 - x)), valid for -1 < x < 1
function atanh(x)
    if x == nil then return nil end
    if x <= -1 or x >= 1 then return nil end  -- Domain error: atanh undefined for |x| >= 1
    return 0.5 * math.log((1 + x) / (1 - x))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function atanh__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
