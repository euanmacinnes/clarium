--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "acosh(x): returns the inverse hyperbolic cosine of x (x >= 1)" }
]]

-- acosh(x): returns the inverse hyperbolic cosine of x
-- acosh(x) = ln(x + sqrt(x^2 - 1)), valid for x >= 1
function acosh(x)
    if x == nil then return nil end
    if x < 1 then return nil end  -- Domain error: acosh undefined for x < 1
    return math.log(x + math.sqrt(x * x - 1))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function acosh__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
