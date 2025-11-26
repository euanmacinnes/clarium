--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "asinh(x): returns the inverse hyperbolic sine of x" }
]]

-- asinh(x): returns the inverse hyperbolic sine of x
-- asinh(x) = ln(x + sqrt(x^2 + 1))
function asinh(x)
    if x == nil then return nil end
    return math.log(x + math.sqrt(x * x + 1))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function asinh__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
