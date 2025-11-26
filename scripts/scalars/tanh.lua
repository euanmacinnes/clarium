--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "tanh(x): returns the hyperbolic tangent of x" }
]]

-- tanh(x): returns the hyperbolic tangent of x
-- tanh(x) = sinh(x) / cosh(x) = (e^x - e^-x) / (e^x + e^-x)
function tanh(x)
    if x == nil then return nil end
    local ex = math.exp(x)
    local emx = 1/ex
    return (ex - emx) / (ex + emx)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function tanh__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
