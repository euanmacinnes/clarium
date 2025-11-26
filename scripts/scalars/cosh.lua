--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "cosh(x): returns the hyperbolic cosine of x" }
]]

-- cosh(x): returns the hyperbolic cosine of x
-- cosh(x) = (e^x + e^-x) / 2
function cosh(x)
    if x == nil then return nil end
    local ex = math.exp(x)
    return (ex + 1/ex) / 2
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function cosh__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
