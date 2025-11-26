--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "sinh(x): returns the hyperbolic sine of x" }
]]

-- sinh(x): returns the hyperbolic sine of x
-- sinh(x) = (e^x - e^-x) / 2
function sinh(x)
    if x == nil then return nil end
    local ex = math.exp(x)
    return (ex - 1/ex) / 2
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function sinh__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
