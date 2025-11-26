--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "ln(x): returns the natural logarithm of x" }
]]

-- ln(x): returns the natural logarithm of x
function ln(x)
    if x == nil then return nil end
    return math.log(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function ln__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
