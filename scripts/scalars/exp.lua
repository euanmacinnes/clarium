--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "exp(x): returns e raised to the power of x" }
]]

-- exp(x): returns e raised to the power of x
function exp(x)
    if x == nil then return nil end
    return math.exp(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function exp__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
