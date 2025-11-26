--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "sqrt(x): returns the square root of x" }
]]

-- sqrt(x): returns the square root of x
function sqrt(x)
    if x == nil then return nil end
    return math.sqrt(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function sqrt__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
