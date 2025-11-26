--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "cos(x): returns the cosine of x (x in radians)" }
]]

-- cos(x): returns the cosine of x (x in radians)
function cos(x)
    if x == nil then return nil end
    return math.cos(x)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function cos__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
