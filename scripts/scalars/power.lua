--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "aliases": ["pow"],
  "doc": "power(x, y): returns x raised to the power of y" }
]]

-- power(x, y): returns x raised to the power of y
function power(x, y)
    if x == nil or y == nil then return nil end
    return math.pow(x, y)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function power__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1, aliases = { "pow" } }
end
