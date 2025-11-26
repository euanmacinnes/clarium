--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "radians(x): converts degrees to radians" }
]]

-- radians(x): converts degrees to radians
function radians(x)
    if x == nil then return nil end
    return x * math.pi / 180
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function radians__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
