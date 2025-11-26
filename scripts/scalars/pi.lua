--[[
{ "kind": "scalar", "returns": ["number"], "nullable": false, "version": 1,
  "doc": "pi(): returns the mathematical constant π (3.14159...)" }
]]

-- pi(): returns the mathematical constant π
function pi()
    return math.pi
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function pi__meta()
    return { kind = "scalar", returns = { "number" }, nullable = false, version = 1 }
end
