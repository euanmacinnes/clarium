--[[
{ "kind": "scalar", "returns": ["number"], "nullable": false, "version": 1,
  "doc": "random(): returns a random value in the range 0.0 <= x < 1.0" }
]]

-- random(): returns a random number between 0 and 1
function random()
    return math.random()
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function random__meta()
    return { kind = "scalar", returns = { "number" }, nullable = false, version = 1 }
end
