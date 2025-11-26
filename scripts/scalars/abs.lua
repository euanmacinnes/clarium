--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "abs(n): returns the absolute value of a number" }
]]

-- abs(n): returns the absolute value
function abs(n)
    if n == nil then
        return nil
    end
    return math.abs(tonumber(n))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function abs__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
