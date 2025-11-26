--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "ceil(n): returns the smallest integer greater than or equal to n" }
]]

-- ceil(n): rounds up to the nearest integer
function ceil(n)
    if n == nil then
        return nil
    end
    return math.ceil(tonumber(n))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function ceil__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
