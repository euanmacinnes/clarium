--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "floor(n): returns the largest integer less than or equal to n" }
]]

-- floor(n): rounds down to the nearest integer
function floor(n)
    if n == nil then
        return nil
    end
    return math.floor(tonumber(n))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function floor__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
