--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "round(n, precision): rounds a number to the specified decimal places (default 0)" }
]]

-- round(n, precision): rounds to specified decimal places
function round(n, precision)
    if n == nil then
        return nil
    end
    n = tonumber(n)
    precision = precision or 0
    local mult = 10 ^ precision
    return math.floor(n * mult + 0.5) / mult
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function round__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
