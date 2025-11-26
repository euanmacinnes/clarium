--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "mod(x, y): returns the remainder of x divided by y (modulo operation)" }
]]

-- mod(x, y): returns x modulo y
function mod(x, y)
    if x == nil or y == nil then
        return nil
    end
    x = tonumber(x)
    y = tonumber(y)
    if y == 0 then
        return nil  -- Division by zero returns NULL in PostgreSQL
    end
    return x % y
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function mod__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
