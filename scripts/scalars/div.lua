--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "div(y, x): returns the integer quotient of y/x (truncates toward zero)" }
]]

-- div(y, x): integer division (quotient)
function div(y, x)
    if y == nil or x == nil then
        return nil
    end
    y = tonumber(y)
    x = tonumber(x)
    
    if x == 0 then
        return nil  -- Division by zero
    end
    
    -- Integer division truncating toward zero
    local quotient = y / x
    if quotient >= 0 then
        return math.floor(quotient)
    else
        return math.ceil(quotient)
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function div__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
