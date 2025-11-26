--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "gcd(a, b): returns the greatest common divisor of a and b" }
]]

-- gcd(a, b): returns the greatest common divisor
function gcd(a, b)
    if a == nil or b == nil then
        return nil
    end
    
    a = math.abs(math.floor(tonumber(a)))
    b = math.abs(math.floor(tonumber(b)))
    
    -- Euclidean algorithm
    while b ~= 0 do
        local temp = b
        b = a % b
        a = temp
    end
    
    return a
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function gcd__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
