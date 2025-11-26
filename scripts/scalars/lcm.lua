--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "lcm(a, b): returns the least common multiple of a and b" }
]]

-- Helper function for GCD (Euclidean algorithm)
local function gcd_helper(a, b)
    while b ~= 0 do
        local temp = b
        b = a % b
        a = temp
    end
    return a
end

-- lcm(a, b): returns the least common multiple
function lcm(a, b)
    if a == nil or b == nil then
        return nil
    end
    
    a = math.abs(math.floor(tonumber(a)))
    b = math.abs(math.floor(tonumber(b)))
    
    if a == 0 or b == 0 then
        return 0
    end
    
    -- LCM = (a * b) / GCD(a, b)
    local gcd_val = gcd_helper(a, b)
    return (a * b) / gcd_val
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function lcm__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
