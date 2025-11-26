--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "to_hex(n): converts an integer to its hexadecimal representation" }
]]

-- to_hex(n): converts integer to hexadecimal string
function to_hex(n)
    if n == nil then
        return nil
    end
    n = tonumber(n)
    
    if n == nil then
        return nil
    end
    
    -- Convert to integer if needed
    local int_val = math.floor(n)
    
    -- Handle negative numbers (two's complement for 32-bit)
    if int_val < 0 then
        int_val = int_val + 4294967296  -- 2^32
    end
    
    return string.format("%x", int_val)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function to_hex__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
