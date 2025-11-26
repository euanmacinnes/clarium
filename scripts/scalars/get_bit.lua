--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "get_bit(str, offset): returns the bit value (0 or 1) at offset (0-based indexing)" }
]]

-- get_bit(str, offset): gets bit at position
function get_bit(str, offset)
    if str == nil or offset == nil then
        return nil
    end
    str = tostring(str)
    offset = tonumber(offset)
    
    -- Calculate byte position and bit position within byte
    local byte_pos = math.floor(offset / 8) + 1  -- 1-based for Lua
    local bit_pos = offset % 8
    
    if byte_pos < 1 or byte_pos > #str then
        return nil  -- Out of bounds
    end
    
    -- Get byte value at position
    local byte_val = string.byte(str, byte_pos)
    
    -- Extract the bit (bits are numbered from most significant to least)
    local bit_mask = 128 >> bit_pos  -- 2^(7-bit_pos)
    if (byte_val & bit_mask) ~= 0 then
        return 1
    else
        return 0
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function get_bit__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
