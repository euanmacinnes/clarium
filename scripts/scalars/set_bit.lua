--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "set_bit(str, offset, newvalue): sets the bit at offset to newvalue (0 or 1, 0-based indexing)" }
]]

-- set_bit(str, offset, newvalue): sets bit at position
function set_bit(str, offset, newvalue)
    if str == nil or offset == nil or newvalue == nil then
        return nil
    end
    str = tostring(str)
    offset = tonumber(offset)
    newvalue = tonumber(newvalue)
    
    -- Validate bit value (0 or 1)
    if newvalue ~= 0 and newvalue ~= 1 then
        return nil
    end
    
    -- Calculate byte position and bit position within byte
    local byte_pos = math.floor(offset / 8) + 1  -- 1-based for Lua
    local bit_pos = offset % 8
    
    if byte_pos < 1 or byte_pos > #str then
        return str  -- Out of bounds, return unchanged
    end
    
    -- Get current byte value
    local byte_val = string.byte(str, byte_pos)
    
    -- Set or clear the bit (bits are numbered from most significant to least)
    local bit_mask = 128 >> bit_pos  -- 2^(7-bit_pos)
    if newvalue == 1 then
        byte_val = byte_val | bit_mask  -- Set bit
    else
        byte_val = byte_val & (~bit_mask)  -- Clear bit
    end
    
    -- Build new string with replaced byte
    local before = str:sub(1, byte_pos - 1)
    local after = str:sub(byte_pos + 1)
    local new_byte = string.char(byte_val)
    
    return before .. new_byte .. after
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function set_bit__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
