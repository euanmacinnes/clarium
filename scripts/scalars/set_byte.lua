--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "set_byte(str, offset, newvalue): sets the byte at offset to newvalue (0-based indexing)" }
]]

-- set_byte(str, offset, newvalue): sets byte at position
function set_byte(str, offset, newvalue)
    if str == nil or offset == nil or newvalue == nil then
        return nil
    end
    str = tostring(str)
    offset = tonumber(offset)
    newvalue = tonumber(newvalue)
    
    -- PostgreSQL uses 0-based indexing for bytes
    -- Convert to 1-based for Lua
    local pos = offset + 1
    
    if pos < 1 or pos > #str then
        return str  -- Out of bounds, return unchanged
    end
    
    -- Validate byte value (0-255)
    if newvalue < 0 or newvalue > 255 then
        return nil
    end
    
    -- Build new string with replaced byte
    local before = str:sub(1, pos - 1)
    local after = str:sub(pos + 1)
    local new_byte = string.char(newvalue)
    
    return before .. new_byte .. after
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function set_byte__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
