--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "get_byte(str, offset): returns the byte value at offset (0-based indexing)" }
]]

-- get_byte(str, offset): gets byte at position
function get_byte(str, offset)
    if str == nil or offset == nil then
        return nil
    end
    str = tostring(str)
    offset = tonumber(offset)
    
    -- PostgreSQL uses 0-based indexing for bytes
    -- Convert to 1-based for Lua
    local pos = offset + 1
    
    if pos < 1 or pos > #str then
        return nil  -- Out of bounds
    end
    
    -- Get byte value at position
    return string.byte(str, pos)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function get_byte__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
