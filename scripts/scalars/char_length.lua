--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1, "aliases": ["character_length"],
  "doc": "char_length(str): returns the number of characters in the string (alias for length)" }
]]

-- char_length(str): returns the number of characters
-- This is an alias for length, matching PostgreSQL behavior
function char_length(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- For UTF-8 strings, try to use utf8.len if available
    if utf8 and utf8.len then
        local len = utf8.len(str)
        if len then
            return len
        end
    end
    
    -- Fallback to byte length
    return string.len(str)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function char_length__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1, aliases = { "character_length" } }
end
