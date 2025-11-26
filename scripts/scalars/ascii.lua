--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "ascii(str): returns the ASCII/Unicode code point of the first character in the string" }
]]

-- ascii(str): returns the ASCII/Unicode code of the first character
function ascii(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    if string.len(str) == 0 then
        return nil
    end
    
    -- Try UTF-8 codepoint first (Lua 5.3+)
    if utf8 and utf8.codepoint then
        local success, result = pcall(utf8.codepoint, str, 1)
        if success then
            return result
        end
    end
    
    -- Fallback to byte value for ASCII
    return string.byte(str, 1)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function ascii__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
