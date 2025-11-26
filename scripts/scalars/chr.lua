--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "chr(n): returns the character with the given ASCII/Unicode code point" }
]]

-- chr(n): converts ASCII/Unicode code to character
function chr(n)
    if n == nil then
        return nil
    end
    n = tonumber(n)
    
    if n < 0 or n > 1114111 then  -- Valid Unicode range
        return nil
    end
    
    -- Lua 5.3+ has utf8.char, fallback to string.char for ASCII
    if utf8 and utf8.char then
        local success, result = pcall(utf8.char, n)
        if success then
            return result
        end
    end
    
    -- Fallback for ASCII range
    if n <= 255 then
        return string.char(n)
    end
    
    return nil
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function chr__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
