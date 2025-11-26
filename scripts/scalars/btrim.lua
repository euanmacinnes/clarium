--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "btrim(str, characters): removes characters (default whitespace) from both ends of the string" }
]]

-- btrim(str, characters): removes specified characters from both ends
function btrim(str, characters)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    if characters == nil or characters == "" then
        -- Default: trim whitespace
        return str:match("^%s*(.-)%s*$")
    end
    
    characters = tostring(characters)
    -- Escape special pattern characters
    local escaped = characters:gsub("([%^%$%(%)%%%.%[%]%*%+%-%?])", "%%%1")
    -- Create character class
    local pattern = "^[" .. escaped .. "]*(.-)[ " .. escaped .. "]*$"
    return str:match(pattern) or str
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function btrim__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
