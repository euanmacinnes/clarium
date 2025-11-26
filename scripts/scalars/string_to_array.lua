--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "string_to_array(str, delimiter, null_string): splits string into array using delimiter (returns comma-separated values)" }
]]

-- string_to_array(str, delimiter, null_string): splits string into array
function string_to_array(str, delimiter, null_string)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    if delimiter == nil then
        -- If delimiter is NULL, return each character
        local chars = {}
        for i = 1, #str do
            chars[i] = str:sub(i, i)
        end
        return table.concat(chars, ",")
    end
    
    delimiter = tostring(delimiter)
    
    -- Escape delimiter for pattern matching
    local escaped_delim = delimiter:gsub("([%^%$%(%)%%%.%[%]%*%+%-%?])", "%%%1")
    
    -- Split the string
    local parts = {}
    if delimiter == "" then
        -- Empty delimiter: split into characters
        for i = 1, #str do
            parts[i] = str:sub(i, i)
        end
    else
        local pattern = "([^" .. escaped_delim .. "]*)" .. escaped_delim .. "?"
        for part in str:gmatch(pattern) do
            -- Handle null_string conversion
            if null_string ~= nil and part == tostring(null_string) then
                table.insert(parts, "NULL")
            else
                table.insert(parts, part)
            end
        end
    end
    
    -- Return comma-separated array representation
    return table.concat(parts, ",")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function string_to_array__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
