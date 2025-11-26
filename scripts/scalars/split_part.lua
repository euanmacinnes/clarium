--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "split_part(str, delimiter, field): splits string on delimiter and returns the nth field (1-based)" }
]]

-- split_part(str, delimiter, field): splits string and returns specified field
function split_part(str, delimiter, field)
    if str == nil or delimiter == nil or field == nil then
        return nil
    end
    str = tostring(str)
    delimiter = tostring(delimiter)
    field = tonumber(field)
    
    if field < 1 then
        return nil
    end
    
    -- Escape delimiter for pattern matching
    local escaped_delim = delimiter:gsub("([%^%$%(%)%%%.%[%]%*%+%-%?])", "%%%1")
    
    -- Split the string
    local parts = {}
    local pattern = "([^" .. escaped_delim .. "]*)" .. escaped_delim .. "?"
    for part in str:gmatch(pattern) do
        table.insert(parts, part)
    end
    
    -- Return the requested field or empty string if out of bounds
    if field <= #parts then
        return parts[field]
    else
        return ""
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function split_part__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
