--[[
{ "kind": "scalar", "returns": ["date"], "nullable": true, "version": 1,
  "doc": "to_date(text, format): converts string to a date value" }
]]

-- to_date(text, format): converts text to date
function to_date(text, format)
    if text == nil then
        return nil
    end
    
    text = tostring(text)
    
    -- Try to parse common date formats
    -- ISO format: YYYY-MM-DD
    local year, month, day = text:match("(%d%d%d%d)%-(%d%d)%-(%d%d)")
    if year and month and day then
        return string.format("%04d-%02d-%02d", tonumber(year), tonumber(month), tonumber(day))
    end
    
    -- Format: MM/DD/YYYY
    month, day, year = text:match("(%d%d?)%/(%d%d?)%/(%d%d%d%d)")
    if year and month and day then
        return string.format("%04d-%02d-%02d", tonumber(year), tonumber(month), tonumber(day))
    end
    
    -- Format: DD-MM-YYYY
    day, month, year = text:match("(%d%d?)%-(%d%d?)%-(%d%d%d%d)")
    if year and month and day then
        return string.format("%04d-%02d-%02d", tonumber(year), tonumber(month), tonumber(day))
    end
    
    -- If we can't parse, return nil
    return nil
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function to_date__meta()
    return { kind = "scalar", returns = { "date" }, nullable = true, version = 1 }
end
