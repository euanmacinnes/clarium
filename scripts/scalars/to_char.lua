--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "to_char(value, format): converts a timestamp, number, or other value to a formatted string" }
]]

-- to_char(value, format): converts value to formatted string
function to_char(value, format)
    if value == nil then
        return nil
    end
    
    -- If no format specified, convert to string
    if format == nil then
        return tostring(value)
    end
    
    format = tostring(format)
    
    -- Handle timestamp formatting
    if type(value) == "number" then
        -- Check if it looks like a Unix timestamp (large number)
        if value > 1000000000 then
            -- Common PostgreSQL date/time format patterns
            if format:find("YYYY") or format:find("MM") or format:find("DD") then
                local t = os.date("*t", value)
                local result = format
                result = result:gsub("YYYY", string.format("%04d", t.year))
                result = result:gsub("MM", string.format("%02d", t.month))
                result = result:gsub("DD", string.format("%02d", t.day))
                result = result:gsub("HH24", string.format("%02d", t.hour))
                result = result:gsub("HH12", string.format("%02d", t.hour % 12 == 0 and 12 or t.hour % 12))
                result = result:gsub("MI", string.format("%02d", t.min))
                result = result:gsub("SS", string.format("%02d", t.sec))
                return result
            else
                -- Numeric formatting
                return string.format("%.2f", value)
            end
        else
            -- Small number - format as numeric
            return string.format("%.2f", value)
        end
    end
    
    -- Default: convert to string
    return tostring(value)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function to_char__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
