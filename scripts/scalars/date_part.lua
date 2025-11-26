--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "date_part(field, source): extracts a field (year, month, day, hour, minute, second) from a timestamp (alias for extract)" }
]]

-- date_part(field, source): extracts a specific field from a timestamp
function date_part(field, source)
    if field == nil or source == nil then return nil end
    
    -- Normalize field name to lowercase
    local f = string.lower(tostring(field))
    
    -- Handle both Unix timestamp and date string
    local t
    if type(source) == "number" then
        t = os.date("*t", source)
    elseif type(source) == "string" then
        -- Try to parse as Unix timestamp
        local ts = tonumber(source)
        if ts then
            t = os.date("*t", ts)
        else
            -- Assume current time if can't parse
            t = os.date("*t")
        end
    else
        t = os.date("*t")
    end
    
    -- Extract the requested field
    if f == "year" then return t.year
    elseif f == "month" then return t.month
    elseif f == "day" then return t.day
    elseif f == "hour" then return t.hour
    elseif f == "minute" or f == "min" then return t.min
    elseif f == "second" or f == "sec" then return t.sec
    elseif f == "dow" or f == "dayofweek" then return t.wday - 1  -- PostgreSQL uses 0-6
    elseif f == "doy" or f == "dayofyear" then return t.yday
    else return nil
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function date_part__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
