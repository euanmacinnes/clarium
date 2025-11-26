--[[
{ "kind": "scalar", "returns": ["timestamp"], "nullable": true, "version": 1,
  "doc": "date_trunc(precision, source): truncates timestamp to specified precision (year, month, day, hour, minute, second)" }
]]

-- date_trunc(precision, source): truncates timestamp to specified precision
function date_trunc(precision, source)
    if precision == nil or source == nil then return nil end
    
    -- Normalize precision to lowercase
    local p = string.lower(tostring(precision))
    
    -- Handle both Unix timestamp and date string
    local ts
    if type(source) == "number" then
        ts = source
    elseif type(source) == "string" then
        ts = tonumber(source) or os.time()
    else
        ts = os.time()
    end
    
    local t = os.date("*t", ts)
    
    -- Truncate based on precision
    if p == "year" then
        t.month = 1
        t.day = 1
        t.hour = 0
        t.min = 0
        t.sec = 0
    elseif p == "month" then
        t.day = 1
        t.hour = 0
        t.min = 0
        t.sec = 0
    elseif p == "day" then
        t.hour = 0
        t.min = 0
        t.sec = 0
    elseif p == "hour" then
        t.min = 0
        t.sec = 0
    elseif p == "minute" or p == "min" then
        t.sec = 0
    elseif p == "second" or p == "sec" then
        -- No truncation needed
    else
        return nil
    end
    
    return os.time(t)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function date_trunc__meta()
    return { kind = "scalar", returns = { "timestamp" }, nullable = true, version = 1 }
end
