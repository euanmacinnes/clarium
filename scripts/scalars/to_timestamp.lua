--[[
{ "kind": "scalar", "returns": ["timestamp"], "nullable": true, "version": 1,
  "doc": "to_timestamp(text, format): converts string to a timestamp value; to_timestamp(unix_timestamp): converts Unix epoch to timestamp" }
]]

-- to_timestamp(text, format): converts text to timestamp
function to_timestamp(text, format)
    if text == nil then
        return nil
    end
    
    -- If text is a number, treat as Unix timestamp
    local unix_ts = tonumber(text)
    if unix_ts ~= nil then
        return unix_ts
    end
    
    text = tostring(text)
    
    -- Try to parse ISO timestamp: YYYY-MM-DD HH:MI:SS
    local year, month, day, hour, min, sec = text:match("(%d%d%d%d)%-(%d%d)%-(%d%d)%s+(%d%d):(%d%d):(%d%d)")
    if year and month and day and hour and min and sec then
        local t = {
            year = tonumber(year),
            month = tonumber(month),
            day = tonumber(day),
            hour = tonumber(hour),
            min = tonumber(min),
            sec = tonumber(sec)
        }
        return os.time(t)
    end
    
    -- Try to parse date only: YYYY-MM-DD
    year, month, day = text:match("(%d%d%d%d)%-(%d%d)%-(%d%d)")
    if year and month and day then
        local t = {
            year = tonumber(year),
            month = tonumber(month),
            day = tonumber(day),
            hour = 0,
            min = 0,
            sec = 0
        }
        return os.time(t)
    end
    
    -- If we can't parse, return nil
    return nil
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function to_timestamp__meta()
    return { kind = "scalar", returns = { "timestamp" }, nullable = true, version = 1 }
end
