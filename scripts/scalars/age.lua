--[[
{ "kind": "scalar", "returns": ["interval"], "nullable": true, "version": 1,
  "doc": "age(timestamp1, timestamp2): returns the interval between two timestamps; age(timestamp): returns interval from timestamp to current time" }
]]

-- age(timestamp1, timestamp2): returns the time difference
function age(ts1, ts2)
    if ts1 == nil then
        return nil
    end
    
    -- Convert to numbers (Unix timestamps)
    local t1 = tonumber(ts1)
    local t2 = ts2 ~= nil and tonumber(ts2) or os.time()
    
    if t1 == nil or t2 == nil then
        return nil
    end
    
    -- Calculate difference in seconds
    local diff = math.abs(t2 - t1)
    
    -- Convert to days, hours, minutes, seconds
    local days = math.floor(diff / 86400)
    local remaining = diff % 86400
    local hours = math.floor(remaining / 3600)
    remaining = remaining % 3600
    local minutes = math.floor(remaining / 60)
    local seconds = remaining % 60
    
    -- Format as PostgreSQL interval string
    if days > 0 then
        return string.format("%d days %02d:%02d:%02d", days, hours, minutes, seconds)
    else
        return string.format("%02d:%02d:%02d", hours, minutes, seconds)
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function age__meta()
    return { kind = "scalar", returns = { "interval" }, nullable = true, version = 1 }
end
