--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "width_bucket(operand, min, max, count): returns the bucket number to which operand would be assigned in a histogram with count equal-width buckets spanning min to max" }
]]

-- width_bucket(operand, min, max, count): returns bucket number for histogram
function width_bucket(operand, b1, b2, count)
    if operand == nil or b1 == nil or b2 == nil or count == nil then
        return nil
    end
    
    operand = tonumber(operand)
    b1 = tonumber(b1)
    b2 = tonumber(b2)
    count = tonumber(count)
    
    if count <= 0 then
        return nil  -- Invalid bucket count
    end
    
    if b1 == b2 then
        return nil  -- Invalid range
    end
    
    -- Handle reversed bounds (b2 < b1)
    if b2 < b1 then
        if operand > b1 then
            return 0
        elseif operand < b2 then
            return count + 1
        else
            return count - math.floor((operand - b2) / (b1 - b2) * count) + 1
        end
    else
        -- Normal case (b1 < b2)
        if operand < b1 then
            return 0
        elseif operand >= b2 then
            return count + 1
        else
            return math.floor((operand - b1) / (b2 - b1) * count) + 1
        end
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function width_bucket__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
