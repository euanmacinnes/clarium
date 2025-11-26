--[[
{ "kind": "scalar", "returns": ["boolean"], "nullable": true, "version": 1,
  "doc": "isfinite(value): returns true if the value is a finite number (not infinity or NaN)" }
]]

-- isfinite(value): checks if value is finite
function isfinite(value)
    if value == nil then
        return nil
    end
    
    local num = tonumber(value)
    if num == nil then
        return false
    end
    
    -- Check for infinity and NaN
    -- In Lua, infinity is represented as math.huge
    if num == math.huge or num == -math.huge then
        return false
    end
    
    -- Check for NaN (NaN is not equal to itself)
    if num ~= num then
        return false
    end
    
    return true
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function isfinite__meta()
    return { kind = "scalar", returns = { "boolean" }, nullable = true, version = 1 }
end
