--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "initcap(str): capitalizes the first letter of each word in the string" }
]]

-- initcap(str): capitalizes first letter of each word
function initcap(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- Capitalize first letter after whitespace or at start
    local result = str:gsub("(%a)([%w_']*)", function(first, rest)
        return string.upper(first) .. string.lower(rest)
    end)
    
    return result
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function initcap__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
