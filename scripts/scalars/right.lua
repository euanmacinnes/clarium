--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "right(str, n): returns the rightmost n characters from the string" }
]]

-- right(str, n): returns the rightmost n characters
function right(str, n)
    if str == nil or n == nil then
        return nil
    end
    str = tostring(str)
    n = tonumber(n)
    
    if n < 0 then
        return ""
    end
    
    local len = string.len(str)
    return string.sub(str, -n)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function right__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
