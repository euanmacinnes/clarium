--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "left(str, n): returns the leftmost n characters from the string" }
]]

-- left(str, n): returns the leftmost n characters
function left(str, n)
    if str == nil or n == nil then
        return nil
    end
    str = tostring(str)
    n = tonumber(n)
    
    if n < 0 then
        return ""
    end
    
    return string.sub(str, 1, n)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function left__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
