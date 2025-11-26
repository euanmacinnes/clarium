--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1, "aliases": ["replicate"],
  "doc": "repeat_string(str, n): repeats the string n times; aliased as repeat and replicate" }
]]

-- repeat_string(str, n): repeats string n times
-- Note: named repeat_string because 'repeat' is a Lua reserved keyword
function repeat_string(str, n)
    if str == nil or n == nil then
        return nil
    end
    str = tostring(str)
    n = tonumber(n)
    
    if n < 0 then
        return nil
    end
    
    if n == 0 then
        return ""
    end
    
    return string.rep(str, n)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function repeat_string__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1, aliases = { "repeat", "replicate" } }
end
