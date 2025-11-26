--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "substring(str, start, length): extracts a substring from str starting at position start (1-based) for length characters" }
]]

-- substring(str, start, length): extracts substring
-- PostgreSQL uses 1-based indexing, which matches Lua
function substring(str, start, len)
    if str == nil then
        return nil
    end
    str = tostring(str)
    start = start or 1
    if len == nil then
        return string.sub(str, start)
    else
        return string.sub(str, start, start + len - 1)
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function substring__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
