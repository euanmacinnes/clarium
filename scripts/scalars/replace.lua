--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "replace(str, from, to): replaces all occurrences of 'from' substring with 'to' substring in str" }
]]

-- replace(str, from, to): replaces all occurrences
function replace(str, from, to)
    if str == nil or from == nil then
        return nil
    end
    str = tostring(str)
    from = tostring(from)
    to = to and tostring(to) or ""
    -- Escape pattern characters in 'from'
    from = from:gsub("([%^%$%(%)%%%.%[%]%*%+%-%?])", "%%%1")
    return str:gsub(from, to)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function replace__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
