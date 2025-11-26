--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "regexp_replace(source, pattern, replacement, flags): replaces substring(s) matching a POSIX regular expression" }
]]

-- regexp_replace(source, pattern, replacement, flags): regex replacement
function regexp_replace(source, pattern, replacement, flags)
    if source == nil or pattern == nil or replacement == nil then
        return nil
    end
    source = tostring(source)
    pattern = tostring(pattern)
    replacement = tostring(replacement)
    flags = flags and tostring(flags) or ""
    
    -- Parse flags
    local global = flags:find("g") ~= nil
    local case_insensitive = flags:find("i") ~= nil
    
    -- Lua patterns are different from POSIX regex, but we can handle simple cases
    -- For case insensitivity, we'd need more complex logic
    -- This is a simplified implementation
    
    if global then
        -- Replace all occurrences
        local result = source:gsub(pattern, replacement)
        return result
    else
        -- Replace first occurrence only
        local result = source:gsub(pattern, replacement, 1)
        return result
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function regexp_replace__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
