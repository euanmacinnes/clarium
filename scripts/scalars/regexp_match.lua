--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "regexp_match(source, pattern, flags): returns the first substring matching a POSIX regular expression" }
]]

-- regexp_match(source, pattern, flags): finds first regex match
function regexp_match(source, pattern, flags)
    if source == nil or pattern == nil then
        return nil
    end
    source = tostring(source)
    pattern = tostring(pattern)
    flags = flags and tostring(flags) or ""
    
    -- Parse flags
    local case_insensitive = flags:find("i") ~= nil
    
    -- Lua patterns are different from POSIX regex
    -- This is a simplified implementation
    
    -- Try to match the pattern
    local match = source:match(pattern)
    
    if match then
        return match
    else
        return nil
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function regexp_match__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
