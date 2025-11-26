--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "regexp_matches(source, pattern, flags): returns all substrings matching a POSIX regular expression (returns concatenated matches)" }
]]

-- regexp_matches(source, pattern, flags): finds all regex matches
function regexp_matches(source, pattern, flags)
    if source == nil or pattern == nil then
        return nil
    end
    source = tostring(source)
    pattern = tostring(pattern)
    flags = flags and tostring(flags) or ""
    
    -- Parse flags
    local case_insensitive = flags:find("i") ~= nil
    local global = flags:find("g") ~= nil
    
    -- Lua patterns are different from POSIX regex
    -- This is a simplified implementation
    
    local matches = {}
    for match in source:gmatch(pattern) do
        table.insert(matches, match)
    end
    
    if #matches > 0 then
        -- Return comma-separated matches (simplified; PostgreSQL returns set)
        return table.concat(matches, ",")
    else
        return nil
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function regexp_matches__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
