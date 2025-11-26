--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "regexp_split_to_array(source, pattern, flags): splits string using a regex pattern (returns comma-separated values)" }
]]

-- regexp_split_to_array(source, pattern, flags): splits string by regex
function regexp_split_to_array(source, pattern, flags)
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
    
    -- Escape pattern if needed for literal split
    -- For now, use pattern directly
    local parts = {}
    local last_pos = 1
    
    for match_start, match_end in source:gmatch("()" .. pattern .. "()") do
        if match_start > last_pos then
            table.insert(parts, source:sub(last_pos, match_start - 1))
        else
            table.insert(parts, "")
        end
        last_pos = match_end
    end
    
    -- Add remaining part
    if last_pos <= #source then
        table.insert(parts, source:sub(last_pos))
    end
    
    -- Return comma-separated array representation
    return table.concat(parts, ",")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function regexp_split_to_array__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
