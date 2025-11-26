--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "overlay(str, replacement, start, length): replaces part of str with replacement starting at position start for length characters" }
]]

-- overlay(str, replacement, start, length): overlays replacement string
function overlay(str, replacement, start, len)
    if str == nil or replacement == nil or start == nil then
        return nil
    end
    str = tostring(str)
    replacement = tostring(replacement)
    start = tonumber(start)
    len = len and tonumber(len) or string.len(replacement)
    
    if start < 1 then
        start = 1
    end
    
    -- Extract parts before and after the overlay position
    local before = string.sub(str, 1, start - 1)
    local after = string.sub(str, start + len)
    
    return before .. replacement .. after
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function overlay__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
