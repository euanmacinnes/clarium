--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "rpad(str, length, fill): pads the string to the specified length by appending fill characters (default space)" }
]]

-- rpad(str, length, fill): right-pads string to length with fill characters
function rpad(str, len, fill)
    if str == nil or len == nil then
        return nil
    end
    str = tostring(str)
    len = tonumber(len)
    fill = fill and tostring(fill) or " "
    
    if fill == "" then
        return str
    end
    
    local current_len = string.len(str)
    if current_len >= len then
        return string.sub(str, 1, len)
    end
    
    local pad_len = len - current_len
    local fill_len = string.len(fill)
    local repetitions = math.ceil(pad_len / fill_len)
    local padding = string.rep(fill, repetitions)
    padding = string.sub(padding, 1, pad_len)
    
    return str .. padding
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function rpad__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
