--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "lpad(str, length, fill): pads the string to the specified length by prepending fill characters (default space)" }
]]

-- lpad(str, length, fill): left-pads string to length with fill characters
function lpad(str, len, fill)
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
    
    return padding .. str
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function lpad__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
