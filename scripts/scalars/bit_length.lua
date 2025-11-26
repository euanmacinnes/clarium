--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "bit_length(str): returns the number of bits in the string (length in bytes * 8)" }
]]

-- bit_length(str): returns the number of bits
function bit_length(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- Each byte is 8 bits
    return string.len(str) * 8
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function bit_length__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
