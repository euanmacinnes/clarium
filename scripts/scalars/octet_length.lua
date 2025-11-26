--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "octet_length(str): returns the number of bytes (octets) in the string" }
]]

-- octet_length(str): returns the number of bytes/octets
function octet_length(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- Return byte length
    return string.len(str)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function octet_length__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
