--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "decode(data, format): decodes data from the specified format (hex, escape, base64)" }
]]

-- decode(data, format): decodes data from specified format
function decode(data, format)
    if data == nil or format == nil then
        return nil
    end
    data = tostring(data)
    format = string.lower(tostring(format))
    
    if format == "hex" then
        -- Decode hex to string
        local result = ""
        for i = 1, #data, 2 do
            local hex_byte = data:sub(i, i+1)
            local byte_val = tonumber(hex_byte, 16)
            if byte_val then
                result = result .. string.char(byte_val)
            else
                return nil  -- Invalid hex
            end
        end
        return result
    elseif format == "escape" then
        -- Unescape special characters
        local unescaped = data:gsub("\\(.)", "%1")
        return unescaped
    elseif format == "base64" then
        -- Simplified base64 decoding
        local b64chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        local result = ""
        
        -- Remove padding
        data = data:gsub("=", "")
        
        for i = 1, #data, 4 do
            local c1 = b64chars:find(data:sub(i, i)) or 1
            local c2 = b64chars:find(data:sub(i+1, i+1)) or 1
            local c3 = b64chars:find(data:sub(i+2, i+2)) or 1
            local c4 = b64chars:find(data:sub(i+3, i+3)) or 1
            
            c1 = c1 - 1
            c2 = c2 - 1
            c3 = c3 - 1
            c4 = c4 - 1
            
            local n = c1 * 262144 + c2 * 4096 + c3 * 64 + c4
            local b1 = math.floor(n / 65536)
            local b2 = math.floor((n % 65536) / 256)
            local b3 = n % 256
            
            result = result .. string.char(b1)
            if i + 2 <= #data then
                result = result .. string.char(b2)
            end
            if i + 3 <= #data then
                result = result .. string.char(b3)
            end
        end
        
        return result
    else
        return nil  -- Unsupported format
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function decode__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
