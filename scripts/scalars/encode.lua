--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "encode(data, format): encodes data in the specified format (hex, escape, base64)" }
]]

-- encode(data, format): encodes data in specified format
function encode(data, format)
    if data == nil or format == nil then
        return nil
    end
    data = tostring(data)
    format = string.lower(tostring(format))
    
    if format == "hex" then
        local hex = ""
        for i = 1, #data do
            hex = hex .. string.format("%02x", string.byte(data, i))
        end
        return hex
    elseif format == "escape" then
        -- Escape special characters
        local escaped = data:gsub("([\\'])", "\\%1")
        return escaped
    elseif format == "base64" then
        -- Simplified base64 encoding
        local b64chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        local result = ""
        local padding = ""
        
        for i = 1, #data, 3 do
            local b1, b2, b3 = string.byte(data, i, i+2)
            b2 = b2 or 0
            b3 = b3 or 0
            
            local n = b1 * 65536 + b2 * 256 + b3
            local c1 = math.floor(n / 262144) + 1
            local c2 = math.floor((n % 262144) / 4096) + 1
            local c3 = math.floor((n % 4096) / 64) + 1
            local c4 = (n % 64) + 1
            
            result = result .. b64chars:sub(c1, c1) .. b64chars:sub(c2, c2)
            if i + 1 <= #data then
                result = result .. b64chars:sub(c3, c3)
            else
                result = result .. "="
            end
            if i + 2 <= #data then
                result = result .. b64chars:sub(c4, c4)
            else
                result = result .. "="
            end
        end
        
        return result
    else
        return nil  -- Unsupported format
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function encode__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
