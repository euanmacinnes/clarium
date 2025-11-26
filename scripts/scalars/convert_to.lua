--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "convert_to(str, dest_encoding): converts string from database encoding to destination encoding (simplified)" }
]]

-- convert_to(str, dest_encoding): converts from database encoding to encoding
function convert_to(str, dest_encoding)
    if str == nil or dest_encoding == nil then
        return nil
    end
    str = tostring(str)
    dest_encoding = string.upper(tostring(dest_encoding))
    
    -- This is a simplified implementation
    -- Assume database encoding is UTF-8
    local db_encoding = "UTF8"
    
    -- If destination is UTF-8 compatible, return as-is
    local utf8_aliases = {"UTF8", "UTF-8", "UNICODE"}
    for _, alias in ipairs(utf8_aliases) do
        if dest_encoding == alias then
            return str
        end
    end
    
    -- For other encodings, return as-is (simplified)
    -- A full implementation would use iconv or similar
    return str
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function convert_to__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
