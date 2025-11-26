--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "to_ascii(str): converts string to ASCII by removing accents and non-ASCII characters (simplified)" }
]]

-- to_ascii(str): converts to ASCII representation
function to_ascii(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- This is a simplified implementation
    -- True accent removal requires complex mapping tables
    
    -- Basic accent/diacritic removal map
    local accent_map = {
        ["á"] = "a", ["à"] = "a", ["â"] = "a", ["ä"] = "a", ["ã"] = "a", ["å"] = "a",
        ["é"] = "e", ["è"] = "e", ["ê"] = "e", ["ë"] = "e",
        ["í"] = "i", ["ì"] = "i", ["î"] = "i", ["ï"] = "i",
        ["ó"] = "o", ["ò"] = "o", ["ô"] = "o", ["ö"] = "o", ["õ"] = "o",
        ["ú"] = "u", ["ù"] = "u", ["û"] = "u", ["ü"] = "u",
        ["ñ"] = "n", ["ç"] = "c",
        ["Á"] = "A", ["À"] = "A", ["Â"] = "A", ["Ä"] = "A", ["Ã"] = "A", ["Å"] = "A",
        ["É"] = "E", ["È"] = "E", ["Ê"] = "E", ["Ë"] = "E",
        ["Í"] = "I", ["Ì"] = "I", ["Î"] = "I", ["Ï"] = "I",
        ["Ó"] = "O", ["Ò"] = "O", ["Ô"] = "O", ["Ö"] = "O", ["Õ"] = "O",
        ["Ú"] = "U", ["Ù"] = "U", ["Û"] = "U", ["Ü"] = "U",
        ["Ñ"] = "N", ["Ç"] = "C"
    }
    
    -- Replace accented characters
    local result = str:gsub(".", function(c)
        return accent_map[c] or c
    end)
    
    -- Remove non-ASCII characters (byte > 127)
    result = result:gsub("[^\1-\127]", "")
    
    return result
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function to_ascii__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
