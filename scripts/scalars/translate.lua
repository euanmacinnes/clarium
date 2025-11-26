--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "translate(str, from, to): replaces each character in str that matches a character in from with the corresponding character in to" }
]]

-- translate(str, from, to): character-by-character substitution
function translate(str, from, to)
    if str == nil or from == nil or to == nil then
        return nil
    end
    str = tostring(str)
    from = tostring(from)
    to = tostring(to)
    
    -- Build translation table
    local trans_table = {}
    for i = 1, #from do
        local from_char = from:sub(i, i)
        local to_char = to:sub(i, i) or ""
        trans_table[from_char] = to_char
    end
    
    -- Apply translation
    local result = {}
    for i = 1, #str do
        local char = str:sub(i, i)
        local translated = trans_table[char]
        if translated ~= nil then
            if translated ~= "" then
                table.insert(result, translated)
            end
            -- If translated is "", the character is removed
        else
            table.insert(result, char)
        end
    end
    
    return table.concat(result, "")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function translate__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
