--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "strpos(string, substring): returns the position of substring within string (1-based index), or 0 if not found (alias for position)" }
]]

-- strpos(string, substring): finds position of substring (1-based)
-- This is an alias for position with argument order matching PostgreSQL strpos
function strpos(str, substring)
    if str == nil or substring == nil then
        return nil
    end
    str = tostring(str)
    substring = tostring(substring)
    
    -- Use plain find (not pattern matching)
    local pos = str:find(substring, 1, true)
    return pos or 0
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function strpos__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
