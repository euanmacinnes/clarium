--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "position(substring, string): returns the position of substring within string (1-based index), or 0 if not found" }
]]

-- position(substring, string): finds position of substring (1-based)
function position(substring, str)
    if substring == nil or str == nil then
        return nil
    end
    substring = tostring(substring)
    str = tostring(str)
    -- Escape pattern characters in substring for literal search
    local escaped = substring:gsub("([%^%$%(%)%%%.%[%]%*%+%-%?])", "%%%1")
    local pos = str:find(escaped, 1, true)
    return pos or 0
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function position__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
