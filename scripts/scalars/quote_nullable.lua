--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "quote_nullable(str): quotes a string for use as a literal in SQL, or returns 'NULL' if the value is NULL" }
]]

-- quote_nullable(str): quotes string as SQL literal or returns 'NULL'
function quote_nullable(str)
    if str == nil then
        return "NULL"
    end
    str = tostring(str)
    
    -- Escape single quotes by doubling them
    local escaped = str:gsub("'", "''")
    
    -- Wrap in single quotes
    return "'" .. escaped .. "'"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function quote_nullable__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
