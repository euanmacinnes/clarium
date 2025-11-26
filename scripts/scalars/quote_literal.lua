--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "quote_literal(str): quotes a string for use as a literal in SQL statements" }
]]

-- quote_literal(str): quotes string as SQL literal
function quote_literal(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- Escape single quotes by doubling them
    local escaped = str:gsub("'", "''")
    
    -- Wrap in single quotes
    return "'" .. escaped .. "'"
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function quote_literal__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
