--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "quote_ident(str): quotes a string for use as an identifier in SQL statements" }
]]

-- quote_ident(str): quotes string as SQL identifier
function quote_ident(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- Escape double quotes by doubling them
    local escaped = str:gsub('"', '""')
    
    -- Wrap in double quotes
    return '"' .. escaped .. '"'
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function quote_ident__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
