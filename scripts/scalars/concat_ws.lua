--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "concat_ws(separator, ...): concatenates all arguments with the separator, skipping NULL values" }
]]

-- concat_ws(separator, ...): concatenates arguments with separator
function concat_ws(separator, ...)
    if separator == nil then
        return nil
    end
    separator = tostring(separator)
    
    local args = {...}
    local result = {}
    for i = 1, #args do
        if args[i] ~= nil then
            table.insert(result, tostring(args[i]))
        end
    end
    
    if #result == 0 then
        return ""
    end
    
    return table.concat(result, separator)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function concat_ws__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
