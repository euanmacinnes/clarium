--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "concat(...): concatenates all arguments into a single string, skipping NULL values" }
]]

-- concat(...): concatenates all non-nil arguments
function concat(...)
    local args = {...}
    local result = {}
    for i = 1, #args do
        if args[i] ~= nil then
            table.insert(result, tostring(args[i]))
        end
    end
    return table.concat(result, "")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function concat__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
