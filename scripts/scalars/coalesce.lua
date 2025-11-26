--[[
{ "kind": "scalar", "returns": ["any"], "nullable": true, "version": 1,
  "doc": "coalesce(...): returns the first non-NULL argument from the list" }
]]

-- coalesce(...): returns the first non-nil (non-NULL) argument
function coalesce(...)
    local args = {...}
    for i = 1, #args do
        if args[i] ~= nil then
            return args[i]
        end
    end
    return nil
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function coalesce__meta()
    return { kind = "scalar", returns = { "any" }, nullable = true, version = 1 }
end
