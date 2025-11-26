--[[
{ "kind": "scalar", "returns": ["any"], "nullable": true, "version": 1,
  "doc": "least(...): returns the smallest (minimum) value from the list of arguments" }
]]

-- least(...): returns the smallest value from arguments
function least(...)
    local args = {...}
    if #args == 0 then
        return nil
    end
    
    local min_val = nil
    for i = 1, #args do
        if args[i] ~= nil then
            if min_val == nil or args[i] < min_val then
                min_val = args[i]
            end
        end
    end
    return min_val
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function least__meta()
    return { kind = "scalar", returns = { "any" }, nullable = true, version = 1 }
end
