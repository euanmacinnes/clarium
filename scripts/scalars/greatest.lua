--[[
{ "kind": "scalar", "returns": ["any"], "nullable": true, "version": 1,
  "doc": "greatest(...): returns the largest (maximum) value from the list of arguments" }
]]

-- greatest(...): returns the largest value from arguments
function greatest(...)
    local args = {...}
    if #args == 0 then
        return nil
    end
    
    local max_val = nil
    for i = 1, #args do
        if args[i] ~= nil then
            if max_val == nil or args[i] > max_val then
                max_val = args[i]
            end
        end
    end
    return max_val
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function greatest__meta()
    return { kind = "scalar", returns = { "any" }, nullable = true, version = 1 }
end
