--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "length(str): returns the number of characters in the string" }
]]

-- length(str): returns the length of the string
function length(str)
    if str == nil then
        return nil
    end
    return string.len(tostring(str))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function length__meta()
    return { kind = "scalar", returns = { "integer" }, nullable = true, version = 1 }
end
