--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "lower(str): converts the string to lowercase" }
]]

-- lower(str): converts string to lowercase
function lower(str)
    if str == nil then
        return nil
    end
    return string.lower(tostring(str))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function lower__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
