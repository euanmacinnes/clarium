--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "upper(str): converts the string to uppercase" }
]]

-- upper(str): converts string to uppercase
function upper(str)
    if str == nil then
        return nil
    end
    return string.upper(tostring(str))
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function upper__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
