--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "trim(str): removes whitespace from both ends of the string" }
]]

-- trim(str): removes leading and trailing whitespace
function trim(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    return str:match("^%s*(.-)%s*$")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function trim__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
