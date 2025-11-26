--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "ltrim(str): removes whitespace from the left (beginning) of the string" }
]]

-- ltrim(str): removes leading whitespace
function ltrim(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    return str:match("^%s*(.-)$")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function ltrim__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
