--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "rtrim(str): removes whitespace from the right (end) of the string" }
]]

-- rtrim(str): removes trailing whitespace
function rtrim(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    return str:match("^(.-)%s*$")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function rtrim__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
