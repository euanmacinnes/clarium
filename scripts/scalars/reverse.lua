--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "reverse(str): reverses the characters in the string" }
]]

-- reverse(str): reverses the string
function reverse(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    return string.reverse(str)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function reverse__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
