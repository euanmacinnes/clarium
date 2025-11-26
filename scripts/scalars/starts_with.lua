--[[
{ "kind": "scalar", "returns": ["boolean"], "nullable": true, "version": 1,
  "doc": "starts_with(str, prefix): returns true if str starts with prefix" }
]]

-- starts_with(str, prefix): checks if string starts with prefix
function starts_with(str, prefix)
    if str == nil or prefix == nil then
        return nil
    end
    str = tostring(str)
    prefix = tostring(prefix)
    
    -- Check if string starts with prefix
    return string.sub(str, 1, string.len(prefix)) == prefix
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function starts_with__meta()
    return { kind = "scalar", returns = { "boolean" }, nullable = true, version = 1 }
end
