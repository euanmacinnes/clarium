--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "to_number(text, format): converts string to a numeric value" }
]]

-- to_number(text, format): converts text to number
function to_number(text, format)
    if text == nil then
        return nil
    end
    
    text = tostring(text)
    
    -- Remove common formatting characters if format is provided
    if format ~= nil then
        -- Remove thousand separators (commas)
        text = text:gsub(",", "")
        -- Remove currency symbols
        text = text:gsub("[$€£¥]", "")
        -- Remove whitespace
        text = text:gsub("%s", "")
    end
    
    -- Try to convert to number
    local num = tonumber(text)
    return num
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function to_number__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
