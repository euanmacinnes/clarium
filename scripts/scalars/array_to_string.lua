--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "array_to_string(array, delimiter, null_string): joins array elements into a string using delimiter" }
]]

-- array_to_string(array, delimiter, null_string): joins array into string
function array_to_string(array, delimiter, null_string)
    if array == nil or delimiter == nil then
        return nil
    end
    
    -- Parse array representation (expecting comma-separated or Lua table)
    local elements = {}
    
    if type(array) == "table" then
        -- Direct Lua table
        elements = array
    else
        -- Parse comma-separated string representation
        local arr_str = tostring(array)
        for element in arr_str:gmatch("([^,]*)") do
            table.insert(elements, element)
        end
    end
    
    delimiter = tostring(delimiter)
    
    -- Build result, handling null_string
    local result = {}
    for i = 1, #elements do
        local elem = elements[i]
        if elem == nil or elem == "NULL" then
            if null_string ~= nil then
                table.insert(result, tostring(null_string))
            end
            -- Otherwise skip NULL values
        else
            table.insert(result, tostring(elem))
        end
    end
    
    return table.concat(result, delimiter)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function array_to_string__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
