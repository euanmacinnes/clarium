--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "pg_typeof(value): returns the data type of any value as a string" }
]]

-- pg_typeof(value): returns the type of the value
function pg_typeof(value)
    local lua_type = type(value)
    
    if lua_type == "nil" then
        return "null"
    elseif lua_type == "boolean" then
        return "boolean"
    elseif lua_type == "number" then
        -- Try to determine if it's an integer or float
        if value == math.floor(value) then
            return "integer"
        else
            return "double precision"
        end
    elseif lua_type == "string" then
        -- Check if it looks like a date/time
        if value:match("^%d%d%d%d%-%d%d%-%d%d$") then
            return "date"
        elseif value:match("^%d%d%d%d%-%d%d%-%d%d %d%d:%d%d:%d%d") then
            return "timestamp"
        elseif value:match("^%d%d:%d%d:%d%d$") then
            return "time"
        else
            return "text"
        end
    elseif lua_type == "table" then
        return "array"
    else
        return "unknown"
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function pg_typeof__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
