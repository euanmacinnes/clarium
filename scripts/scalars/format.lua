--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "format(formatstr, ...): formats arguments according to a format string (simplified sprintf-like)" }
]]

-- format(formatstr, ...): formats arguments according to format string
function format(formatstr, ...)
    if formatstr == nil then
        return nil
    end
    formatstr = tostring(formatstr)
    
    local args = {...}
    local arg_idx = 1
    
    -- Replace %s with arguments, %% with %, %I with quoted identifier, %L with quoted literal
    local result = formatstr:gsub("%%(.)", function(char)
        if char == "%" then
            return "%"
        elseif char == "s" then
            -- String substitution
            if arg_idx <= #args then
                local val = args[arg_idx]
                arg_idx = arg_idx + 1
                return val ~= nil and tostring(val) or ""
            end
            return ""
        elseif char == "I" then
            -- Identifier (quote with double quotes)
            if arg_idx <= #args then
                local val = args[arg_idx]
                arg_idx = arg_idx + 1
                if val ~= nil then
                    local str = tostring(val)
                    local escaped = str:gsub('"', '""')
                    return '"' .. escaped .. '"'
                end
            end
            return '""'
        elseif char == "L" then
            -- Literal (quote with single quotes)
            if arg_idx <= #args then
                local val = args[arg_idx]
                arg_idx = arg_idx + 1
                if val ~= nil then
                    local str = tostring(val)
                    local escaped = str:gsub("'", "''")
                    return "'" .. escaped .. "'"
                else
                    return "NULL"
                end
            end
            return "NULL"
        else
            -- Unknown format, keep as-is
            return "%" .. char
        end
    end)
    
    return result
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function format__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
