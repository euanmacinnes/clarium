--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "to_vec(x): normalize a vector literal into a stable comma-separated string of floats. Accepts formats like '[1, 2, 3]', '1,2,3', or any string with numbers separated by commas/space'." }
]]

local function trim(s)
    return (tostring(s):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function parse_numbers(s)
    s = trim(s)
    -- strip surrounding brackets if present
    if (#s >= 2) then
        local first = s:sub(1,1)
        local last = s:sub(-1)
        if (first == "[" and last == "]") or (first == "(" and last == ")") then
            s = s:sub(2, #s-1)
        end
    end
    -- Replace any non-number separators with comma to unify splitting
    -- Keep minus signs, decimal points, exponent markers
    -- We'll split on commas after converting whitespace to commas
    s = s:gsub("%s+", ",")
    -- Also allow semicolons or pipes as separators
    s = s:gsub(";", ","):gsub("|", ",")
    local out = {}
    for part in string.gmatch(s, "([^,]+)") do
        local p = trim(part)
        if #p > 0 then
            local num = tonumber(p)
            if num == nil then
                -- If an element cannot be parsed, abort by returning nil to signal error
                return nil
            end
            table.insert(out, num)
        end
    end
    if #out == 0 then return nil end
    return out
end

-- to_vec(x): returns a canonical comma-separated string of floats
function to_vec(x)
    if x == nil then return nil end
    local t = type(x)
    if t == "number" then
        -- Single number vector
        return tostring(x)
    end
    local arr
    if t == "string" then
        arr = parse_numbers(x)
    elseif t == "table" then
        -- Assume array-like table of numbers
        arr = {}
        for i = 1, #x do
            local v = x[i]
            if v == nil then return nil end
            local num = tonumber(v)
            if num == nil then return nil end
            table.insert(arr, num)
        end
    else
        return nil
    end
    if arr == nil then return nil end
    -- Canonicalize with minimal float formatting
    for i = 1, #arr do
        -- Normalize -0.0 to 0
        if arr[i] == 0 then arr[i] = 0.0 end
    end
    local strs = {}
    for i = 1, #arr do
        strs[i] = tostring(arr[i])
    end
    return table.concat(strs, ",")
end

function to_vec__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
