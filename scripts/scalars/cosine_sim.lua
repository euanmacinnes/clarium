--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "cosine_sim(a,b): cosine similarity between two vectors encoded as comma-separated numbers (use to_vec(...) to normalize)" }
]]

local function trim(s)
    return (tostring(s):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function parse_vec(s)
    if s == nil then return nil end
    local t = type(s)
    if t == "table" then
        -- Accept Lua array/table directly
        local arr = {}
        for i = 1, #s do
            local v = s[i]
            local num = tonumber(v)
            if num == nil then return nil end
            arr[#arr+1] = num
        end
        if #arr == 0 then return nil end
        return arr
    end
    if t == "number" then
        return { s }
    end
    s = tostring(s)
    s = trim(s)
    -- Allow bracketed lists too
    if (#s >= 2) then
        local first = s:sub(1,1)
        local last = s:sub(-1)
        if (first == "[" and last == "]") or (first == "(" and last == ")") then
            s = s:sub(2, #s-1)
        end
    end
    -- Replace whitespace with commas to support loose input
    s = s:gsub("%s+", ",")
    local arr = {}
    for part in string.gmatch(s, "([^,]+)") do
        local p = trim(part)
        if #p > 0 then
            local num = tonumber(p)
            if num == nil then return nil end
            arr[#arr+1] = num
        end
    end
    if #arr == 0 then return nil end
    return arr
end

local function dot(a,b)
    local n = math.min(#a,#b)
    local sum = 0.0
    for i = 1, n do sum = sum + a[i]*b[i] end
    return sum
end

local function norm(a)
    local s = 0.0
    for i = 1, #a do s = s + a[i]*a[i] end
    return math.sqrt(s)
end

function cosine_sim(a, b)
    if a == nil or b == nil then return nil end
    local va = parse_vec(a)
    local vb = parse_vec(b)
    if va == nil or vb == nil then return nil end
    local na = norm(va)
    local nb = norm(vb)
    if na == 0 or nb == 0 then return nil end
    return dot(va,vb) / (na*nb)
end

function cosine_sim__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
