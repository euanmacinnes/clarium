--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "vec_ip(a,b): Inner product (dot product) between two vectors encoded as comma-separated numbers or bracketed lists" }
]]

local function trim(s)
    return (tostring(s):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function parse_vec(s)
    if s == nil then return nil end
    s = tostring(s)
    s = trim(s)
    if (#s >= 2) then
        local first = s:sub(1,1)
        local last = s:sub(-1)
        if (first == "[" and last == "]") or (first == "(" and last == ")") then
            s = s:sub(2, #s-1)
        end
    end
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

function vec_ip(a, b)
    if a == nil or b == nil then return nil end
    local va = parse_vec(a)
    local vb = parse_vec(b)
    if va == nil or vb == nil then return nil end
    local n = math.min(#va, #vb)
    local sum = 0.0
    for i = 1, n do sum = sum + va[i]*vb[i] end
    return sum
end

function vec_ip__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
