--[[
{ "kind": "scalar", "returns": ["null"], "nullable": true, "version": 1,
  "doc": "setseed(seed): sets the seed for subsequent random() calls (seed must be between -1.0 and 1.0)" }
]]

-- setseed(seed): sets the seed for random number generation
function setseed(seed)
    if seed == nil then
        return nil
    end
    seed = tonumber(seed)
    
    -- PostgreSQL expects seed between -1.0 and 1.0
    -- Convert to Lua's math.randomseed range
    if seed < -1.0 or seed > 1.0 then
        return nil
    end
    
    -- Convert seed to integer for Lua (multiply by large number)
    local lua_seed = math.floor(seed * 2147483647)
    math.randomseed(lua_seed)
    
    return nil  -- setseed returns void (NULL)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function setseed__meta()
    return { kind = "scalar", returns = { "null" }, nullable = true, version = 1 }
end
