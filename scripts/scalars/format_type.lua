--[[
{ "kind": "scalar", "returns": ["string"], "nullable": false, "version": 1,
  "doc": "format_type(typbasetype, typtypmod): maps common PostgreSQL type OIDs to human-readable names" }
]]

-- format_type(typbasetype, typtypmod): map a few OIDs to names
function format_type(typbasetype, typtypmod)
    -- Accept various inbound types for the OID (drivers may pass strings/floats/bools)
    local m = {
        [16] = 'boolean',
        [20] = 'bigint',
        [21] = 'smallint',
        [23] = 'integer',
        [25] = 'text',
        [700] = 'real',
        [701] = 'double precision',
        [1043] = 'character varying',
        [1082] = 'date',
        [1114] = 'timestamp',
        [1184] = 'timestamptz',
    }
    local tn = typbasetype
    local k = type(tn)
    if k == 'string' then
        local parsed = tonumber(tn)
        if parsed ~= nil then tn = parsed end
    elseif k == 'boolean' then
        tn = tn and 1 or 0
    elseif k == 'number' then
        -- Force to integer key if it's a float
        local toint = math.tointeger and math.tointeger(tn) or nil
        if toint ~= nil then
            tn = toint
        else
            tn = math.floor(tn)
        end
    elseif tn == nil then
        tn = nil
    end
    local t = m[tn]
    if t == nil then t = 'text' end
    return t
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function format_type__meta()
    return { kind = "scalar", returns = { "string" }, nullable = false, version = 1 }
end
