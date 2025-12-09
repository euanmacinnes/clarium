--[[
{ "kind": "scalar", "returns": ["integer"], "nullable": true, "version": 1,
  "doc": "to_regtype(type_name): converts a type name to its OID, returns NULL if type doesn't exist" }
]]

-- to_regtype(type_name): convert a type name string to its PostgreSQL OID
function to_regtype(type_name)
    if type_name == nil then
        return nil
    end
    
    -- Convert to string and lowercase for case-insensitive matching
    local tn = tostring(type_name):lower()
    
    -- Map of PostgreSQL type names to OIDs
    -- These match standard PostgreSQL OIDs for compatibility
    local type_map = {
        -- Integer types
        ["int4"] = 23,
        ["integer"] = 23,
        ["int"] = 23,
        ["int8"] = 20,
        ["bigint"] = 20,
        ["int2"] = 21,
        ["smallint"] = 21,
        
        -- Floating point types
        ["float8"] = 701,
        ["double precision"] = 701,
        ["float4"] = 700,
        ["real"] = 700,
        
        -- Text/string types
        ["text"] = 25,
        ["varchar"] = 1043,
        ["character varying"] = 1043,
        ["char"] = 1042,
        ["character"] = 1042,
        
        -- Boolean type
        ["bool"] = 16,
        ["boolean"] = 16,
        
        -- Date/time types
        ["timestamp"] = 1114,
        ["timestamp without time zone"] = 1114,
        ["timestamptz"] = 1184,
        ["timestamp with time zone"] = 1184,
        ["date"] = 1082,
        ["time"] = 1083,
        ["timetz"] = 1266,
        
        -- Other common types
        ["bytea"] = 17,
        ["json"] = 114,
        ["jsonb"] = 3802,
        ["uuid"] = 2950
    }
    
    local oid = type_map[tn]
    return oid
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function to_regtype__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
