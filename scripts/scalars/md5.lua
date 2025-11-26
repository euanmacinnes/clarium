--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "md5(str): calculates the MD5 hash of the string (simplified implementation)" }
]]

-- md5(str): calculates MD5 hash
-- Note: This is a placeholder that returns a deterministic hash-like string
-- A full MD5 implementation would require external library or C binding
function md5(str)
    if str == nil then
        return nil
    end
    str = tostring(str)
    
    -- Simplified hash function (not cryptographically secure)
    -- In production, this should use a proper MD5 library
    local hash = 0
    for i = 1, #str do
        hash = (hash * 31 + string.byte(str, i)) % 4294967296
    end
    
    -- Format as hex string similar to MD5 output (32 chars)
    return string.format("%032x", hash)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function md5__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
