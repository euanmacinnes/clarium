--[[
{ "kind": "scalar", "returns": ["timestamp"], "nullable": false, "version": 1,
  "doc": "clock_timestamp(): returns the current date and time with maximum precision (changes during statement execution)" }
]]

-- clock_timestamp(): returns the current timestamp with high precision
function clock_timestamp()
    -- Returns Unix timestamp (Lua's os.time() has second precision)
    -- In a real implementation, this would use system calls for microsecond precision
    return os.time()
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function clock_timestamp__meta()
    return { kind = "scalar", returns = { "timestamp" }, nullable = false, version = 1 }
end
