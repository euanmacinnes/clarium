--[[
{ "kind": "scalar", "returns": ["timestamp"], "nullable": false, "version": 1,
  "doc": "statement_timestamp(): returns the start time of the current statement" }
]]

-- statement_timestamp(): returns the statement start time
function statement_timestamp()
    -- Call Rust-provided context accessor function
    -- Returns epoch seconds as a number directly
    local ts = get_context("statement_timestamp")
    if ts then
        return ts
    end
    return os.time()
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function statement_timestamp__meta()
    return { kind = "scalar", returns = { "timestamp" }, nullable = false, version = 1 }
end
