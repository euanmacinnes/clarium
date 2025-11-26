--[[
{ "kind": "scalar", "returns": ["timestamp"], "nullable": false, "version": 1,
  "doc": "transaction_timestamp(): returns the start time of the current transaction (equivalent to now())" }
]]

-- transaction_timestamp(): returns the transaction start time
function transaction_timestamp()
    -- Call Rust-provided context accessor function
    -- Returns epoch seconds as a number directly
    local ts = get_context("transaction_timestamp")
    if ts then
        return ts
    end
    return os.time()
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function transaction_timestamp__meta()
    return { kind = "scalar", returns = { "timestamp" }, nullable = false, version = 1 }
end
