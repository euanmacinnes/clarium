--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "log(x): returns the base-10 logarithm of x; log(b, x): returns logarithm of x to base b" }
]]

-- log(b, x): returns logarithm of x to base b, or base-10 if only one arg
function log(b, x)
    if x == nil then
        -- Single argument: base-10 logarithm
        if b == nil then return nil end
        return math.log(b) / math.log(10)
    else
        -- Two arguments: logarithm to specified base
        if b == nil or x == nil then return nil end
        return math.log(x) / math.log(b)
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function log__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
