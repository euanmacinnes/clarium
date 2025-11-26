--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "aliases": ["truncate"],
  "doc": "trunc(x): truncates x toward zero, removing fractional part" }
]]

-- trunc(x): truncates x toward zero
function trunc(x)
    if x == nil then return nil end
    if x >= 0 then
        return math.floor(x)
    else
        return math.ceil(x)
    end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function trunc__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1, aliases = { "truncate" } }
end
