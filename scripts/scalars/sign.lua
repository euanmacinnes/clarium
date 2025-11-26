--[[
{ "kind": "scalar", "returns": ["number"], "nullable": true, "version": 1,
  "doc": "sign(x): returns -1 if x < 0, 0 if x = 0, 1 if x > 0" }
]]

-- sign(x): returns -1 if x < 0, 0 if x = 0, 1 if x > 0
function sign(x)
    if x == nil then return nil end
    if x < 0 then return -1 end
    if x > 0 then return 1 end
    return 0
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function sign__meta()
    return { kind = "scalar", returns = { "number" }, nullable = true, version = 1 }
end
