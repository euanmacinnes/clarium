--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 1,
  "doc": "nullif(a,b): returns NULL if a == b else returns a" }
]]

-- nullif(a, b): returns nil (NULL) if a == b else a
function nullif(a, b)
    if a == b then return nil else return a end
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function nullif__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 1 }
end
