--[[
{ "kind": "scalar", "returns": ["date"], "nullable": false, "version": 1,
  "aliases": ["curdate"],
  "doc": "current_date(): returns the current date" }
]]

-- current_date(): returns the current date (YYYY-MM-DD format)
function current_date()
    return os.date("%Y-%m-%d")
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function current_date__meta()
    return { kind = "scalar", returns = { "date" }, nullable = false, version = 1, aliases = { "curdate" } }
end
