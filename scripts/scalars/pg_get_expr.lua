--[[
{ "kind": "scalar", "returns": ["string"], "nullable": true, "version": 2,
  "doc": "pg_get_expr(expr_text, relation_oid[, pretty]): returns the text form of an expression (simplified passthrough implementation). The third argument is accepted for compatibility and ignored." }
]]

-- pg_get_expr(expr_text, relation_oid[, pretty]): returns expression text
-- In PostgreSQL, this function decompiles an internal expression tree back to SQL text.
-- For our simplified implementation, we just return the expr_text argument as-is.
function pg_get_expr(expr_text, relation_oid, pretty)
    if expr_text == nil then
        return nil
    end
    return tostring(expr_text)
end

-- Optional metadata function (used if sidecar/docstring parsing is disabled)
function pg_get_expr__meta()
    return { kind = "scalar", returns = { "string" }, nullable = true, version = 2 }
end
