use anyhow::Result;
use regex::Regex;

use crate::server::query::Command;

// MATCH grammar (first iteration):
// MATCH [USING GRAPH <graph>] (s:Label { key: <start_expr> })-[:<etype>*L..U]->(t:Label)
// [WHERE <expr>] RETURN <proj_list> [ORDER BY <expr_list>] [LIMIT n]
// We rewrite it into a SELECT over TVF graph_neighbors with optional WHERE/ORDER/LIMIT.

pub fn parse_match(s: &str) -> Result<Command> {
    let text = s.trim();
    let upper = text.to_ascii_uppercase();
    if !upper.starts_with("MATCH") { anyhow::bail!("Invalid MATCH statement"); }
    let is_shortest = upper.starts_with("MATCH SHORTEST");
    // Extract optional USING GRAPH <graph>
    let using_re = Regex::new(r"(?i)USING\s+GRAPH\s+([^\s]+)").unwrap();
    let graph = using_re
        .captures(text)
        .and_then(|c| c.get(1).map(|m| m.as_str().trim().trim_matches(['"','\''])) )
        .map(|s| s.to_string());

    // Extract pattern core: (s:Label { key: <start> })-[:Type*L..U]->(t:Label [{ key: <dst> }])
    // This is deliberately permissive; we only need start key, optional dst key (required for SHORTEST), edge type, and hops upper bound.
    let pat_re = Regex::new(r"\(\s*s\s*:\s*([A-Za-z_][A-Za-z0-9_]*)[^\)]*?\{[^}]*key\s*:\s*([^}]+)\}[^\)]*\)\s*-\s*\[\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\*\s*([0-9]+)\s*(?:\.\.\s*([0-9]+))?\s*\]\s*->\s*\(\s*t\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\{[^}]*key\s*:\s*([^}]+)\}[^\)]*)?\)").unwrap();
    let caps = pat_re.captures(text).ok_or_else(|| anyhow::anyhow!("Unsupported MATCH pattern. Expect (s:Label {{ key: ... }})-[:Type*L..U]->(t:Label)"))?;
    let _s_label = caps.get(1).unwrap().as_str();
    let start_expr_raw = caps.get(2).unwrap().as_str().trim();
    let etype = caps.get(3).unwrap().as_str();
    let _l = caps.get(4).unwrap().as_str();
    let u = caps.get(5).map(|m| m.as_str()).unwrap_or(caps.get(4).unwrap().as_str());
    let _t_label = caps.get(6).unwrap().as_str();
    let dst_expr_raw = caps.get(7).map(|m| m.as_str().trim());

    // Extract WHERE/RETURN/ORDER/LIMIT segments (optional). We’ll keep their original text and do minimal identifier mapping.
    let where_part = extract_clause(text, "WHERE", &["RETURN", "ORDER BY", "LIMIT"]);
    let return_part = extract_clause(text, "RETURN", &["ORDER BY", "LIMIT"]).ok_or_else(|| anyhow::anyhow!("MATCH requires a RETURN clause"))?;
    let order_part = extract_clause(text, "ORDER BY", &["LIMIT"]);
    let limit_part = extract_clause(text, "LIMIT", &[]);

    // Map variables: t.key -> node_id; s.key -> literal of start_expr; prev.key -> prev_id
    let start_sql = normalize_start_expr(start_expr_raw);
    let mut proj_sql = return_part.to_string();
    proj_sql = proj_sql.replace("t.key", "node_id");
    proj_sql = proj_sql.replace("s.key", &start_sql);
    proj_sql = proj_sql.replace("prev.key", "prev_id");
    // WHERE and ORDER BY receive the same substitutions
    let where_sql = where_part.map(|w| w.replace("t.key", "node_id").replace("s.key", &start_sql).replace("prev.key", "prev_id"));
    let order_sql = order_part.map(|o| o.replace("t.key", "node_id").replace("s.key", &start_sql).replace("prev.key", "prev_id"));

    // Determine graph name: explicit USING GRAPH wins; else defer to session default at execution time
    let mut gf = graph.unwrap_or_else(|| "__SESSION_DEFAULT__".to_string());
    if gf == "__SESSION_DEFAULT__" {
        if let Some(sess) = crate::system::get_current_graph_opt() {
            gf = sess;
        }
    }
    let etype_sql = format!("'{}'", etype);
    let u_sql = u;

    let from_sql = if is_shortest {
        let dst_sql = normalize_start_expr(dst_expr_raw.ok_or_else(|| anyhow::anyhow!("MATCH SHORTEST requires target node key in t: {{ key: ... }}"))?);
        format!(
            "graph_paths({},{},{},{},{})",
            quote_graph_if_needed(&gf),
            start_sql,
            dst_sql,
            u_sql,
            etype_sql
        )
    } else {
        format!(
            "graph_neighbors({},{},{},{})",
            quote_graph_if_needed(&gf),
            start_sql,
            etype_sql,
            u_sql
        )
    };
    // Use TVF directly in FROM, aligned with existing parser/tests: FROM graph_neighbors(...) g
    let mut select_sql = format!("SELECT {} FROM {} g", proj_sql, from_sql);
    if let Some(ws) = where_sql { select_sql.push_str(" WHERE "); select_sql.push_str(ws.trim()); }
    if let Some(os) = order_sql { select_sql.push_str(" ORDER BY "); select_sql.push_str(os.trim()); }
    if let Some(ls) = limit_part { select_sql.push_str(" LIMIT "); select_sql.push_str(ls.trim()); }

    Ok(Command::MatchRewrite { sql: select_sql })
}

fn extract_clause<'a>(text: &'a str, kw: &str, stops: &[&str]) -> Option<&'a str> {
    let up = text.to_ascii_uppercase();
    let kwu = kw.to_ascii_uppercase();
    let pos = up.find(&kwu)?;
    let after = &text[pos + kw.len()..];
    // find earliest stop
    let mut end = after.len();
    for s in stops { if let Some(i) = after.to_ascii_uppercase().find(&s.to_ascii_uppercase()) { end = end.min(i); } }
    Some(after[..end].trim())
}

fn normalize_start_expr(expr: &str) -> String {
    let t = expr.trim();
    // If it already looks like a quoted string or number, return as-is; else quote as string literal
    if t.starts_with('\'') && t.ends_with('\'') { return t.to_string(); }
    if t.starts_with('"') && t.ends_with('"') { return format!("'{}'", t[1..t.len()-1].replace('\'', "''")); }
    if t.chars().all(|c| c.is_ascii_digit()) { return t.to_string(); }
    format!("'{}'", t.replace('\'', "''"))
}

fn quote_graph_if_needed(name: &str) -> String {
    if name == "__SESSION_DEFAULT__" { return name.to_string(); }
    // Ensure it's a single-quoted string literal for the TVF argument
    if name.starts_with('\'') && name.ends_with('\'') { name.to_string() } else { format!("'{}'", name.replace('\'', "''")) }
}
