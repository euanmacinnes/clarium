use anyhow::Result;
use crate::server::query::Command;

// Syntax: GC GRAPH [<qualified_graph_name>]
// When name is omitted, will use session default graph if set, else applies to all graphs.
pub fn parse_gc(s: &str) -> Result<Command> {
    let rest = s.trim();
    let up = rest.to_ascii_uppercase();
    if !up.starts_with("GC") { anyhow::bail!("Invalid GC command"); }
    let tail = rest[2..].trim();
    let up_tail = tail.to_ascii_uppercase();
    if up_tail.starts_with("GRAPH") {
        let name = tail[5..].trim();
        if name.is_empty() { return Ok(Command::GcGraph { name: None }); }
        // Allow trailing semicolon
        let name = name.trim_end_matches(';').trim();
        // Special keyword NONE is not used here; empty means default/all
        let normalized = crate::ident::normalize_identifier(name);
        return Ok(Command::GcGraph { name: Some(normalized) });
    }
    anyhow::bail!("Unsupported GC command; use: GC GRAPH [<db/schema/graph>]")
}
