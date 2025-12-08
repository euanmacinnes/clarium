use anyhow::Result;

use crate::server::query::Command;

pub fn parse_vector_ddl(s: &str) -> Option<Result<Command>> {
    let up = s.trim().to_uppercase();
    if up.starts_with("ALTER VECTOR INDEX ") {
        // ALTER VECTOR INDEX <name> SET MODE = <mode>
        let tail = s.trim()["ALTER VECTOR INDEX ".len()..].trim();
        if tail.is_empty() { return Some(Err(anyhow::anyhow!("ALTER VECTOR INDEX: missing name"))); }
        let tail_up = tail.to_uppercase();
        let set_pos = tail_up.find(" SET ");
        if set_pos.is_none() { return Some(Err(anyhow::anyhow!("ALTER VECTOR INDEX: expected SET clause"))); }
        let set_pos = set_pos.unwrap();
        let name = tail[..set_pos].trim();
        let after = tail[set_pos + 5..].trim(); // past " SET "
        let after_up = after.to_uppercase();
        if !after_up.starts_with("MODE") { return Some(Err(anyhow::anyhow!("ALTER VECTOR INDEX: only SET MODE is supported"))); }
        // Accept forms:
        //  - MODE = <value>
        //  - MODE=<value>
        //  - MODE <value>
        let mut mode_val = String::new();
        // Strip leading MODE token
        let tail = after["MODE".len()..].trim();
        if tail.is_empty() {
            return Some(Err(anyhow::anyhow!("ALTER VECTOR INDEX: expected MODE <value>")));
        }
        let tv = if tail.starts_with('=') { tail[1..].trim() } else { tail };
        // Extract first non-whitespace token (optionally quoted)
        if tv.starts_with('\'') || tv.starts_with('"') {
            mode_val = tv.trim().trim_matches('\'').trim_matches('"').to_string();
        } else {
            // take up to first whitespace
            let parts: Vec<&str> = tv.split_whitespace().collect();
            if parts.is_empty() { return Some(Err(anyhow::anyhow!("ALTER VECTOR INDEX: expected MODE <value>"))); }
            mode_val = parts[0].to_string();
        }
        crate::tprintln!("[PARSE] ALTER VECTOR INDEX SET MODE parsed name='{}' mode='{}'", name, mode_val);
        if name.is_empty() { return Some(Err(anyhow::anyhow!("ALTER VECTOR INDEX: missing index name"))); }
        let normalized = crate::ident::normalize_identifier(name);
        return Some(Ok(Command::AlterVectorIndexSetMode { name: normalized, mode: mode_val }));
    }
    if up.starts_with("BUILD VECTOR INDEX ") {
        let tail = s.trim()["BUILD VECTOR INDEX ".len()..].trim();
        // Optional WITH (k=v,...)
        let mut name = tail.to_string();
        let mut options: Vec<(String, String)> = Vec::new();
        let tail_up = tail.to_uppercase();
        if let Some(pos) = tail_up.find(" WITH ") {
            name = tail[..pos].trim().to_string();
            let after = tail[pos+6..].trim();
            let after_up = after.to_uppercase();
            if !after_up.starts_with("(") { return Some(Err(anyhow::anyhow!("Invalid BUILD VECTOR INDEX: expected WITH (k=v,...)"))); }
            // parse inside parentheses
            let mut depth = 0i32; let mut buf = String::new();
            for ch in after.chars() {
                if ch == '(' { depth += 1; if depth == 1 { continue; } }
                if ch == ')' { depth -= 1; if depth == 0 { break; } }
                if depth >= 1 { buf.push(ch); }
            }
            for part in buf.split(',') {
                let p = part.trim(); if p.is_empty() { continue; }
                if let Some(eq) = p.find('=') {
                    let k = p[..eq].trim().to_string();
                    let v = p[eq+1..].trim().trim_matches('\'').trim_matches('"').to_string();
                    options.push((k, v));
                } else { return Some(Err(anyhow::anyhow!(format!("Invalid option '{}'; expected k=v", p)))); }
            }
        }
        if name.is_empty() { return Some(Err(anyhow::anyhow!("BUILD VECTOR INDEX: missing name"))); }
        let normalized = crate::ident::normalize_identifier(&name);
        return Some(Ok(Command::BuildVectorIndex { name: normalized, options }));
    }
    if up.starts_with("REINDEX VECTOR INDEX ") {
        let name = s.trim()["REINDEX VECTOR INDEX ".len()..].trim();
        if name.is_empty() { return Some(Err(anyhow::anyhow!("REINDEX VECTOR INDEX: missing name"))); }
        let normalized = crate::ident::normalize_identifier(name);
        return Some(Ok(Command::ReindexVectorIndex { name: normalized }));
    }
    if up.starts_with("SHOW VECTOR INDEX STATUS") {
        let tail = s.trim()["SHOW VECTOR INDEX STATUS".len()..].trim();
        if tail.is_empty() { return Some(Ok(Command::ShowVectorIndexStatus { name: None })); }
        let normalized = crate::ident::normalize_identifier(tail);
        return Some(Ok(Command::ShowVectorIndexStatus { name: Some(normalized) }));
    }
    None
}
