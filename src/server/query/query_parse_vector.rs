use anyhow::Result;

use crate::server::query::Command;

pub fn parse_vector_ddl(s: &str) -> Option<Result<Command>> {
    let up = s.trim().to_uppercase();
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
