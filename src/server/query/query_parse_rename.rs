use crate::server::query::query_common::*;
use crate::server::query::*;

pub fn parse_rename(s: &str) -> Result<Command> {
    // RENAME SCRIPT old TO new
    let rest = s[6..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("SCRIPT ") {
        let after = &rest[7..];
        let parts: Vec<&str> = after.splitn(2, " TO ").collect();
        if parts.len() != 2 { anyhow::bail!("Invalid RENAME SCRIPT syntax. Use: RENAME SCRIPT <old> TO <new>"); }
        let old = parts[0].trim();
        let newn = parts[1].trim();
        if old.is_empty() || newn.is_empty() { anyhow::bail!("Invalid RENAME SCRIPT: missing names"); }
        // Script names already normalized in scripts.rs, pass as-is
        return Ok(Command::RenameScript { from: old.to_string(), to: newn.to_string() });
    }
    // Otherwise existing rename handlers below
    // RENAME DATABASE <from> TO <to>
    // RENAME SCHEMA <db>/<from> TO <to>
    // RENAME TIME TABLE <db>/<schema>/<from>.time TO <db>/<schema>/<to>.time OR unqualified TO name (we will rebuild path at exec if needed)
    let rest = s[6..].trim();
    let up = rest.to_uppercase();
    let to_kw = " TO ";
    if up.starts_with("DATABASE ") {
        let arg = &rest[9..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME DATABASE syntax: expected RENAME DATABASE <from> TO <to>"); }
            let normalized_from = crate::ident::normalize_identifier(from);
            let normalized_to = crate::ident::normalize_identifier(to);
            return Ok(Command::RenameDatabase { from: normalized_from, to: normalized_to });
        } else { anyhow::bail!("Invalid RENAME DATABASE: missing TO <new_name>"); }
    }
    if up.starts_with("SCHEMA ") {
        let arg = &rest[7..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME SCHEMA syntax: expected RENAME SCHEMA <from> TO <to>"); }
            let normalized_from = crate::ident::normalize_identifier(from);
            let normalized_to = crate::ident::normalize_identifier(to);
            return Ok(Command::RenameSchema { from: normalized_from, to: normalized_to });
        } else { anyhow::bail!("Invalid RENAME SCHEMA: missing TO <new_name>"); }
    }
    if up.starts_with("TIME TABLE ") {
        let arg = &rest[11..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME TIME TABLE syntax: expected RENAME TIME TABLE <from> TO <to>"); }
            if !from.ends_with(".time") || !to.ends_with(".time") { anyhow::bail!("RENAME TIME TABLE requires .time suffix on both names"); }
            return Ok(Command::RenameTimeTable { from: from.to_string(), to: to.to_string() });
        } else { anyhow::bail!("Invalid RENAME TIME TABLE: missing TO <new_name>"); }
    }
    if up.starts_with("STORE ") {
        // RENAME STORE <db>.store.<from> TO <to>
        let arg = &rest[6..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let left = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if to.is_empty() { anyhow::bail!("Invalid RENAME STORE: missing destination name"); }
            let (db, from_store) = parse_store_addr(left)?;
            return Ok(Command::RenameStore { database: db, from: from_store, to: to.to_string() });
        } else { anyhow::bail!("Invalid RENAME STORE: missing TO <new_name>"); }
    }
    if up.starts_with("TABLE ") {
        let arg = &rest[6..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME TABLE syntax: expected RENAME TABLE <from> TO <to>"); }
            if from.ends_with(".time") || to.ends_with(".time") { anyhow::bail!("RENAME TABLE is for regular tables only; use RENAME TIME TABLE for .time tables"); }
            return Ok(Command::RenameTable { from: from.to_string(), to: to.to_string() });
        } else { anyhow::bail!("Invalid RENAME TABLE: missing TO <new_name>"); }
    }
    anyhow::bail!("Invalid RENAME syntax")
}