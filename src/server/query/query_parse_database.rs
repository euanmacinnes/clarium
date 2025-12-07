use crate::server::query::*;

pub fn parse_database(s: &str) -> Result<Command> {
    // DATABASE ADD <db> | DATABASE DELETE <db> | DATABASE DROP <db>
    let rest = s[9..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("ADD ") {
        let db = rest[4..].trim();
        if db.is_empty() { anyhow::bail!("Invalid DATABASE ADD: missing database"); }
        return Ok(Command::DatabaseAdd { database: db.to_string() });
    }
    if up.starts_with("DELETE ") || up.starts_with("DROP ") {
        let db = if up.starts_with("DELETE ") { &rest[7..] } else { &rest[5..] };
        let db = db.trim();
        if db.is_empty() { anyhow::bail!("Invalid DATABASE DELETE: missing database"); }
        return Ok(Command::DatabaseDelete { database: db.to_string() });
    }
    anyhow::bail!("Invalid DATABASE syntax")
}


pub fn parse_schema(s: &str) -> Result<Command> {
    // SCHEMA SHOW <db> | SCHEMA SHOW FROM <db>
    // SCHEMA ADD <name Type>[, <name Type> ...] (FROM|IN|TO) <db>
    let rest = s[6..].trim();
    let rest_up = rest.to_uppercase();
    if rest_up.starts_with("SHOW") {
        let after = rest[4..].trim();
        let mut db = after;
        if after.to_uppercase().starts_with("FROM ") || after.to_uppercase().starts_with("IN ") || after.to_uppercase().starts_with("TO ") {
            db = &after[5..];
        }
        let database = db.trim().to_string();
        if database.is_empty() { anyhow::bail!("Invalid SCHEMA SHOW: missing schema name"); }
        return Ok(Command::SchemaShow { database });
    } else if rest_up.starts_with("ADD ") {
        let after = &rest[4..];
        // Find position of FROM/IN/TO to split entries and database
        let up = after.to_uppercase();
        let mut split_pos: Option<(usize, usize)> = None; // (index, len)
        for kw in [" FROM ", " IN ", " TO "] {
            if let Some(i) = up.find(kw) { split_pos = Some((i, kw.len())); break; }
        }
        let (mut entries_part, db_part) = if let Some((i, l)) = split_pos { (&after[..i], &after[i+l..]) } else { anyhow::bail!("Invalid SCHEMA ADD: missing database (use FROM/IN/TO <db>)"); };
        let database = db_part.trim().to_string();
        if database.is_empty() { anyhow::bail!("Invalid SCHEMA ADD: missing database"); }
        // Extract optional PRIMARY KEY and PARTITION BY clauses from entries_part
        let mut primary_key: Option<Vec<String>> = None;
        let mut partitions: Option<Vec<String>> = None;
        let up_entries = entries_part.to_uppercase();
        // helper to parse list inside parentheses
        let parse_list = |s: &str| -> Vec<String> { s.split(',').map(|t| t.trim().to_string()).filter(|x| !x.is_empty()).collect() };
        // Find positions
        let mut cut_indices: Vec<(usize, usize)> = Vec::new();
        if let Some(i) = up_entries.find("PRIMARY KEY") {
            // find following '(' and ')'
            if let Some(p1) = entries_part[i..].find('(') { if let Some(p2) = entries_part[i+p1+1..].find(')') {
                let start = i + p1 + 1; let end = i + p1 + 1 + p2; let list = &entries_part[start..end];
                let cols = parse_list(list);
                if !cols.is_empty() { primary_key = Some(cols); }
                cut_indices.push((i, end+1));
            }}
        }
        let up_entries2 = entries_part.to_uppercase();
        if let Some(i) = up_entries2.find("PARTITION BY") {
            if let Some(p1) = entries_part[i..].find('(') { if let Some(p2) = entries_part[i+p1+1..].find(')') {
                let start = i + p1 + 1; let end = i + p1 + 1 + p2; let list = &entries_part[start..end];
                let cols = parse_list(list);
                if !cols.is_empty() { partitions = Some(cols); }
                cut_indices.push((i, end+1));
            }}
        }
        // Remove clauses from entries_part by slicing before the earliest clause
        if !cut_indices.is_empty() {
            cut_indices.sort_by_key(|x| x.0);
            let (start, _) = cut_indices[0];
            entries_part = &entries_part[..start];
        }
        // Parse entries: comma-separated pairs of name and type word
        let mut entries: Vec<(String, String)> = Vec::new();
        for chunk in entries_part.split(',') {
            let t = chunk.trim(); if t.is_empty() { continue; }
            let mut parts = t.split_whitespace();
            let name = parts.next().ok_or_else(|| anyhow::anyhow!("Invalid entry: missing name"))?.to_string();
            let ty = parts.next().ok_or_else(|| anyhow::anyhow!("Invalid entry: missing type for {}", name))?.to_string();
            entries.push((name, ty));
        }
        if entries.is_empty() && primary_key.is_none() && partitions.is_none() { anyhow::bail!("SCHEMA ADD: no entries or metadata provided"); }
        return Ok(Command::SchemaAdd { database, entries, primary_key, partitions });
    }
    anyhow::bail!("Invalid SCHEMA syntax")
}
