use anyhow::{Result, anyhow};

use crate::server::query::Command;

// Simple parser for transactional GraphStore DDL:
// BEGIN [GRAPH <name>]
// COMMIT
// ABORT
// INSERT NODE <Label> KEY <'key'> [AS <node_id>] [GRAPH <name>]
// INSERT EDGE <src_id> -> <dst_id> [ETYPE <etype_id>] [PART <n>] [GRAPH <name>]

pub fn parse_txn(s: &str) -> Result<Command> {
    let t = s.trim();
    let up = t.to_ascii_uppercase();
    if up == "BEGIN" { return Ok(Command::BeginGraphTxn { graph: None }); }
    if up.starts_with("BEGIN ") {
        // BEGIN GRAPH <name>
        let rest = t[6..].trim();
        let rup = rest.to_ascii_uppercase();
        if rup.starts_with("GRAPH ") {
            let name = rest[6..].trim();
            if name.is_empty() { return Err(anyhow!("BEGIN GRAPH: missing graph name")); }
            return Ok(Command::BeginGraphTxn { graph: Some(name.to_string()) });
        }
        return Err(anyhow!("Unsupported BEGIN syntax; use BEGIN or BEGIN GRAPH <name>"));
    }
    if up == "COMMIT" { return Ok(Command::CommitGraphTxn); }
    if up == "ABORT" || up == "ROLLBACK" { return Ok(Command::AbortGraphTxn); }

    if up.starts_with("INSERT NODE") {
        // INSERT NODE <Label> KEY <'key'> [AS <node_id>] [GRAPH <name>]
        let rest = t[11..].trim(); // after INSERT NODE
        // Label is first token until space
        let mut cur = rest;
        let (label, r1) = take_ident(cur)?; cur = r1.trim_start();
        let up2 = cur.to_ascii_uppercase();
        if !up2.starts_with("KEY ") { return Err(anyhow!("INSERT NODE: expected KEY <literal>")); }
        cur = cur[4..].trim_start();
        let (key_lit, r2) = take_literal(cur)?; cur = r2.trim_start();
        let mut node_id: Option<u64> = None;
        let mut graph: Option<String> = None;
        loop {
            if cur.is_empty() { break; }
            let up3 = cur.to_ascii_uppercase();
            if up3.starts_with("AS ") {
                cur = cur[3..].trim_start();
                let (nid, r3) = take_u64(cur)?; cur = r3.trim_start();
                node_id = Some(nid);
                continue;
            }
            if up3.starts_with("GRAPH ") {
                cur = cur[6..].trim_start();
                let (gname, r4) = take_ident_or_quoted(cur)?; cur = r4.trim_start();
                graph = Some(gname);
                continue;
            }
            break;
        }
        return Ok(Command::InsertNodeTxn { graph, label, key: key_lit, node_id });
    }

    if up.starts_with("INSERT EDGE") {
        // INSERT EDGE <src_id> -> <dst_id> [ETYPE <etype_id>] [PART <n>] [GRAPH <name>]
        let rest = t[11..].trim(); // after INSERT EDGE
        let mut cur = rest;
        let (src, r1) = take_u64(cur)?; cur = r1.trim_start();
        if !cur.starts_with("->") && !cur.starts_with("-> ") && !cur.starts_with(" ->") {
            return Err(anyhow!("INSERT EDGE: expected '->' between src and dst"));
        }
        cur = cur.trim_start_matches(' ').trim_start_matches('-').trim_start_matches('>').trim_start();
        let (dst, r2) = take_u64(cur)?; cur = r2.trim_start();
        let mut etype_id: Option<u16> = None;
        let mut part: Option<u32> = None;
        let mut graph: Option<String> = None;
        loop {
            if cur.is_empty() { break; }
            let up3 = cur.to_ascii_uppercase();
            if up3.starts_with("ETYPE ") {
                cur = cur[6..].trim_start();
                let (eid_u64, r3) = take_u64(cur)?; cur = r3.trim_start();
                etype_id = Some(eid_u64 as u16);
                continue;
            }
            if up3.starts_with("PART ") {
                cur = cur[5..].trim_start();
                let (p, r4) = take_u64(cur)?; cur = r4.trim_start();
                part = Some(p as u32);
                continue;
            }
            if up3.starts_with("GRAPH ") {
                cur = cur[6..].trim_start();
                let (gname, r5) = take_ident_or_quoted(cur)?; cur = r5.trim_start();
                graph = Some(gname);
                continue;
            }
            break;
        }
        return Ok(Command::InsertEdgeTxn { graph, src, dst, etype_id, part });
    }

    Err(anyhow!("Unsupported transactional statement"))
}

fn take_ident(input: &str) -> Result<(String, &str)> {
    let mut it = input.char_indices();
    let mut end = 0usize;
    for (i, c) in it.by_ref() {
        if i == 0 {
            if !(c.is_ascii_alphabetic() || c == '_' || c == '"') { break; }
            end = i + c.len_utf8();
            continue;
        }
        if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '/' { end = i + c.len_utf8(); } else { break; }
    }
    if end == 0 { return Err(anyhow!("expected identifier")); }
    Ok((input[..end].trim_matches('"').to_string(), &input[end..]))
}

fn take_ident_or_quoted(input: &str) -> Result<(String, &str)> {
    let t = input.trim_start();
    if t.starts_with('\'') {
        let mut i = 1usize;
        while i < t.len() && !t.as_bytes()[i..=i].eq(b"'") { i += 1; }
        if i >= t.len() { return Err(anyhow!("unterminated string")); }
        let val = t[1..i].to_string();
        return Ok((val, &t[i+1..]));
    }
    take_ident(t)
}

fn take_literal(input: &str) -> Result<(String, &str)> {
    let t = input.trim_start();
    if t.starts_with('\'') {
        let mut i = 1usize;
        while i < t.len() && !t.as_bytes()[i..=i].eq(b"'") { i += 1; }
        if i >= t.len() { return Err(anyhow!("unterminated string literal")); }
        let val = t[1..i].to_string();
        return Ok((val, &t[i+1..]));
    }
    // allow bareword number or token
    let (id, rest) = take_ident(t)?;
    Ok((id, rest))
}

fn take_u64(input: &str) -> Result<(u64, &str)> {
    let t = input.trim_start();
    let mut end = 0usize;
    for (i, c) in t.char_indices() {
        if c.is_ascii_digit() { end = i + 1; } else { break; }
    }
    if end == 0 { return Err(anyhow!("expected integer")); }
    let n: u64 = t[..end].parse().map_err(|_| anyhow!("invalid integer"))?;
    Ok((n, &t[end..]))
}
