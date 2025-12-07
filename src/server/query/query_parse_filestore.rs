use anyhow::{Result, bail};

use crate::server::query::Command;

pub fn parse_filestore(s: &str) -> Result<Command> {
    let up = s.trim().to_uppercase();
    // DDL -------------------------------------------------
    if up.starts_with("CREATE FILESTORE ") {
        // CREATE FILESTORE <name> [WITH <json>]
        let tail = s.trim()["CREATE FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        let mut rest = tail;
        let name_end = rest.find(' ').unwrap_or(rest.len());
        let fname = crate::ident::normalize_identifier(&rest[..name_end]);
        rest = rest.get(name_end..).unwrap_or("").trim();
        let uprest = rest.to_uppercase();
        let cfg_json = if uprest.starts_with("WITH ") {
            Some(rest[5..].trim().to_string())
        } else { None };
        return Ok(Command::CreateFilestoreCmd { filestore: fname, cfg_json });
    }
    if up.starts_with("ALTER FILESTORE ") {
        // ALTER FILESTORE <name> SET <json>
        let tail = s.trim()["ALTER FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        let name_end = tail.find(' ').unwrap_or(tail.len());
        let fname = crate::ident::normalize_identifier(&tail[..name_end]);
        let rest = tail.get(name_end..).unwrap_or("").trim();
        let uprest = rest.to_uppercase();
        if !uprest.starts_with("SET ") { bail!("ALTER FILESTORE requires SET <json>"); }
        let update_json = rest[4..].trim().to_string();
        return Ok(Command::AlterFilestoreCmd { filestore: fname, update_json });
    }
    if up.starts_with("DROP FILESTORE ") {
        // DROP FILESTORE <name> [FORCE]
        let tail = s.trim()["DROP FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        let mut rest = tail;
        let name_end = rest.find(' ').unwrap_or(rest.len());
        let fname = crate::ident::normalize_identifier(&rest[..name_end]);
        rest = rest.get(name_end..).unwrap_or("").trim();
        let force = rest.eq_ignore_ascii_case("FORCE");
        return Ok(Command::DropFilestoreCmd { filestore: fname, force });
    }
    // Mutations ------------------------------------------
    if up.starts_with("INGEST FILESTORE ") {
        // INGEST FILESTORE <name> FILE PATH '<logical>' FROM BYTES '<payload>' [CONTENT_TYPE '<ct>']
        // INGEST FILESTORE <name> FILE PATH '<logical>' FROM HOST_PATH '<host>' [CONTENT_TYPE '<ct>']
        let mut tail = s.trim()["INGEST FILESTORE ".len()..].trim().trim_end_matches(';').trim().to_string();
        // name
        let sp = tail.find(' ').unwrap_or(tail.len());
        let fs = crate::ident::normalize_identifier(&tail[..sp]);
        tail = tail[sp..].trim().to_string();
        let up2 = tail.to_uppercase();
        if !up2.starts_with("FILE PATH ") { bail!("INGEST FILESTORE: expected FILE PATH '<logical>'"); }
        let rest1 = tail[10..].trim();
        // parse quoted logical path
        let (logical, rest2) = parse_quoted_first(rest1)?;
        let up3 = rest2.to_uppercase();
        if !up3.starts_with("FROM ") { bail!("INGEST FILESTORE: expected FROM"); }
        let mode = rest2[5..].trim();
        let upm = mode.to_uppercase();
        if upm.starts_with("BYTES ") {
            let payload_str = mode[6..].trim();
            let (payload, rem) = parse_quoted_first(payload_str)?;
            let (content_type, _rest_after_ct) = parse_optional_kv_str(&rem, "CONTENT_TYPE")?;
            return Ok(Command::IngestFileFromBytesCmd { filestore: fs, logical_path: logical, payload, content_type });
        } else if upm.starts_with("HOST_PATH ") {
            let host_str = mode[10..].trim();
            let (host_path, rem) = parse_quoted_first(host_str)?;
            let (content_type, _rest_after_ct) = parse_optional_kv_str(&rem, "CONTENT_TYPE")?;
            return Ok(Command::IngestFileFromHostPathCmd { filestore: fs, logical_path: logical, host_path, content_type });
        } else { bail!("INGEST FILESTORE: expected BYTES or HOST_PATH"); }
    }
    if up.starts_with("UPDATE FILESTORE ") {
        // UPDATE FILESTORE <name> FILE PATH '<logical>' IF_MATCH '<etag>' FROM BYTES '<payload>' [CONTENT_TYPE '<ct>']
        let mut tail = s.trim()["UPDATE FILESTORE ".len()..].trim().trim_end_matches(';').trim().to_string();
        let sp = tail.find(' ').unwrap_or(tail.len());
        let fs = crate::ident::normalize_identifier(&tail[..sp]);
        tail = tail[sp..].trim().to_string();
        let up2 = tail.to_uppercase();
        if !up2.starts_with("FILE PATH ") { bail!("UPDATE FILESTORE: expected FILE PATH"); }
        let (logical, rest1) = parse_quoted_first(&tail[10..].trim())?;
        let up3 = rest1.to_uppercase();
        if !up3.starts_with("IF_MATCH ") { bail!("UPDATE FILESTORE: expected IF_MATCH '<etag>'"); }
        let (if_match, rest2) = parse_quoted_first(&rest1[9..].trim())?;
        let up4 = rest2.to_uppercase();
        if !up4.starts_with("FROM BYTES ") { bail!("UPDATE FILESTORE: expected FROM BYTES '<payload>'"); }
        let (payload, rem) = parse_quoted_first(&rest2[11..].trim())?;
        let (content_type, _rest_after_ct) = parse_optional_kv_str(&rem, "CONTENT_TYPE")?;
        return Ok(Command::UpdateFileFromBytesCmd { filestore: fs, logical_path: logical, if_match, payload, content_type });
    }
    if up.starts_with("RENAME FILESTORE ") {
        // RENAME FILESTORE <name> FROM PATH '<old>' TO PATH '<new>'
        let mut tail = s.trim()["RENAME FILESTORE ".len()..].trim().trim_end_matches(';').trim().to_string();
        let sp = tail.find(' ').unwrap_or(tail.len());
        let fs = crate::ident::normalize_identifier(&tail[..sp]);
        tail = tail[sp..].trim().to_string();
        let up2 = tail.to_uppercase();
        if !up2.starts_with("FROM PATH ") { bail!("RENAME FILESTORE: expected FROM PATH"); }
        let (from, rest1) = parse_quoted_first(&tail[10..].trim())?;
        let up3 = rest1.to_uppercase();
        if !up3.starts_with("TO PATH ") { bail!("RENAME FILESTORE: expected TO PATH"); }
        let (to, _) = parse_quoted_first(&rest1[8..].trim())?;
        return Ok(Command::RenameFilePathCmd { filestore: fs, from, to });
    }
    if up.starts_with("DELETE FILESTORE ") {
        // DELETE FILESTORE <name> FILE PATH '<logical>'
        let mut tail = s.trim()["DELETE FILESTORE ".len()..].trim().trim_end_matches(';').trim().to_string();
        let sp = tail.find(' ').unwrap_or(tail.len());
        let fs = crate::ident::normalize_identifier(&tail[..sp]);
        tail = tail[sp..].trim().to_string();
        let up2 = tail.to_uppercase();
        if !up2.starts_with("FILE PATH ") { bail!("DELETE FILESTORE: expected FILE PATH"); }
        let (logical, _) = parse_quoted_first(&tail[10..].trim())?;
        return Ok(Command::DeleteFilePathCmd { filestore: fs, logical_path: logical });
    }
    // Versioning -----------------------------------------
    if up.starts_with("CREATE TREE IN FILESTORE ") {
        // CREATE TREE IN FILESTORE <name> [LIKE '<prefix>']
        let mut tail = s.trim()["CREATE TREE IN FILESTORE ".len()..].trim().trim_end_matches(';').trim().to_string();
        let sp = tail.find(' ').unwrap_or(tail.len());
        let fs = crate::ident::normalize_identifier(&tail[..sp]);
        tail = tail[sp..].trim().to_string();
        let mut prefix: Option<String> = None;
        let up2 = tail.to_uppercase();
        if up2.starts_with("LIKE ") {
            let (p, _) = parse_quoted_first(&tail[5..].trim())?;
            prefix = Some(p);
        }
        return Ok(Command::CreateTreeCmd { filestore: fs, prefix });
    }
    if up.starts_with("COMMIT TREE IN FILESTORE ") {
        // COMMIT TREE IN FILESTORE <name> TREE '<tree_id>' [PARENTS '<id1,id2,...>'] [BRANCH '<branch>'] [AUTHOR_NAME '<name>'] [AUTHOR_EMAIL '<email>'] [MESSAGE '<msg>'] [TAGS '<t1,t2,...>']
        let mut tail = s.trim()["COMMIT TREE IN FILESTORE ".len()..].trim().trim_end_matches(';').trim().to_string();
        let sp = tail.find(' ').unwrap_or(tail.len());
        let fs = crate::ident::normalize_identifier(&tail[..sp]);
        tail = tail[sp..].trim().to_string();
        let up2 = tail.to_uppercase();
        if !up2.starts_with("TREE ") { bail!("COMMIT TREE: expected TREE '<tree_id>'"); }
        let (tree_id, mut rest) = parse_quoted_first(&tail[5..].trim())?;
        let mut parents: Vec<String> = Vec::new();
        let mut branch: Option<String> = None;
        let mut author_name: Option<String> = None;
        let mut author_email: Option<String> = None;
        let mut message: Option<String> = None;
        let mut tags: Vec<String> = Vec::new();
        loop {
            let upr = rest.to_uppercase();
            if upr.is_empty() { break; }
            if upr.starts_with("PARENTS ") {
                let (v, r2) = parse_quoted_first(&rest[8..].trim())?; rest = r2;
                parents = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                continue;
            }
            if upr.starts_with("BRANCH ") { let (v, r2) = parse_quoted_first(&rest[7..].trim())?; branch = Some(v); rest = r2; continue; }
            if upr.starts_with("AUTHOR_NAME ") { let (v, r2) = parse_quoted_first(&rest[12..].trim())?; author_name = Some(v); rest = r2; continue; }
            if upr.starts_with("AUTHOR_EMAIL ") { let (v, r2) = parse_quoted_first(&rest[13..].trim())?; author_email = Some(v); rest = r2; continue; }
            if upr.starts_with("MESSAGE ") { let (v, r2) = parse_quoted_first(&rest[8..].trim())?; message = Some(v); rest = r2; continue; }
            if upr.starts_with("TAGS ") { let (v, r2) = parse_quoted_first(&rest[5..].trim())?; tags = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(); rest = r2; continue; }
            break;
        }
        return Ok(Command::CommitTreeCmd { filestore: fs, tree_id, parents, branch, author_name, author_email, message, tags });
    }
    anyhow::bail!("Unsupported FILESTORE command")
}

// Helpers: parse a single quoted string (') from start of `s`; returns (value, rest)
fn parse_quoted_first(s: &str) -> Result<(String, String)> {
    let st = s.trim();
    if !st.starts_with('\'') { bail!("expected quoted string"); }
    if let Some(idx) = st[1..].find('\'') { let val = &st[1..1+idx]; let rest = st[1+idx+1..].trim().to_string(); return Ok((val.to_string(), rest)); }
    bail!("unterminated quoted string")
}

fn parse_optional_kv_str(s: &str, key: &str) -> Result<(Option<String>, String)> {
    let st = s.trim();
    let up = st.to_uppercase();
    let want = format!("{} ", key);
    if up.starts_with(&want) {
        let (v, rest) = parse_quoted_first(&st[want.len()..])?;
        Ok((Some(v), rest))
    } else {
        Ok((None, st.to_string()))
    }
}
