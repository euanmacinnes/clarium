use crate::server::query::*;



pub fn parse_show(s: &str) -> Result<Command> {
    let up = s.trim().to_uppercase();
    if up == "SHOW CURRENT GRAPH" { return Ok(Command::ShowCurrentGraph); }
    // SHOW GRAPH STATUS [<name>]
    if up.starts_with("SHOW GRAPH STATUS") {
        let tail = s.trim()["SHOW GRAPH STATUS".len()..].trim();
        if tail.is_empty() || tail == ";" {
            return Ok(Command::ShowGraphStatus { name: None });
        } else {
            let name = tail.trim_matches(';').trim();
            let normalized_name = crate::ident::normalize_identifier(name);
            return Ok(Command::ShowGraphStatus { name: Some(normalized_name) });
        }
    }
    if up == "SHOW TRANSACTION ISOLATION LEVEL" { return Ok(Command::ShowTransactionIsolation); }
    if up == "SHOW STANDARD_CONFORMING_STRINGS" { return Ok(Command::ShowStandardConformingStrings); }
    if up.starts_with("SHOW SERVER_VERSION") { return Ok(Command::ShowServerVersion); }
    if up == "SHOW CLIENT_ENCODING" { return Ok(Command::ShowClientEncoding); }
    if up == "SHOW SERVER_ENCODING" { return Ok(Command::ShowServerEncoding); }
    if up == "SHOW DATESTYLE" { return Ok(Command::ShowDateStyle); }
    if up == "SHOW INTEGER_DATETIMES" { return Ok(Command::ShowIntegerDateTimes); }
    if up == "SHOW TIME ZONE" || up == "SHOW TIMEZONE" { return Ok(Command::ShowTimeZone); }
    if up == "SHOW SEARCH_PATH" { return Ok(Command::ShowSearchPath); }
    if up == "SHOW DEFAULT_TRANSACTION_ISOLATION" { return Ok(Command::ShowDefaultTransactionIsolation); }
    if up == "SHOW TRANSACTION_READ_ONLY" { return Ok(Command::ShowTransactionReadOnly); }
    if up == "SHOW APPLICATION_NAME" { return Ok(Command::ShowApplicationName); }
    if up == "SHOW EXTRA_FLOAT_DIGITS" { return Ok(Command::ShowExtraFloatDigits); }
    if up == "SHOW ALL" { return Ok(Command::ShowAll); }
    if up.starts_with("SHOW SCHEMAS") || up.starts_with("SHOW SCHEMA") { return Ok(Command::ShowSchemas); }
    if up == "SHOW TABLES" { return Ok(Command::ShowTables); }
    if up == "SHOW OBJECTS" { return Ok(Command::ShowObjects); }
    if up == "SHOW SCRIPTS" { return Ok(Command::ShowScripts); }

    // ------------------------
    // FILESTORE SHOW commands
    // ------------------------
    if up.starts_with("SHOW FILESTORES") {
        // Optional: SHOW FILESTORES IN <database>
        let tail = s.trim()["SHOW FILESTORES".len()..].trim().trim_end_matches(';').trim();
        if tail.is_empty() { return Ok(Command::ShowFilestores { database: None }); }
        let mut db: Option<String> = None;
        let up_tail = tail.to_uppercase();
        if up_tail.starts_with("IN ") {
            let name = tail[3..].trim();
            db = Some(crate::ident::normalize_identifier(name));
        }
        return Ok(Command::ShowFilestores { database: db });
    }

    if up.starts_with("SHOW FILESTORE CONFIG ") {
        // SHOW FILESTORE CONFIG <name> [FOLDER <prefix>]
        let tail = s.trim()["SHOW FILESTORE CONFIG ".len()..].trim().trim_end_matches(';').trim();
        let mut parts = tail.splitn(2, ' ');
        let name = parts.next().unwrap_or("").trim();
        if name.is_empty() { anyhow::bail!("SHOW FILESTORE CONFIG: missing filestore name"); }
        let fs = crate::ident::normalize_identifier(name);
        let mut folder_prefix: Option<String> = None;
        if let Some(rest) = parts.next() {
            let upr = rest.to_uppercase();
            if upr.starts_with("FOLDER ") {
                let p = rest[7..].trim().trim_matches('\'').to_string();
                folder_prefix = Some(p);
            }
        }
        return Ok(Command::ShowFilestoreConfig { filestore: fs, folder_prefix });
    }

    if up.starts_with("SHOW FILES IN FILESTORE ") {
        // SHOW FILES IN FILESTORE <name> [LIKE '<prefix>'] [LIMIT n] [OFFSET k]
        let tail = s.trim()["SHOW FILES IN FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        // Extract filestore name (first token)
        let mut rest = tail;
        let tok_end = rest.find(' ').unwrap_or(rest.len());
        let fs = crate::ident::normalize_identifier(&rest[..tok_end]);
        rest = rest.get(tok_end..).unwrap_or("").trim();
        let mut prefix: Option<String> = None;
        let mut limit: Option<i64> = None;
        let mut offset: Option<i64> = None;
        // Simple linear option parsing
        let mut r = rest.to_string();
        let up_r = r.to_uppercase();
        if let Some(pos) = up_r.find("LIKE ") {
            let after = r[pos + 5..].trim();
            // Expect quoted or bare
            let val = after.trim().trim_matches('\'').to_string();
            prefix = if val.is_empty() { None } else { Some(val) };
            r = r[..pos].trim().to_string();
        }
        // LIMIT
        let mut upx = r.to_uppercase();
        if let Some(pos) = upx.find(" LIMIT ") {
            let after = r[pos + 7..].trim();
            if let Some(sp) = after.find(' ') { let num = &after[..sp]; limit = num.parse::<i64>().ok(); r = format!("{} {}", r[..pos].trim(), &after[sp..].trim()); }
            else { limit = after.parse::<i64>().ok(); r = r[..pos].trim().to_string(); }
            upx = r.to_uppercase();
        }
        // OFFSET
        if let Some(pos) = upx.find(" OFFSET ") {
            let after = r[pos + 8..].trim();
            if let Some(sp) = after.find(' ') { let num = &after[..sp]; offset = num.parse::<i64>().ok(); r = format!("{} {}", r[..pos].trim(), &after[sp..].trim()); }
            else { offset = after.parse::<i64>().ok(); r = r[..pos].trim().to_string(); }
        }
        return Ok(Command::ShowFilesInFilestore { filestore: fs, prefix, limit, offset });
    }

    if up.starts_with("SHOW TREES IN FILESTORE ") {
        let tail = s.trim()["SHOW TREES IN FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        if tail.is_empty() { anyhow::bail!("SHOW TREES IN FILESTORE: missing filestore name"); }
        let fs = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowTreesInFilestore { filestore: fs });
    }
    if up.starts_with("SHOW COMMITS IN FILESTORE ") {
        let tail = s.trim()["SHOW COMMITS IN FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        if tail.is_empty() { anyhow::bail!("SHOW COMMITS IN FILESTORE: missing filestore name"); }
        let fs = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowCommitsInFilestore { filestore: fs });
    }
    if up.starts_with("SHOW CHUNKS IN FILESTORE ") {
        let tail = s.trim()["SHOW CHUNKS IN FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        if tail.is_empty() { anyhow::bail!("SHOW CHUNKS IN FILESTORE: missing filestore name"); }
        let fs = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowChunksInFilestore { filestore: fs });
    }
    if up.starts_with("SHOW ALIASES IN FILESTORE ") {
        let tail = s.trim()["SHOW ALIASES IN FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        if tail.is_empty() { anyhow::bail!("SHOW ALIASES IN FILESTORE: missing filestore name"); }
        let fs = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowAliasesInFilestore { filestore: fs });
    }
    if up.starts_with("SHOW ADMIN IN FILESTORE ") {
        let tail = s.trim()["SHOW ADMIN IN FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        if tail.is_empty() { anyhow::bail!("SHOW ADMIN IN FILESTORE: missing filestore name"); }
        let fs = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowAdminInFilestore { filestore: fs });
    }
    if up.starts_with("SHOW HEALTH IN FILESTORE ") {
        let tail = s.trim()["SHOW HEALTH IN FILESTORE ".len()..].trim().trim_end_matches(';').trim();
        if tail.is_empty() { anyhow::bail!("SHOW HEALTH IN FILESTORE: missing filestore name"); }
        let fs = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowHealthInFilestore { filestore: fs });
    }

    if up.starts_with("SHOW DIFF IN FILESTORE ") {
        // SHOW DIFF IN FILESTORE <name> LEFT <tree_id> [RIGHT <tree_id> | LIVE LIKE '<prefix>']
        let mut tail = s.trim()["SHOW DIFF IN FILESTORE ".len()..].trim().trim_end_matches(';').trim().to_string();
        // filestore name is first token
        let sp = tail.find(' ').unwrap_or(tail.len());
        let fs = crate::ident::normalize_identifier(&tail[..sp]);
        tail = tail[sp..].trim().to_string();
        let up2 = tail.to_uppercase();
        if !up2.starts_with("LEFT ") { anyhow::bail!("SHOW DIFF: expected LEFT <tree_id>"); }
        let mut rest = tail[5..].trim().to_string();
        let left_tree_id = rest.split_whitespace().next().unwrap_or("").trim().trim_matches('\'').to_string();
        rest = rest[left_tree_id.len()..].trim().to_string();
        let mut right_tree_id: Option<String> = None;
        let mut live_prefix: Option<String> = None;
        let upr = rest.to_uppercase();
        if upr.starts_with("RIGHT ") {
            let id = rest[6..].trim().trim_matches('\'').to_string();
            right_tree_id = Some(id);
        } else if upr.starts_with("LIVE ") {
            let rem = rest[5..].trim().to_uppercase();
            if rem.starts_with("LIKE ") {
                let p = rest[5..].trim()["LIKE ".len()..].trim().trim_matches('\'').to_string();
                live_prefix = Some(p);
            }
        }
        return Ok(Command::ShowDiffInFilestore { filestore: fs, left_tree_id, right_tree_id, live_prefix });
    }
    if up.starts_with("SHOW VECTOR INDEXES") { return Ok(Command::ShowVectorIndexes); }
    if up.starts_with("SHOW VECTOR INDEX ") {
        let name = s.trim()["SHOW VECTOR INDEX ".len()..].trim();
        if name.is_empty() { anyhow::bail!("SHOW VECTOR INDEX: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::ShowVectorIndex { name: normalized_name });
    }
    if up.starts_with("SHOW GRAPH ") {
        let tail = s.trim()["SHOW GRAPH ".len()..].trim();
        if tail.eq_ignore_ascii_case("S") || tail.eq_ignore_ascii_case("S;") || tail.eq_ignore_ascii_case("S; ") { /* unlikely */ }
        // Accept SHOW GRAPHS and SHOW GRAPH <name>
        if tail.eq_ignore_ascii_case("S") || tail.eq_ignore_ascii_case("GRAPHS") { return Ok(Command::ShowGraphs); }
        if tail.eq_ignore_ascii_case("RAPHS") { return Ok(Command::ShowGraphs); }
        if tail.eq_ignore_ascii_case("GRAPHS;") { return Ok(Command::ShowGraphs); }
        let normalized_name = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowGraph { name: normalized_name });
    }
    if up.starts_with("SHOW GRAPHS") { return Ok(Command::ShowGraphs); }
    if up.starts_with("SHOW VIEW ") {
        let name = s.trim()["SHOW VIEW ".len()..].trim();
        if name.is_empty() { anyhow::bail!("SHOW VIEW: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::ShowView { name: normalized_name });
    }
    anyhow::bail!("Unsupported SHOW command")
}