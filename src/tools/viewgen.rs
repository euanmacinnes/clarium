use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::fs;

fn find_as_token(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        let c0 = bytes[i] as char;
        let before_ok = if i == 0 { true } else { (bytes[i - 1] as char).is_whitespace() };
        if (c0 == 'A' || c0 == 'a') && before_ok {
            let c1 = bytes[i + 1] as char;
            if c1 == 'S' || c1 == 's' {
                let after_ok = if i + 2 >= bytes.len() { true } else { (bytes[i + 2] as char).is_whitespace() };
                if after_ok { return Some(i); }
            }
        }
        i += 1;
    }
    None
}

#[derive(Debug, Clone)]
pub struct GenOptions {
    /// Root folder to scan. We now scan this folder (recursively) for any
    /// directory that contains a file named `original_schema_views.md` and
    /// generate all `.view` files into that same directory.
    pub out_dir: PathBuf,
    pub overwrite: bool,
    pub dry_run: bool,
}

/// Generate system .view JSON files from markdown reference under `plans/postgres system objects`.
/// Heuristic parser: finds CREATE VIEW statements or bare SELECT blocks with preceding name hints.
/// - When CREATE VIEW schema.name AS SELECT ... is found → use schema/name directly.
/// - When bare SELECT is found and a preceding line contains `View:` or `Name:` with schema.name → use it.
/// Column schema extraction is best-effort by parsing a simple comma-separated projection list.
pub fn generate_system_views(opts: &GenOptions) -> Result<usize> {
    // New behavior:
    // - Scan opts.out_dir recursively for any folder containing a file named
    //   "original_schema_views.md".
    // - Parse that file, extracting all CREATE [OR REPLACE] VIEW ... AS SELECT ...
    //   statements, and generate one .view JSON per view into the SAME folder.
    // - Only process views under schemas pg_catalog or information_schema.

    if !opts.out_dir.exists() {
        return Err(anyhow!(format!("out dir not found: {}", opts.out_dir.display())));
    }
    let _ = fs::create_dir_all(&opts.out_dir);
    let mut total_written = 0usize;

    let mut md_targets: Vec<PathBuf> = Vec::new();
    collect_original_schema_md(&opts.out_dir, &mut md_targets);

    for md in md_targets.into_iter() {
        let parent_dir = md.parent().unwrap_or(&opts.out_dir).to_path_buf();
        let text = match fs::read_to_string(&md) { Ok(s) => s, Err(_) => continue };

        // Prefer AST-based parser from our query engine first, then fall back to heuristics.
        let default_schema = derive_default_schema(&md);
        let mut items = parse_views_via_ast(&text, default_schema.as_deref());
        if items.is_empty() {
            items = parse_views_from_plain_sql(&text, default_schema.as_deref());
        }
        if items.is_empty() {
            // fallback to fenced code blocks with legacy heuristics
            let lines: Vec<&str> = text.lines().collect();
            let mut i = 0usize;
            while i < lines.len() {
                let line = lines[i].trim();
                if line.starts_with("```") {
                    let mut j = i + 1;
                    let mut sql_lines: Vec<String> = Vec::new();
                    while j < lines.len() && !lines[j].starts_with("```") { sql_lines.push(lines[j].to_string()); j += 1; }
                    let sql_raw = sql_lines.join("\n");
                    if let Some((schema, name, select_sql)) = extract_view_name_and_sql(&sql_raw, &lines, i, default_schema.as_deref()) {
                        items.push((schema, name, select_sql));
                    }
                    i = j + 1; continue;
                }
                i += 1;
            }
        }

        for (schema, name, select_sql) in items.into_iter() {
            // Only generate for supported schemas
            if !(schema.eq_ignore_ascii_case("pg_catalog") || schema.eq_ignore_ascii_case("information_schema")) {
                continue;
            }
            // Best-effort columns extraction
            let cols = extract_columns_from_select(&select_sql);
            let json = serde_json::json!({
                "schema": schema,
                "name": name,
                "columns": cols.iter().map(|c| serde_json::json!({
                    "name": c.clone(),
                    "data_type": "text",
                    "udt_name": "text"
                })).collect::<Vec<_>>(),
                // Ensure we do NOT include the CREATE/ALTER header; only the SELECT
                "definition_sql": select_sql,
            });
            let fname = format!("{}.view", sanitize_filename(&name));
            let out_path = parent_dir.join(fname);
            if out_path.exists() && !opts.overwrite { continue; }
            if !opts.dry_run {
                if let Err(e) = fs::write(&out_path, serde_json::to_string_pretty(&json)?) {
                    eprintln!("[viewgen] failed to write {}: {}", out_path.display(), e);
                } else {
                    total_written += 1;
                }
            }
        }
    }

    Ok(total_written)
}

fn collect_md_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() { return; }
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                collect_md_files(&p, out);
            } else if p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("md")).unwrap_or(false) {
                out.push(p);
            }
        }
    }
}

fn collect_original_schema_md(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() { return; }
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                collect_original_schema_md(&p, out);
            } else if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name.eq_ignore_ascii_case("original_schema_views.md") {
                    out.push(p);
                }
            }
        }
    }
}

fn extract_view_name_and_sql(sql_raw: &str, lines: &Vec<&str>, start_idx: usize, default_schema: Option<&str>) -> Option<(String, String, String)> {
    // Normalize whitespace a bit
    let sql_trim = sql_raw.trim();
    // Try pattern: CREATE VIEW schema.name AS SELECT ... (with optional OR REPLACE / IF NOT EXISTS)
    let up = sql_raw.to_ascii_uppercase();
    if up.starts_with("CREATE ") && up.contains(" VIEW ") {
        // remove optional OR REPLACE / IF NOT EXISTS
        // Find the segment after the keyword VIEW
        if let Some(pos_view) = up.find(" VIEW ") {
            let rest = &sql_trim[pos_view + " VIEW ".len()..];
            // tolerate AS across newlines/whitespace
            let pos_as = find_as_token(rest);
            if let Some(as_pos) = pos_as {
                let ident = rest[..as_pos].trim().trim_matches('"');
                let (schema, name) = split_ident(ident).or_else(|| default_schema.map(|sch| (sch, ident)))?;
                // advance past AS + whitespace
                let mut k = as_pos + 2;
                while k < rest.len() && rest.as_bytes()[k].is_ascii_whitespace() { k += 1; }
                let select_sql = rest[k..].trim().to_string();
                return Some((schema.to_string(), name.to_string(), select_sql));
            }
        }
    }
    // Try to find a hint line above the code block: View: schema.name or Name: schema.name
    let mut k = start_idx;
    while k > 0 { k -= 1; let l = lines[k].trim(); if l.is_empty() { continue; } if l.starts_with("View:") || l.starts_with("Name:") {
            if let Some((_, rhs)) = l.split_once(':') {
                let ident = rhs.trim().trim_matches('`').trim_matches('"');
                if let Some((schema, name)) = split_ident(ident) {
                    return Some((schema.to_string(), name.to_string(), sql_trim.to_string()));
                } else if let Some(sch) = default_schema { return Some((sch.to_string(), ident.to_string(), sql_trim.to_string())); }
            }
        }
        // Stop at heading or separator
        if l.starts_with('#') || l.starts_with("----") { break; }
    }
    // Try heading-based inference like "### view_name" or backticked identifiers
    let mut k2 = start_idx;
    while k2 > 0 { k2 -= 1; let l = lines[k2].trim(); if l.is_empty() { continue; }
        if l.starts_with('#') {
            // Capture the first token after hashes, stripping code/backticks
            let name_token = l.trim_start_matches('#').trim().trim_matches('`').trim_matches('"');
            if !name_token.is_empty() {
                if let Some((schema, name)) = split_ident(name_token) {
                    return Some((schema.to_string(), name.to_string(), sql_trim.to_string()));
                } else if let Some(sch) = default_schema { return Some((sch.to_string(), name_token.to_string(), sql_trim.to_string())); }
            }
            break;
        }
        // Bullet list with backticked identifier
        if l.starts_with('-') || l.starts_with('*') {
            let v = l.trim_start_matches(|c| c=='-' || c=='*').trim();
            let v = v.trim_matches('`').trim_matches('"');
            if let Some((schema, name)) = split_ident(v) {
                return Some((schema.to_string(), name.to_string(), sql_trim.to_string()));
            }
        }
    }
    // If the block is a bare SELECT and we have a default schema and can infer a simple name from the first table, derive a name
    if sql_trim.to_ascii_uppercase().starts_with("SELECT ") {
        if let Some(sch) = default_schema {
            // crude: use first word after FROM as name if simple identifier
            let up = sql_trim.to_ascii_uppercase();
            if let Some(pos_from) = up.find(" FROM ") {
                let after = &sql_trim[pos_from + 6..].trim();
                let ident = after.split_whitespace().next().unwrap_or("");
                let ident_clean = ident.trim_matches('"').trim_matches('`');
                if !ident_clean.is_empty() {
                    // if it's qualified a.b, use b as name
                    let name = ident_clean.rsplit('.').next().unwrap_or(ident_clean);
                    return Some((sch.to_string(), name.to_string(), sql_trim.to_string()));
                }
            }
        }
    }
    None
}

fn split_ident(ident: &str) -> Option<(&str, &str)> {
    if let Some((schema, name)) = ident.split_once('.') { Some((schema.trim(), name.trim())) } else { None }
}

fn extract_columns_from_select(select_sql: &str) -> Vec<String> {
    // Very simple heuristic: take text between SELECT and FROM, split by commas, strip aliases.
    let up = select_sql.to_ascii_uppercase();
    if let Some(pos_sel) = up.find("SELECT ") {
        let after = &select_sql[pos_sel + 7..];
        if let Some(pos_from) = after.to_ascii_uppercase().find(" FROM ") {
            let proj = &after[..pos_from];
            let parts = proj.split(',');
            let mut cols: Vec<String> = Vec::new();
            for p in parts {
                let mut s = p.trim().to_string();
                // Remove function calls or qualifiers as best-effort
                if let Some((_lhs, rhs)) = s.to_ascii_uppercase().split_once(" AS ") { s = rhs.trim().to_string(); }
                // Strip table alias prefix a.b → b
                if let Some((_t, c)) = s.rsplit_once('.') { s = c.trim().to_string(); }
                // Remove quotes/backticks
                s = s.trim_matches('"').trim_matches('`').to_string();
                // Skip empty or literals
                if s.is_empty() || s.chars().all(|ch| ch.is_numeric()) { continue; }
                cols.push(s);
            }
            cols.retain(|c| !c.is_empty());
            return cols;
        }
    }
    Vec::new()
}

fn derive_default_schema(md_path: &Path) -> Option<String> {
    let pstr = md_path.to_string_lossy().to_ascii_lowercase();
    if pstr.contains("pg_catalog") { Some("pg_catalog".to_string()) }
    else if pstr.contains("information schema") || pstr.contains("information_schema") { Some("information_schema".to_string()) }
    else { None }
}

// Parse a big plain SQL text that may contain many CREATE [OR REPLACE] VIEW statements
// and return (schema, name, select_sql) items. The select_sql excludes the CREATE header.
fn parse_views_from_plain_sql(text: &str, default_schema: Option<&str>) -> Vec<(String, String, String)> {
    let mut out: Vec<(String, String, String)> = Vec::new();
    let mut i = 0usize;
    let lines: Vec<&str> = text.lines().collect();
    while i < lines.len() {
        let l = lines[i].trim();
        if l.to_ascii_uppercase().starts_with("CREATE ") && l.to_ascii_uppercase().contains(" VIEW ") {
            // Accumulate until we hit a semicolon line (end of statement) or EOF
            let mut buf: Vec<String> = Vec::new();
            let mut j = i;
            while j < lines.len() {
                buf.push(lines[j].to_string());
                if lines[j].contains(';') { j += 1; break; }
                j += 1;
            }
            let stmt = buf.join("\n");
            // Normalize one space for parsing
            let stmt_trim = stmt.trim();
            let up = stmt_trim.to_ascii_uppercase();
            if let Some(pos_view_kw) = up.find(" VIEW ") {
                let after_view = &stmt_trim[pos_view_kw + " VIEW ".len()..];
                // tolerate AS with arbitrary whitespace
                if let Some(pos_as_kw) = find_as_token(after_view) {
                    let ident = after_view[..pos_as_kw].trim().trim_matches('"');
                    let (schema, name) = split_ident(ident)
                        .or_else(|| default_schema.map(|sch| (sch, ident)))
                        .unwrap_or(("", ident));
                    if !schema.is_empty() {
                        // advance after AS + ws
                        let mut k = pos_as_kw + 2;
                        while k < after_view.len() && after_view.as_bytes()[k].is_ascii_whitespace() { k += 1; }
                        let select_sql = after_view[k..].trim().trim_end_matches(';').trim().to_string();
                        out.push((schema.to_string(), name.to_string(), select_sql));
                    }
                }
            }
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

// Use the central query parser to extract view definitions from full CREATE statements
fn parse_views_via_ast(text: &str, default_schema: Option<&str>) -> Vec<(String, String, String)> {
    let mut out: Vec<(String, String, String)> = Vec::new();
    let mut i = 0usize;
    let lines: Vec<&str> = text.lines().collect();
    while i < lines.len() {
        let l = lines[i].trim();
        if l.to_ascii_uppercase().starts_with("CREATE ") && l.to_ascii_uppercase().contains(" VIEW ") {
            // Accumulate full statement up to semicolon
            let mut buf: Vec<String> = Vec::new();
            let mut j = i;
            while j < lines.len() {
                buf.push(lines[j].to_string());
                if lines[j].contains(';') { j += 1; break; }
                j += 1;
            }
            let stmt = buf.join("\n");
            let stmt_trim = stmt.trim();
            // Parse using central parser
            match crate::server::query::parse(stmt_trim) {
                Ok(crate::server::query::Command::CreateView { name, definition_sql, .. }) => {
                    // split schema and view name
                    if let Some((schema, vname)) = split_ident(&name) {
                        // Filter schemas
                        if schema.eq_ignore_ascii_case("pg_catalog") || schema.eq_ignore_ascii_case("information_schema") {
                            out.push((schema.to_string(), vname.to_string(), definition_sql.trim().trim_end_matches(';').trim().to_string()));
                        }
                    } else if let Some(sch) = default_schema {
                        out.push((sch.to_string(), name.to_string(), definition_sql.trim().trim_end_matches(';').trim().to_string()));
                    }
                }
                _ => {
                    // ignore parse errors; fallback will try heuristics
                }
            }
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

fn sanitize_filename(name: &str) -> String {
    // Replace Windows-invalid filename characters and control chars with '_'
    let invalid: [char; 9] = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
    let mut s = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_control() || invalid.contains(&ch) { s.push('_'); }
        else { s.push(ch); }
    }
    // Also trim trailing dots/spaces which are problematic on Windows
    let s = s.trim().trim_matches('.').trim().to_string();
    if s.is_empty() { "view".to_string() } else { s }
}
