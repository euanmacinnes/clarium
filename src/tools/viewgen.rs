use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::fs;

#[derive(Debug, Clone)]
pub struct GenOptions {
    pub plans_dir: PathBuf,
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
    let plans_root = &opts.plans_dir;
    if !plans_root.exists() { return Err(anyhow!(format!("plans dir not found: {}", plans_root.display()))); }
    // Ensure base out_dir exists
    let _ = fs::create_dir_all(&opts.out_dir);
    let mut total_written = 0usize;
    let mut md_files: Vec<PathBuf> = Vec::new();
    // Collect markdown files recursively under the given plans dir
    collect_md_files(plans_root, &mut md_files);
    for md in md_files.into_iter() {
        let text = match fs::read_to_string(&md) { Ok(s) => s, Err(_) => continue };
        let lines: Vec<&str> = text.lines().collect();
        // Scan for fenced code blocks ```sql ... ```
        let mut i = 0usize;
        while i < lines.len() {
            let line = lines[i].trim();
            if line.starts_with("```") {
                // Capture block
                let mut j = i + 1;
                let mut sql_lines: Vec<String> = Vec::new();
                while j < lines.len() && !lines[j].starts_with("```") { sql_lines.push(lines[j].to_string()); j += 1; }
                let sql_raw = sql_lines.join("\n").trim().trim_matches(';').to_string();
                // Determine schema + name
                let default_schema = derive_default_schema(&md);
                if let Some((schema, name, select_sql)) = extract_view_name_and_sql(&sql_raw, &lines, i, default_schema.as_deref()) {
                    // Determine output subdir by schema
                    let schema_dir = if schema.eq_ignore_ascii_case("pg_catalog") {
                        opts.out_dir.join("pg_catalog")
                    } else if schema.eq_ignore_ascii_case("information_schema") {
                        opts.out_dir.join("information_schema")
                    } else {
                        // Only generate for pg_catalog and information_schema
                        i = j + 1; continue;
                    };
                    let _ = fs::create_dir_all(&schema_dir);
                    // Best-effort columns extraction
                    let cols = extract_columns_from_select(&select_sql);
                    // Build JSON
                    let json = serde_json::json!({
                        "schema": schema,
                        "name": name,
                        "columns": cols.iter().map(|c| serde_json::json!({
                            "name": c.clone(),
                            // Default types for generator; developers may refine later
                            "data_type": "text",
                            "udt_name": "text"
                        })).collect::<Vec<_>>(),
                        "definition_sql": select_sql,
                    });
                    let fname = format!("{}.view", &name);
                    let out_path = schema_dir.join(fname);
                    if out_path.exists() && !opts.overwrite { i = j + 1; continue; }
                    if !opts.dry_run {
                        if let Err(e) = fs::write(&out_path, serde_json::to_string_pretty(&json)?) {
                            eprintln!("[viewgen] failed to write {}: {}", out_path.display(), e);
                        } else {
                            total_written += 1;
                        }
                    }
                }
                i = j + 1;
                continue;
            }
            i += 1;
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
            let rest_up = rest.to_ascii_uppercase();
            if let Some(pos_as) = rest_up.find(" AS ") {
                let ident = rest[..pos_as].trim().trim_matches('"');
                let (schema, name) = split_ident(ident).or_else(|| default_schema.map(|sch| (sch, ident)))?;
                let select_sql = rest[pos_as + 4..].trim().to_string();
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
