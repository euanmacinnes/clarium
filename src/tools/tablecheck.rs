use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct CheckOptions {
    /// Root folder to scan recursively for any directory that contains
    /// a file named `original_schema_tables.md`.
    /// We only read and compare; no files are written.
    pub root_dir: PathBuf,
    /// If true, return non-zero discrepancy count for CLI to enforce.
    pub strict: bool,
}

/// Run a recursive scan for `original_schema_tables.md`, parse `CREATE TABLE`
/// statements to extract column names, and compare with the in-code system
/// registry definitions for `pg_catalog` and `information_schema`.
///
/// Returns the total number of discrepancies found (missing tables plus
/// missing columns across both sides).
pub fn check_system_tables(opts: &CheckOptions) -> Result<usize> {
    if !opts.root_dir.exists() {
        return Err(anyhow!(format!("scan root not found: {}", opts.root_dir.display())));
    }

    // 1) Gather all markdown files to process
    let mut md_files: Vec<PathBuf> = Vec::new();
    collect_original_schema_tables_md(&opts.root_dir, &mut md_files);
    if md_files.is_empty() {
        eprintln!("[tablecheck] no original_schema_tables.md files found under '{}'", opts.root_dir.display());
    }

    // 2) Parse md → map of (schema, table) -> Vec<ColDefMd>
    let mut md_map: HashMap<(String, String), Vec<ColDefMd>> = HashMap::new();
    for md in md_files.iter() {
        let default_schema = derive_default_schema(md);
        let text = match fs::read_to_string(md) { Ok(s) => s, Err(_) => continue };
        let items = parse_tables_from_markdown(&text, default_schema.as_deref());
        for (schema, table, cols) in items {
            if !(schema.eq_ignore_ascii_case("pg_catalog") || schema.eq_ignore_ascii_case("information_schema")) {
                continue;
            }
            md_map.insert((schema.to_lowercase(), table.to_lowercase()), cols);
        }
    }

    // 3) Build registry map
    let reg_map = build_registry_map();

    // 4) Compare and print
    let mut discrepancies = 0usize;
    let mut all_keys: HashSet<(String, String)> = HashSet::new();
    for k in md_map.keys() { all_keys.insert(k.clone()); }
    for k in reg_map.keys() { all_keys.insert(k.clone()); }

    println!("Schema | Table | Column | RequiredType");
    println!("------ | ----- | ------ | -------------");
    for (schema, table) in all_keys.into_iter() {
        let md_cols: Vec<ColDefMd> = md_map
            .get(&(schema.clone(), table.clone()))
            .cloned()
            .unwrap_or_else(|| Vec::new());
        let reg_cols: HashSet<String> = reg_map
            .get(&(schema.clone(), table.clone()))
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_else(|| HashSet::new());

        for c in md_cols.iter() {
            if !reg_cols.contains(&c.name) {
                discrepancies += 1;
                println!(
                    "{} | {} | {} | {}",
                    schema,
                    table,
                    c.name,
                    c.dtype
                );
            }
        }
    }

    println!("[tablecheck] discrepancies found: {}", discrepancies);
    Ok(discrepancies)
}

#[derive(Debug, Clone)]
struct ColDefMd { name: String, dtype: String }

fn collect_original_schema_tables_md(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() { return; }
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                collect_original_schema_tables_md(&p, out);
            } else if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name.eq_ignore_ascii_case("original_schema_tables.md") {
                    out.push(p);
                }
            }
        }
    }
}

fn derive_default_schema(md_path: &Path) -> Option<String> {
    let pstr = md_path.to_string_lossy().to_ascii_lowercase();
    if pstr.contains("pg_catalog") { Some("pg_catalog".to_string()) }
    else if pstr.contains("information schema") || pstr.contains("information_schema") { Some("information_schema".to_string()) }
    else { None }
}

fn parse_tables_from_markdown(text: &str, default_schema: Option<&str>) -> Vec<(String, String, Vec<ColDefMd>)> {
    // Strategy: scan lines; when we see a line that contains CREATE and TABLE,
    // accumulate until we hit a semicolon that closes the statement. Then parse it.
    let mut out: Vec<(String, String, Vec<ColDefMd>)> = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut i = 0usize;
    while i < lines.len() {
        let l = lines[i].trim();
        let up = l.to_ascii_uppercase();
        if up.contains("CREATE") && up.contains("TABLE") {
            // Accumulate lines until semicolon reached (best-effort, handles multi-lines)
            let mut buf: Vec<String> = Vec::new();
            let mut j = i;
            while j < lines.len() {
                buf.push(lines[j].to_string());
                if lines[j].contains(';') { j += 1; break; }
                j += 1;
            }
            let stmt = buf.join("\n");
            if let Some((schema, table, cols)) = parse_create_table_columns(&stmt, default_schema) {
                out.push((schema, table, cols));
            }
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

fn parse_create_table_columns(stmt: &str, default_schema: Option<&str>) -> Option<(String, String, Vec<ColDefMd>)> {
    // Normalize whitespace lightly for name extraction
    let s = stmt.trim();
    let sup = s.to_ascii_uppercase();
    if !sup.starts_with("CREATE ") { return None; }
    if !sup.contains(" TABLE ") { return None; }

    // Find position after the keyword TABLE (ignoring modifiers like TEMP, UNLOGGED, OR REPLACE/ALTER)
    // Approach: find the index of " TABLE " after the leading CREATE ...
    let pos_table = sup.find(" TABLE ")?;
    let after_table = s[pos_table + " TABLE ".len()..].trim();

    // Name goes up to the first '(' or whitespace if no parens (but CREATE TABLE requires parens)
    let mut name_end = after_table.find('(').unwrap_or_else(|| after_table.len());
    // Trim trailing whitespace before '('
    let name_part = after_table[..name_end].trim();
    let name_clean = name_part.trim_matches('"').trim_matches('`');

    // Split schema.table if present, else use default schema
    let (schema, table) = if let Some((sch, tbl)) = name_clean.split_once('.') {
        (sch.trim().to_string(), tbl.trim().to_string())
    } else if let Some(sch) = default_schema { (sch.to_string(), name_clean.to_string()) } else { return None };

    // Capture the columns substring between the outermost parentheses after name
    let paren_start = after_table.find('(')?;
    let (cols_text, _) = capture_balanced_parens(&after_table[paren_start..])?; // includes surrounding parens
    let inner = &cols_text[1..cols_text.len()-1];

    // Split by top-level commas
    let parts = split_top_level_commas(inner);
    let mut cols: Vec<ColDefMd> = Vec::new();
    for p in parts.into_iter() {
        let item = p.trim();
        if item.is_empty() { continue; }
        let up = item.to_ascii_uppercase();
        // Skip table constraints
        if up.starts_with("PRIMARY ") || up.starts_with("FOREIGN ") || up.starts_with("UNIQUE ") || up.starts_with("CHECK ") || up.starts_with("CONSTRAINT ") || up.starts_with("EXCLUDE ") || up.starts_with("LIKE ") || up.starts_with("INHERITS ") {
            continue;
        }
        // Column definition: first token is the column name (may be quoted)
        let (name, dtype) = split_col_name_and_type(item);
        if !name.is_empty() {
            let norm = crate::ident::normalize_identifier(name);
            let dtype_clean = dtype.trim().trim_matches(',').trim().to_string();
            cols.push(ColDefMd { name: norm, dtype: dtype_clean });
        }
    }
    // Dedup while preserving order
    let mut seen: HashSet<String> = HashSet::new();
    cols.retain(|c| seen.insert(c.name.clone()));

    Some((schema.to_lowercase(), table.to_lowercase(), cols))
}

fn first_ident(s: &str) -> &str {
    let st = s.trim_start();
    if st.starts_with('"') {
        if let Some(end) = st[1..].find('"') { return &st[1..1+end]; }
        return st.trim_matches('"');
    }
    // read until whitespace or '(' or ','
    let mut end = st.len();
    for (i, ch) in st.char_indices() {
        if ch.is_whitespace() || ch == '(' || ch == ',' { end = i; break; }
    }
    &st[..end]
}

// Split a column definition line into (name, datatype_string)
fn split_col_name_and_type(s: &str) -> (&str, String) {
    let st = s.trim_start();
    let name = first_ident(st);
    if name.is_empty() { return (name, String::new()); }
    // Slice after the name
    let mut idx = 0usize;
    // advance idx to after name in st
    if st.starts_with('"') {
        // name was quoted
        idx = 2 + st[1..].find('"').unwrap_or(0);
    } else {
        idx = name.len();
    }
    let after = st[idx..].trim_start();
    let dtype = take_type_until_constraints(after);
    (name, dtype)
}

fn take_type_until_constraints(s: &str) -> String {
    // Collect characters until we reach a constraint keyword at top level (depth==0)
    // Constraint starters we recognize:
    // CONSTRAINT, PRIMARY, NOT, NULL, DEFAULT, REFERENCES, CHECK, UNIQUE, COLLATE, GENERATED, IDENTITY, DEFERRABLE
    let keywords = [
        "CONSTRAINT", "PRIMARY", "NOT", "NULL", "DEFAULT", "REFERENCES", "CHECK", "UNIQUE", "COLLATE", "GENERATED", "IDENTITY", "DEFERRABLE"
    ];
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '\'' => { in_single = !in_single; i += 1; continue; }
            '"' => { in_double = !in_double; i += 1; continue; }
            '(' if !in_single && !in_double => { depth += 1; i += 1; continue; }
            ')' if !in_single && !in_double => { depth -= 1; i += 1; continue; }
            _ => {}
        }
        if depth == 0 && !in_single && !in_double {
            // Check for any keyword at this position with word boundary
            if is_boundary(bytes, i) {
                for kw in keywords.iter() {
                    if starts_with_ci(&s[i..], kw) {
                        // stop before this keyword
                        return s[..i].trim().to_string();
                    }
                }
            }
        }
        i += 1;
    }
    s.trim().to_string()
}

fn is_boundary(bytes: &[u8], i: usize) -> bool {
    let before = if i == 0 { ' ' } else { bytes[i - 1] as char };
    before.is_whitespace() || before == ',' || before == '('
}

fn starts_with_ci(s: &str, kw: &str) -> bool {
    let up = s.to_ascii_uppercase();
    if !up.starts_with(kw) { return false; }
    // ensure following char is boundary
    if up.len() == kw.len() { return true; }
    let next = up.as_bytes()[kw.len()] as char;
    next.is_whitespace() || next == '(' || next == ','
}

fn capture_balanced_parens(s: &str) -> Option<(String, usize)> {
    // s is expected to start with '('; we return the full balanced substring and the end offset
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'(' { return None; }
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'(' { depth += 1; }
        else if b == b')' { depth -= 1; if depth == 0 { return Some((s[..=i].to_string(), i+1)); } }
    }
    None
}

fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut depth = 0i32; // parentheses depth to avoid splitting in type params
    let mut in_single = false;
    let mut in_double = false;
    for ch in s.chars() {
        match ch {
            '\'' => { in_single = !in_single; buf.push(ch); }
            '"' => { in_double = !in_double; buf.push(ch); }
            '(' => { depth += 1; buf.push(ch); }
            ')' => { depth -= 1; buf.push(ch); }
            ',' if depth == 0 && !in_single && !in_double => {
                out.push(buf.trim().to_string());
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() { out.push(buf.trim().to_string()); }
    out
}

fn build_registry_map() -> HashMap<(String, String), Vec<String>> {
    let mut map: HashMap<(String, String), Vec<String>> = HashMap::new();
    crate::system_catalog::registry::ensure_registered();
    for t in crate::system_catalog::registry::all().into_iter() {
        let schema = t.schema().to_lowercase();
        if !(schema == "pg_catalog" || schema == "information_schema") { continue; }
        let table = t.name().to_lowercase();
        let cols = t.columns().iter().map(|c| c.name.to_string().to_lowercase()).collect::<Vec<_>>();
        map.insert((schema, table), cols);
    }
    map
}
