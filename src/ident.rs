//! Identifier qualification and path resolution utilities
//! ------------------------------------------------------
//! Single source of truth for resolving database/schema/table identifiers
//! and mapping them to local filesystem paths.

use std::path::{Path, PathBuf};

pub const DEFAULT_DB: &str = "clarium";
pub const DEFAULT_SCHEMA: &str = "public";

/// Normalize an identifier according to SQL rules:
/// - If enclosed in double-quotes, strip quotes and preserve case
/// - Otherwise, convert to lowercase for case-insensitive matching
pub fn normalize_identifier(ident: &str) -> String {
    let trimmed = ident.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        // Double-quoted: preserve case, strip quotes
        trimmed[1..trimmed.len()-1].to_string()
    } else {
        // Unquoted: convert to lowercase
        trimmed.to_ascii_lowercase()
    }
}

#[derive(Debug, Clone)]
pub struct QueryDefaults {
    pub current_database: String,
    pub current_schema: String,
}

impl QueryDefaults {
    pub fn new(db: impl Into<String>, schema: impl Into<String>) -> Self {
        Self { current_database: db.into(), current_schema: schema.into() }
    }
    pub fn from_options(db: Option<&str>, schema: Option<&str>) -> Self {
        Self {
            current_database: db.unwrap_or(DEFAULT_DB).to_string(),
            current_schema: schema.unwrap_or(DEFAULT_SCHEMA).to_string(),
        }
    }
}

/// Detects KV address pattern: <database>.store.<store>.<key>
pub fn is_kv_address(name: &str) -> bool {
    name.contains(".store.")
}

/// Qualify a regular (non-time) table identifier with defaults into canonical form
/// <db>/<schema>/<table>
pub fn qualify_regular_ident(ident: &str, d: &QueryDefaults) -> String {
    qualify_table_ident(ident, d, false)
}

/// Qualify a time-series table identifier, ensuring `.time` suffix on the table segment.
pub fn qualify_time_ident(ident: &str, d: &QueryDefaults) -> String {
    qualify_table_ident(ident, d, true)
}

/// Core qualifier. If `require_time` is true, ensure trailing `.time` on last segment.
pub fn qualify_table_ident(ident: &str, d: &QueryDefaults, require_time: bool) -> String {
    // Path-like input: accept missing db/schema and apply defaults
    let s = ident.replace('\\', "/");
    let (db, schema) = (&d.current_database, &d.current_schema);
    if s.contains('/') {
        let parts: Vec<&str> = s.split('/').filter(|p| !p.is_empty()).collect();
        let (dpart, spart, mut t): (String, String, String) = match parts.len() {
            0 => (normalize_identifier(db), normalize_identifier(schema), String::new()),
            1 => (normalize_identifier(db), normalize_identifier(schema), normalize_identifier(parts[0])),
            2 => (normalize_identifier(db), normalize_identifier(parts[0]), normalize_identifier(parts[1])),
            _ => (normalize_identifier(parts[0]), normalize_identifier(parts[1]), parts[2..].iter().map(|p| normalize_identifier(p)).collect::<Vec<_>>().join("/")),
        };
        if require_time && !t.to_lowercase().ends_with(".time") { t.push_str(".time"); }
        return format!("{}/{}/{}", dpart, spart, t);
    }
    // Dotted or bare identifier
    let parts: Vec<&str> = s.split('.').collect();
    // Special-case dotted inputs that end with a standalone "time" token when a time table is required
    if require_time && parts.len() == 2 && parts[1].eq_ignore_ascii_case("time") {
        let base = normalize_identifier(parts[0]);
        let t = format!("{}.time", base);
        return format!("{}/{}/{}", normalize_identifier(db), normalize_identifier(schema), t);
    }
    if require_time && parts.len() >= 3 && parts.last().map(|x| x.eq_ignore_ascii_case("time")).unwrap_or(false) {
        let dpart = normalize_identifier(parts[0]);
        let spart = normalize_identifier(parts[1]);
        let base = normalize_identifier(parts[parts.len() - 2]);
        let t = format!("{}.time", base);
        return format!("{}/{}/{}", dpart, spart, t);
    }
    let (dpart, spart, mut t): (String, String, String) = match parts.len() {
        0 => (normalize_identifier(db), normalize_identifier(schema), String::new()),
        1 => (normalize_identifier(db), normalize_identifier(schema), normalize_identifier(parts[0])),
        2 => (normalize_identifier(db), normalize_identifier(parts[0]), normalize_identifier(parts[1])),
        _ => (normalize_identifier(parts[0]), normalize_identifier(parts[1]), parts[2..].iter().map(|p| normalize_identifier(p)).collect::<Vec<_>>().join(".")),
    };
    if require_time && !t.to_lowercase().ends_with(".time") { t.push_str(".time"); }
    format!("{}/{}/{}", dpart, spart, t)
}

/// Convert a canonical qualified identifier (with '/' separators) into a local filesystem path under `root`.
/// This also accepts partially qualified inputs and will sanitize path segments.
pub fn to_local_path(root: &Path, qualified_or_raw: &str) -> PathBuf {
    // We tolerate either fully qualified (db/schema/table[.time]) or raw; callers should pass `qualify_*` first for consistency.
    let s = qualified_or_raw.replace('\\', "/");
    let mut out = root.to_path_buf();
    for part in s.split('/') {
        let p = part.trim();
        if p.is_empty() || p == "." || p == ".." { continue; }
        out = out.join(p);
    }
    out
}
