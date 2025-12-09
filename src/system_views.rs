use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use parking_lot::RwLock;

use crate::tprintln;

#[derive(Clone, Debug)]
pub struct SystemViewDef {
    pub schema: String,
    pub name: String,
    pub sql: String,
}

static REGISTRY: parking_lot::RwLock<Vec<SystemViewDef>> = parking_lot::RwLock::new(Vec::new());

fn system_dir(root: &Path) -> PathBuf { root.join(".system") }

fn schema_dir(root: &Path, schema: &str) -> PathBuf { system_dir(root).join(schema) }

fn ensure_dirs(root: &Path) {
    let _ = fs::create_dir_all(schema_dir(root, "pg_catalog"));
    let _ = fs::create_dir_all(schema_dir(root, "information_schema"));
}

fn global_system_views_root() -> Option<PathBuf> {
    // Look under <repo>/scripts/system_views
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("scripts");
    p.push("system_views");
    if p.exists() { Some(p) } else { None }
}

fn copy_global_to_root(root: &Path) {
    if let Some(glob) = global_system_views_root() {
        for schema in ["pg_catalog", "information_schema"] {
            let src = glob.join(schema);
            if !src.exists() { continue; }
            let dst = schema_dir(root, schema);
            let _ = fs::create_dir_all(&dst);
            if let Ok(rd) = fs::read_dir(&src) {
                for e in rd.flatten() {
                    let p = e.path();
                    if p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("sql")).unwrap_or(false) {
                        let fname = p.file_name().unwrap();
                        let tgt = dst.join(fname);
                        if !tgt.exists() {
                            let _ = fs::copy(&p, &tgt);
                            tprintln!("[views] copied global system view '{}' -> '{}'", p.display(), tgt.display());
                        }
                    }
                }
            }
        }
    }
}

fn load_from_folder(folder: &Path, schema: &str, out: &mut Vec<SystemViewDef>) {
    if !folder.exists() { return; }
    if let Ok(rd) = fs::read_dir(folder) {
        for e in rd.flatten() {
            let p = e.path();
            if !p.is_file() { continue; }
            if !p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("sql")).unwrap_or(false) { continue; }
            let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
            if name.is_empty() { continue; }
            let text = match fs::read_to_string(&p) { Ok(s) => s, Err(_) => continue };
            let sql = extract_select_sql(&text).unwrap_or_else(|| text.clone());
            out.push(SystemViewDef { schema: schema.to_string(), name, sql });
        }
    }
}

fn extract_select_sql(text: &str) -> Option<String> {
    let mut s = String::new();
    for line in text.lines() {
        let lt = line.trim();
        if lt.starts_with("--") || lt.is_empty() { continue; }
        s.push_str(lt);
        s.push(' ');
    }
    let up = s.to_ascii_lowercase();
    if up.contains("create view") && up.contains(" as select ") {
        // very rough split
        if let Some(pos) = up.find(" as select ") {
            let start = pos + " as ".len();
            let tail = &s[start..];
            return Some(tail.trim().trim_end_matches(';').to_string());
        }
    }
    if up.starts_with("select ") { return Some(s.trim().trim_end_matches(';').to_string()); }
    None
}

pub fn load_system_views_for_root(root: &Path) {
    ensure_dirs(root);
    copy_global_to_root(root);
    let mut acc: Vec<SystemViewDef> = Vec::new();
    let pg_dir = schema_dir(root, "pg_catalog");
    let is_dir = schema_dir(root, "information_schema");
    load_from_folder(&pg_dir, "pg_catalog", &mut acc);
    load_from_folder(&is_dir, "information_schema", &mut acc);
    tprintln!("[views] loaded system views: {} items", acc.len());
    let mut w = REGISTRY.write();
    *w = acc;
    // Persist manifest
    let manifest = system_dir(root).join("views_manifest.json");
    let json = serde_json::json!({
        "views": w.iter().map(|v| serde_json::json!({
            "schema": v.schema, "name": v.name, "sql": v.sql
        })).collect::<Vec<_>>()
    });
    let _ = fs::write(&manifest, serde_json::to_string_pretty(&json).unwrap_or_else(|_| "{}".to_string()));
}

pub fn list_views() -> Vec<SystemViewDef> { REGISTRY.read().clone() }
