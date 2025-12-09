use std::fs;
use std::path::{Path, PathBuf};

use crate::tprintln;
use crate::system_paths as sp;

#[derive(Clone, Debug)]
pub struct SystemViewDef {
    pub schema: String,
    pub name: String,
    pub sql: String,
    pub columns: Vec<ViewCol>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ViewCol {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub udt_name: Option<String>,
}

static REGISTRY: parking_lot::RwLock<Vec<SystemViewDef>> = parking_lot::RwLock::new(Vec::new());

fn ensure_dirs(root: &Path) {
    // System views by schema
    let _ = fs::create_dir_all(sp::pg_catalog_views_dir(root));
    let _ = fs::create_dir_all(sp::information_schema_views_dir(root));
    // UDF seeding target (scalars, aggregates, constraints, tvfs)
    let _ = fs::create_dir_all(sp::udf_scalars_dir(root));
    let _ = fs::create_dir_all(sp::udf_aggregates_dir(root));
    let _ = fs::create_dir_all(sp::udf_constraints_dir(root));
    let _ = fs::create_dir_all(sp::udf_tvfs_dir(root));
}

fn global_system_views_root() -> Option<PathBuf> {
    // Look under <repo>/scripts/system_views
    let p = sp::repo_system_views_root();
    if p.exists() { Some(p) } else { None }
}

fn copy_global_to_root(root: &Path) {
    if let Some(glob) = global_system_views_root() {
        for schema in ["pg_catalog", "information_schema"] {
            let src = glob.join(schema);
            if !src.exists() { continue; }
            let dst = if schema.eq_ignore_ascii_case("pg_catalog") {
                sp::pg_catalog_views_dir(root)
            } else {
                sp::information_schema_views_dir(root)
            };
            let _ = fs::create_dir_all(&dst);
            if let Ok(rd) = fs::read_dir(&src) {
                for e in rd.flatten() {
                    let p = e.path();
                    if p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("view")).unwrap_or(false) {
                        let fname = p.file_name().unwrap();
                        let tgt = dst.join(fname);
//                         if !tgt.exists() {
//                             let _ = fs::copy(&p, &tgt);
//                             tprintln!("[views] copied global system view '{}' -> '{}'", p.display(), tgt.display());
//                         }
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
            if !p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("view")).unwrap_or(false) { continue; }
            let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
            if name.is_empty() { continue; }
            // Read .view JSON
            match fs::read_to_string(&p).ok().and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()) {
                Some(json) => {
                    let def_sql = json.get("definition_sql").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let cols: Vec<ViewCol> = json.get("columns")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter().filter_map(|e| {
                                if let Some(obj) = e.as_object() {
                                    let n = obj.get("name").and_then(|v| v.as_str())?.to_string();
                                    let dt = obj.get("data_type").and_then(|v| v.as_str())?.to_string();
                                    let udt = obj.get("udt_name").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    Some(ViewCol { name: n, data_type: dt, udt_name: udt })
                                } else { None }
                            }).collect()
                        })
                        .unwrap_or_default();
                    out.push(SystemViewDef { schema: schema.to_string(), name, sql: def_sql, columns: cols });
                }
                None => {
                    tprintln!("[views] failed to parse .view JSON: {}", p.display());
                }
            }
        }
    }
}

pub fn load_system_views_for_root(root: &Path) {
    ensure_dirs(root);
    copy_global_to_root(root);
    let mut acc: Vec<SystemViewDef> = Vec::new();
    let pg_dir = sp::pg_catalog_views_dir(root);
    let is_dir = sp::information_schema_views_dir(root);
    load_from_folder(&pg_dir, "pg_catalog", &mut acc);
    load_from_folder(&is_dir, "information_schema", &mut acc);
    tprintln!("[views] loaded system views: {} items", acc.len());
    let mut w = REGISTRY.write();
    *w = acc;
    // Persist manifest
    let manifest = sp::system_root(root).join("views_manifest.json");
    let json = serde_json::json!({
        "views": w.iter().map(|v| serde_json::json!({
            "schema": v.schema,
            "name": v.name,
            "sql": v.sql,
            "columns": v.columns.iter().map(|c| serde_json::json!({
                "name": c.name,
                "data_type": c.data_type,
                "udt_name": c.udt_name
            })).collect::<Vec<_>>()
        })).collect::<Vec<_>>()
    });
    let _ = fs::write(&manifest, serde_json::to_string_pretty(&json).unwrap_or_else(|_| "{}".to_string()));
}

pub fn list_views() -> Vec<SystemViewDef> { REGISTRY.read().clone() }

// --- UDF seeding: copy repo scripts to runtime .system/udf on startup ---

fn global_udf_roots() -> Vec<PathBuf> {
    // <repo>/scripts/{scalars,aggregates,constraints,tvfs}
    let mut out = Vec::new();
    for sub in ["scalars", "aggregates", "constraints", "tvfs"] {
        let d = sp::repo_udf_subdir(sub);
        if d.exists() { out.push(d); }
    }
    out
}

pub fn seed_udf_into_root(root: &Path) {
    ensure_dirs(root);
    let dst_base = sp::udf_root(root);
    for src in global_udf_roots() {
        let sub = src.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
        if sub.is_empty() { continue; }
        let dst = dst_base.join(&sub);
        let _ = fs::create_dir_all(&dst);
        if let Ok(rd) = fs::read_dir(&src) {
            for e in rd.flatten() {
                let p = e.path();
                if !p.is_file() { continue; }
                if !p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("lua")).unwrap_or(false) { continue; }
                let fname = p.file_name().unwrap();
                let tgt = dst.join(fname);
                if !tgt.exists() {
//                     if fs::copy(&p, &tgt).is_ok() {
//                         tprintln!("[udf] seeded '{}' -> '{}'", p.display(), tgt.display());
//                     }
                }
            }
        }
    }
}
