use anyhow::Result;
use polars::prelude::*;

use crate::storage::SharedStore;

fn root_path(store: &SharedStore) -> std::path::PathBuf {
    let g = store.0.lock();
    g.root_path().clone()
}

/// SHOW TABLES as a DataFrame
/// Columns: table_database, table_schema, table_name
pub fn df_show_tables(store: &SharedStore) -> Result<DataFrame> {
    use std::fs;
    let root = root_path(store);
    let mut dbs_vec: Vec<String> = Vec::new();
    let mut schemas_vec: Vec<String> = Vec::new();
    let mut names_vec: Vec<String> = Vec::new();
    if let Ok(dbs) = fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path();
            if !db_path.is_dir() { continue; }
            let dbname = db_ent.file_name().to_string_lossy().to_string();
            if let Ok(sd) = fs::read_dir(&db_path) {
                for schema_dir in sd.flatten() {
                    let sp = schema_dir.path();
                    if !sp.is_dir() { continue; }
                    let sname = schema_dir.file_name().to_string_lossy().to_string();
                    if let Ok(td) = fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_dir() && tp.join("schema.json").exists() {
                                let tname_os = tentry.file_name();
                                let mut tname = tname_os.to_string_lossy().to_string();
                                if let Some(stripped) = tname.strip_suffix(".time") { tname = stripped.to_string(); }
                                dbs_vec.push(dbname.clone());
                                schemas_vec.push(sname.clone());
                                names_vec.push(tname);
                            }
                        }
                    }
                }
            }
        }
    }
    let df = DataFrame::new(vec![
        Series::new("table_database".into(), dbs_vec).into(),
        Series::new("table_schema".into(), schemas_vec).into(),
        Series::new("table_name".into(), names_vec).into(),
    ])?;
    Ok(df)
}

/// SHOW SCHEMAS as a DataFrame
/// Columns: schema_name
pub fn df_show_schemas(store: &SharedStore) -> Result<DataFrame> {
    use std::fs;
    use std::collections::BTreeSet;
    let mut schemas: BTreeSet<String> = BTreeSet::new();
    let root = root_path(store);
    if let Ok(dbs) = fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = fs::read_dir(&db_path) {
                for sch_ent in sd.flatten() {
                    let p = sch_ent.path(); if p.is_dir() {
                        if let Some(name) = p.file_name().and_then(|s| s.to_str()) { if !name.starts_with('.') { schemas.insert(name.to_string()); } }
                    }
                }
            }
        }
    }
    let list: Vec<String> = schemas.into_iter().collect();
    let df = DataFrame::new(vec![Series::new("schema_name".into(), list).into()])?;
    Ok(df)
}

/// SHOW OBJECTS as a DataFrame
/// Columns: object_database, object_schema, name, type
pub fn df_show_objects(store: &SharedStore) -> Result<DataFrame> {
    use std::fs;
    let root = root_path(store);
    let mut dbs_vec: Vec<String> = Vec::new();
    let mut schemas_vec: Vec<String> = Vec::new();
    let mut names_vec: Vec<String> = Vec::new();
    let mut types_vec: Vec<String> = Vec::new();
    if let Ok(dbs) = fs::read_dir(&root) {
        for db_ent in dbs.flatten() {
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            let dbname = db_ent.file_name().to_string_lossy().to_string();
            if let Ok(sd) = fs::read_dir(&db_path) {
                for schema_dir in sd.flatten() {
                    let sp = schema_dir.path(); if !sp.is_dir() { continue; }
                    let sname = schema_dir.file_name().to_string_lossy().to_string();
                    if let Ok(td) = fs::read_dir(&sp) {
                        for tentry in td.flatten() {
                            let tp = tentry.path();
                            if tp.is_dir() && tp.join("schema.json").exists() {
                                let mut name = tentry.file_name().to_string_lossy().to_string();
                                if let Some(stripped) = name.strip_suffix(".time") { name = stripped.to_string(); }
                                dbs_vec.push(dbname.clone());
                                schemas_vec.push(sname.clone());
                                names_vec.push(name);
                                types_vec.push("table".to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    let df = DataFrame::new(vec![
        Series::new("object_database".into(), dbs_vec).into(),
        Series::new("object_schema".into(), schemas_vec).into(),
        Series::new("name".into(), names_vec).into(),
        Series::new("type".into(), types_vec).into(),
    ])?;
    Ok(df)
}

/// SHOW SCRIPTS as a DataFrame (db, schema, name, kind, folder)
pub fn df_show_scripts(store: &SharedStore) -> Result<DataFrame> {
    use std::fs;
    use crate::scripts::scripts_dir_for;
    let root = root_path(store);
    let mut dbs: Vec<String> = Vec::new();
    let mut schemas: Vec<String> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut kinds: Vec<String> = Vec::new();
    let mut folders: Vec<String> = Vec::new();
    if let Ok(dbs_iter) = fs::read_dir(&root) {
        for db_ent in dbs_iter.flatten() {
            let dbname = db_ent.file_name().to_string_lossy().to_string();
            let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
            if let Ok(sd) = fs::read_dir(&db_path) {
                for sch_ent in sd.flatten() {
                    let sname = sch_ent.file_name().to_string_lossy().to_string();
                    let sdir = scripts_dir_for(std::path::Path::new(&root), &dbname, &sname);
                    if sdir.exists() {
                        for sub in ["scalars", "aggregates", "constraints", "tvfs", "packages"] {
                            let subd = sdir.join(sub);
                            if subd.exists() {
                                if let Ok(listing) = fs::read_dir(&subd) {
                                    for f in listing.flatten() {
                                        let p = f.path();
                                        if p.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") {
                                            let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                            let kind = match sub {
                                                "aggregates" => "aggregate",
                                                "constraints" => "constraint",
                                                "tvfs" => "tvf",
                                                "packages" => "package",
                                                _ => "scalar",
                                            };
                                            dbs.push(dbname.clone());
                                            schemas.push(sname.clone());
                                            names.push(name);
                                            kinds.push(kind.to_string());
                                            folders.push(sub.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let df = DataFrame::new(vec![
        Series::new("db".into(), dbs).into(),
        Series::new("schema".into(), schemas).into(),
        Series::new("name".into(), names).into(),
        Series::new("kind".into(), kinds).into(),
        Series::new("folder".into(), folders).into(),
    ])?;
    Ok(df)
}

/// Try evaluate built-in SHOW TVFs like show_tables(), show_objects(), etc.
/// Returns Some(DataFrame) if recognized, otherwise None.
pub fn try_show_tvf(store: &SharedStore, raw: &str) -> Result<Option<DataFrame>> {
    let s = raw.trim();
    let fname = s.split('(').next().unwrap_or("").trim().to_ascii_lowercase();
    // Must end with ')' (even for empty args)
    if !s.ends_with(')') { return Ok(None); }
    match fname.as_str() {
        "show_tables" => Ok(Some(df_show_tables(store)?)),
        "show_objects" => Ok(Some(df_show_objects(store)?)),
        "show_schemas" | "show_schema" => Ok(Some(df_show_schemas(store)?)),
        "show_scripts" => Ok(Some(df_show_scripts(store)?)),
        _ => Ok(None),
    }
}
