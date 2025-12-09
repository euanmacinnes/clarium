use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use crate::tprintln;
use std::path::{Path, PathBuf};
use serde_json::Value as JsonValue;

pub struct PgViews;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "schemaname", coltype: ColType::Text },
    ColumnDef { name: "viewname", coltype: ColType::Text },
    ColumnDef { name: "definition", coltype: ColType::Text },
];

impl SystemTable for PgViews {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_views" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        // Enumerate views by scanning DB root for files with `.view` extension under <db>/<schema>/
        // Matches legacy behavior in system.rs::enumerate_views.
        let mut schemaname: Vec<String> = Vec::new();
        let mut viewname: Vec<String> = Vec::new();
        let mut definition: Vec<String> = Vec::new();

        let root = store.root_path();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                let dbname = match db_path.file_name().and_then(|s| s.to_str()) { Some(n) => n.to_string(), None => continue };
                if dbname.starts_with('.') { continue; }
                if let Ok(schemas) = std::fs::read_dir(&db_path) {
                    for sch_ent in schemas.flatten() {
                        let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                        let schema_name = sch_path.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
                        if schema_name.starts_with('.') { continue; }
                        if let Ok(entries) = std::fs::read_dir(&sch_path) {
                            for e in entries.flatten() {
                                let p: PathBuf = e.path();
                                if p.is_file() && p.extension().and_then(|s| s.to_str()).map(|x| x.eq_ignore_ascii_case("view")).unwrap_or(false) {
                                    let vname = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                    let mut def_sql = String::new();
                                    if let Some(json) = read_json_file(&p) {
                                        if let Some(s) = json.get("definition_sql").and_then(|v| v.as_str()) { def_sql = s.to_string(); }
                                    }
                                    schemaname.push(schema_name.clone());
                                    viewname.push(vname);
                                    definition.push(def_sql);
                                }
                            }
                        }
                    }
                }
            }
        }

        tprintln!("[loader] pg_views built: rows={} cols=3", schemaname.len());
        DataFrame::new(vec![
            Series::new("schemaname".into(), schemaname).into(),
            Series::new("viewname".into(), viewname).into(),
            Series::new("definition".into(), definition).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgViews)); }

fn read_json_file(path: &Path) -> Option<JsonValue> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<JsonValue>(&s).ok())
}
