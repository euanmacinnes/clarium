use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use std::path::{Path, PathBuf};
use serde_json::Value as JsonValue;

pub struct IViews;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "table_schema", coltype: ColType::Text },
    ColumnDef { name: "table_name", coltype: ColType::Text },
    ColumnDef { name: "view_definition", coltype: ColType::Text },
];

impl SystemTable for IViews {
    fn schema(&self) -> &'static str { "information_schema" }
    fn name(&self) -> &'static str { "views" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let mut schemas: Vec<String> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut defs: Vec<String> = Vec::new();

        let root = store.root_path();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                let dbname = match db_path.file_name().and_then(|s| s.to_str()) { Some(n) => n.to_string(), None => continue };
                if dbname.starts_with('.') { continue; }
                if let Ok(schemas_dir) = std::fs::read_dir(&db_path) {
                    for sch_ent in schemas_dir.flatten() {
                        let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                        let schema_name = sch_path.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
                        if schema_name.starts_with('.') { continue; }
                        if let Ok(entries) = std::fs::read_dir(&sch_path) {
                            for e in entries.flatten() {
                                let p: PathBuf = e.path();
                                if p.is_file() {
                                    if let Some(ext) = p.extension().and_then(|s| s.to_str()) {
                                        if ext.eq_ignore_ascii_case("view") {
                                            let tname = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                                            let mut def = String::new();
                                            if let Some(json) = read_json_file(&p) {
                                                if let Some(s) = json.get("definition_sql").and_then(|v| v.as_str()) {
                                                    def = s.to_string();
                                                }
                                            }
                                            schemas.push(schema_name.clone());
                                            names.push(tname);
                                            defs.push(def);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        DataFrame::new(vec![
            Series::new("table_schema".into(), schemas).into(),
            Series::new("table_name".into(), names).into(),
            Series::new("view_definition".into(), defs).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(IViews)); }

fn read_json_file(path: &Path) -> Option<JsonValue> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<JsonValue>(&s).ok())
}
