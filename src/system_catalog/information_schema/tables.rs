use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use crate::tprintln;

pub struct ITables;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "table_schema", coltype: ColType::Text },
    ColumnDef { name: "table_name", coltype: ColType::Text },
    ColumnDef { name: "table_type", coltype: ColType::Text },
];

impl SystemTable for ITables {
    fn schema(&self) -> &'static str { "information_schema" }
    fn name(&self) -> &'static str { "tables" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let mut schema_col: Vec<String> = Vec::new();
        let mut table_col: Vec<String> = Vec::new();
        let mut type_col: Vec<String> = Vec::new();

        // 1) Real user tables on disk
        let root = store.root_path();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                if let Ok(sd) = std::fs::read_dir(&db_path) {
                    for schema_dir in sd.flatten().filter(|e| e.path().is_dir()) {
                        let schema_name = schema_dir.file_name().to_string_lossy().to_string();
                        if schema_name.starts_with('.') { continue; }
                        if let Ok(td) = std::fs::read_dir(schema_dir.path()) {
                            for tentry in td.flatten() {
                                let tp = tentry.path();
                                if tp.is_dir() {
                                    let has_schema = tp.join("schema.json").exists();
                                    let has_data = tp.join("data.parquet").exists();
                                    let has_chunks = std::fs::read_dir(&tp).ok().and_then(|iter| {
                                        for e in iter.flatten() {
                                            if let Some(name) = e.file_name().to_str() {
                                                if name.starts_with("data-") && name.ends_with(".parquet") { return Some(true); }
                                            }
                                        }
                                        None
                                    }).unwrap_or(false);
                                    if has_schema || has_data || has_chunks {
                                        let mut tname = tentry.file_name().to_string_lossy().to_string();
                                        // Normalize time-table directory names by stripping ".time" suffix
                                        if tname.ends_with(".time") {
                                            tprintln!("[INFO_SCHEMA] tables: stripping '.time' from table_name='{}'", tname);
                                            tname.truncate(tname.len() - 5);
                                        }
                                        schema_col.push(schema_name.clone());
                                        table_col.push(tname);
                                        type_col.push("BASE TABLE".to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2) Registry-based system tables
        let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
        for i in 0..schema_col.len() {
            seen.insert((schema_col[i].clone(), table_col[i].clone()));
        }
        for t in registry::all() {
            let key = (t.schema().to_string(), t.name().to_string());
            if seen.insert(key.clone()) {
                schema_col.push(key.0);
                table_col.push(key.1);
                type_col.push("BASE TABLE".to_string());
            }
        }

        DataFrame::new(vec![
            Series::new("table_schema".into(), schema_col).into(),
            Series::new("table_name".into(), table_col).into(),
            Series::new("table_type".into(), type_col).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(ITables)); }
