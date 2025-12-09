use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct ISchemata;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "schema_name", coltype: ColType::Text },
];

impl SystemTable for ISchemata {
    fn schema(&self) -> &'static str { "information_schema" }
    fn name(&self) -> &'static str { "schemata" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let root = store.root_path();
        let mut schemas: Vec<String> = Vec::new();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                if let Ok(sd) = std::fs::read_dir(&db_path) {
                    for sch_ent in sd.flatten() {
                        let sch_path = sch_ent.path(); if !sch_path.is_dir() { continue; }
                        if let Some(name) = sch_path.file_name().and_then(|s| s.to_str()) {
                            if !name.starts_with('.') { schemas.push(name.to_string()); }
                        }
                    }
                }
            }
        }
        schemas.push("pg_catalog".to_string());
        schemas.push("information_schema".to_string());
        schemas.sort();
        schemas.dedup();
        DataFrame::new(vec![Series::new("schema_name".into(), schemas).into()]).ok()
    }
}

pub fn register() { registry::register(Box::new(ISchemata)); }
