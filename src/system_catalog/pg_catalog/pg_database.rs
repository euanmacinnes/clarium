use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use xxhash_rust::xxh3::xxh3_64;

pub struct PgDatabase;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "datname", coltype: ColType::Text },
    ColumnDef { name: "datdba", coltype: ColType::Integer },
    ColumnDef { name: "encoding", coltype: ColType::Integer },
    ColumnDef { name: "datcollate", coltype: ColType::Text },
    ColumnDef { name: "datctype", coltype: ColType::Text },
    ColumnDef { name: "datistemplate", coltype: ColType::Boolean },
    ColumnDef { name: "datallowconn", coltype: ColType::Boolean },
    ColumnDef { name: "datconnlimit", coltype: ColType::Integer },
];

impl SystemTable for PgDatabase {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_database" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let root = store.root_path();
        let mut names: Vec<String> = Vec::new();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for ent in dbs.flatten() {
                let p = ent.path();
                if p.is_dir() {
                    if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                        if !name.starts_with('.') { names.push(name.to_string()); }
                    }
                }
            }
        }
        if names.is_empty() { names.push("clarium".to_string()); }

        // oid: stable positive OID derived from name; we can reuse a simple stable hash
        let oids: Vec<i32> = names.iter().map(|n| {
            let h = xxh3_64(format!("db:{}", n).as_bytes());
            20000 + ((h as u32) % 1_000_000) as i32
        }).collect();
        // datdba: arbitrary stable owner OID (10)
        let datdba: Vec<i32> = vec![10; names.len()];
        // encoding: 6 corresponds to UTF8 in PostgreSQL catalogs
        let encoding: Vec<i32> = vec![6; names.len()];
        let datcollate: Vec<String> = vec!["en_US.UTF-8".into(); names.len()];
        let datctype: Vec<String> = vec!["en_US.UTF-8".into(); names.len()];
        let datistemplate: Vec<bool> = vec![false; names.len()];
        let datallowconn: Vec<bool> = vec![true; names.len()];
        let datconnlimit: Vec<i32> = vec![-1; names.len()];

        DataFrame::new(vec![
            Series::new("oid".into(), oids).into(),
            Series::new("datname".into(), names).into(),
            Series::new("datdba".into(), datdba).into(),
            Series::new("encoding".into(), encoding).into(),
            Series::new("datcollate".into(), datcollate).into(),
            Series::new("datctype".into(), datctype).into(),
            Series::new("datistemplate".into(), datistemplate).into(),
            Series::new("datallowconn".into(), datallowconn).into(),
            Series::new("datconnlimit".into(), datconnlimit).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgDatabase)); }
