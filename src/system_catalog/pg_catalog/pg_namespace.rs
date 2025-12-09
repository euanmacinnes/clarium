use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgNamespace;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "nspname", coltype: ColType::Text },
];

impl SystemTable for PgNamespace {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_namespace" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        // Provide minimal pg_namespace with OIDs for pg_catalog, information_schema and public
        let nspname: Vec<String> = vec!["pg_catalog".into(), "information_schema".into(), "public".into()];
        let oid: Vec<i32> = vec![11, 13211, 2200];
        DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("nspname".into(), nspname).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgNamespace)); }
