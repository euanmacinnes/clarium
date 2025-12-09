use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgProc;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "proname", coltype: ColType::Text },
    ColumnDef { name: "pronamespace", coltype: ColType::Integer },
];

impl SystemTable for PgProc {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_proc" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        // Empty by default; future work may populate from UDF registry
        DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("proname".into(), Vec::<String>::new()).into(),
            Series::new("pronamespace".into(), Vec::<i32>::new()).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgProc)); }
