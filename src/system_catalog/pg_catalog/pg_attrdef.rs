use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgAttrDef;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "adrelid", coltype: ColType::Integer },
    ColumnDef { name: "adnum", coltype: ColType::Integer },
    ColumnDef { name: "adbin", coltype: ColType::Text },
];

impl SystemTable for PgAttrDef {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_attrdef" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        // Schema-only empty for now
        DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("adrelid".into(), Vec::<i32>::new()).into(),
            Series::new("adnum".into(), Vec::<i32>::new()).into(),
            Series::new("adbin".into(), Vec::<String>::new()).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgAttrDef)); }
