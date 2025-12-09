use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgProc;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "proname", coltype: ColType::Text },
    ColumnDef { name: "pronamespace", coltype: ColType::Integer },
    // added per reconciliation
    ColumnDef { name: "proowner", coltype: ColType::Integer },
    ColumnDef { name: "prolang", coltype: ColType::Integer },
    ColumnDef { name: "procost", coltype: ColType::Text },
    ColumnDef { name: "prorows", coltype: ColType::Text },
    ColumnDef { name: "provariadic", coltype: ColType::Integer },
    ColumnDef { name: "prosupport", coltype: ColType::Integer },
    ColumnDef { name: "prokind", coltype: ColType::Text },
    ColumnDef { name: "prosecdef", coltype: ColType::Boolean },
    ColumnDef { name: "proleakproof", coltype: ColType::Boolean },
    ColumnDef { name: "proisstrict", coltype: ColType::Boolean },
    ColumnDef { name: "proretset", coltype: ColType::Boolean },
    ColumnDef { name: "provolatile", coltype: ColType::Text },
    ColumnDef { name: "proparallel", coltype: ColType::Text },
    ColumnDef { name: "pronargs", coltype: ColType::Integer },
    ColumnDef { name: "pronargdefaults", coltype: ColType::Integer },
    ColumnDef { name: "prorettype", coltype: ColType::Integer },
    ColumnDef { name: "proargtypes", coltype: ColType::Text },
    ColumnDef { name: "proallargtypes", coltype: ColType::Text },
    ColumnDef { name: "proargmodes", coltype: ColType::Text },
    ColumnDef { name: "proargnames", coltype: ColType::Text },
    ColumnDef { name: "proargdefaults", coltype: ColType::Text },
    ColumnDef { name: "protrftypes", coltype: ColType::Text },
    ColumnDef { name: "prosrc", coltype: ColType::Text },
    ColumnDef { name: "probin", coltype: ColType::Text },
    ColumnDef { name: "prosqlbody", coltype: ColType::Text },
    ColumnDef { name: "proconfig", coltype: ColType::Text },
    ColumnDef { name: "proacl", coltype: ColType::Text },
];

impl SystemTable for PgProc {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_proc" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        // Empty by default; future work may populate from UDF registry
        // Build an empty frame with all declared columns
        DataFrame::new(vec![
            Series::new("oid".into(), Vec::<i32>::new()).into(),
            Series::new("proname".into(), Vec::<String>::new()).into(),
            Series::new("pronamespace".into(), Vec::<i32>::new()).into(),
            Series::new("proowner".into(), Vec::<i32>::new()).into(),
            Series::new("prolang".into(), Vec::<i32>::new()).into(),
            Series::new("procost".into(), Vec::<String>::new()).into(),
            Series::new("prorows".into(), Vec::<String>::new()).into(),
            Series::new("provariadic".into(), Vec::<i32>::new()).into(),
            Series::new("prosupport".into(), Vec::<i32>::new()).into(),
            Series::new("prokind".into(), Vec::<String>::new()).into(),
            Series::new("prosecdef".into(), Vec::<bool>::new()).into(),
            Series::new("proleakproof".into(), Vec::<bool>::new()).into(),
            Series::new("proisstrict".into(), Vec::<bool>::new()).into(),
            Series::new("proretset".into(), Vec::<bool>::new()).into(),
            Series::new("provolatile".into(), Vec::<String>::new()).into(),
            Series::new("proparallel".into(), Vec::<String>::new()).into(),
            Series::new("pronargs".into(), Vec::<i32>::new()).into(),
            Series::new("pronargdefaults".into(), Vec::<i32>::new()).into(),
            Series::new("prorettype".into(), Vec::<i32>::new()).into(),
            Series::new("proargtypes".into(), Vec::<String>::new()).into(),
            Series::new("proallargtypes".into(), Vec::<String>::new()).into(),
            Series::new("proargmodes".into(), Vec::<String>::new()).into(),
            Series::new("proargnames".into(), Vec::<String>::new()).into(),
            Series::new("proargdefaults".into(), Vec::<String>::new()).into(),
            Series::new("protrftypes".into(), Vec::<String>::new()).into(),
            Series::new("prosrc".into(), Vec::<String>::new()).into(),
            Series::new("probin".into(), Vec::<String>::new()).into(),
            Series::new("prosqlbody".into(), Vec::<String>::new()).into(),
            Series::new("proconfig".into(), Vec::<String>::new()).into(),
            Series::new("proacl".into(), Vec::<String>::new()).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgProc)); }
