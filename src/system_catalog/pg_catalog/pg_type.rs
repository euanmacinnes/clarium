use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgType;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "typname", coltype: ColType::Text },
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "typarray", coltype: ColType::Integer },
    ColumnDef { name: "typnamespace", coltype: ColType::Integer },
    ColumnDef { name: "typelem", coltype: ColType::Integer },
    ColumnDef { name: "typrelid", coltype: ColType::Integer },
    ColumnDef { name: "typbasetype", coltype: ColType::Integer },
    ColumnDef { name: "typtypmod", coltype: ColType::Integer },
    ColumnDef { name: "typcategory", coltype: ColType::Text },
    ColumnDef { name: "typtype", coltype: ColType::Text },
    ColumnDef { name: "typdelim", coltype: ColType::Text },
];

impl SystemTable for PgType {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_type" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        // Mirror existing minimal pg_type setup from legacy implementation
        let names: Vec<String> = vec![
            "int4".into(), "int8".into(), "float8".into(), "text".into(), "bool".into(),
            "timestamp".into(), "timestamptz".into(), "hstore".into(), "vector".into(),
        ];
        let oids: Vec<i32> = vec![23, 20, 701, 25, 16, 1114, 1184, 16414, 70400];
        let arrays: Vec<i32> = vec![1007, 1016, 1022, 1009, 1000, 1115, 1185, 16415, 70401];
        let pg_catalog_oid: i32 = 11;
        let typnamespace: Vec<i32> = vec![pg_catalog_oid; names.len()];
        let typelem: Vec<i32> = vec![0; names.len()];
        let typrelid: Vec<i32> = vec![0; names.len()];
        let typbasetype: Vec<i32> = vec![0; names.len()];
        let typtypmod: Vec<i32> = vec![-1; names.len()];
        let typcategory: Vec<String> = vec![
            "N".into(), // int4 numeric
            "N".into(), // int8 numeric
            "N".into(), // float8 numeric
            "S".into(), // text string
            "B".into(), // bool boolean
            "D".into(), // timestamp datetime
            "D".into(), // timestamptz datetime
            "U".into(), // hstore user-defined
            "U".into(), // vector user-defined
        ];
        let typtype: Vec<String> = vec!["b".into(); names.len()];
        let typdelim: Vec<String> = vec![",".into(); names.len()];

        DataFrame::new(vec![
            Series::new("typname".into(), names).into(),
            Series::new("oid".into(), oids).into(),
            Series::new("typarray".into(), arrays).into(),
            Series::new("typnamespace".into(), typnamespace).into(),
            Series::new("typelem".into(), typelem).into(),
            Series::new("typrelid".into(), typrelid).into(),
            Series::new("typbasetype".into(), typbasetype).into(),
            Series::new("typtypmod".into(), typtypmod).into(),
            Series::new("typcategory".into(), typcategory).into(),
            Series::new("typtype".into(), typtype).into(),
            Series::new("typdelim".into(), typdelim).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgType)); }
