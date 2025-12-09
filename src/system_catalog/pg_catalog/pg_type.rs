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
    // Added per reconciliation
    ColumnDef { name: "typowner", coltype: ColType::Integer },
    ColumnDef { name: "typlen", coltype: ColType::Integer },
    ColumnDef { name: "typbyval", coltype: ColType::Boolean },
    ColumnDef { name: "typispreferred", coltype: ColType::Boolean },
    ColumnDef { name: "typisdefined", coltype: ColType::Boolean },
    ColumnDef { name: "typsubscript", coltype: ColType::Integer },
    ColumnDef { name: "typinput", coltype: ColType::Integer },
    ColumnDef { name: "typoutput", coltype: ColType::Integer },
    ColumnDef { name: "typreceive", coltype: ColType::Integer },
    ColumnDef { name: "typsend", coltype: ColType::Integer },
    ColumnDef { name: "typmodin", coltype: ColType::Integer },
    ColumnDef { name: "typmodout", coltype: ColType::Integer },
    ColumnDef { name: "typanalyze", coltype: ColType::Integer },
    ColumnDef { name: "typalign", coltype: ColType::Text },
    ColumnDef { name: "typstorage", coltype: ColType::Text },
    ColumnDef { name: "typnotnull", coltype: ColType::Boolean },
    ColumnDef { name: "typndims", coltype: ColType::Integer },
    ColumnDef { name: "typcollation", coltype: ColType::Integer },
    ColumnDef { name: "typdefaultbin", coltype: ColType::Text },
    ColumnDef { name: "typdefault", coltype: ColType::Text },
    ColumnDef { name: "typacl", coltype: ColType::Text },
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

        // Added columns defaults
        let typowner: Vec<i32> = vec![10; names.len()];
        let typlen: Vec<i32> = vec![-1; names.len()];
        let typbyval: Vec<bool> = vec![false; names.len()];
        let typispreferred: Vec<bool> = vec![false; names.len()];
        let typisdefined: Vec<bool> = vec![true; names.len()];
        let zeros_i32: Vec<i32> = vec![0; names.len()];
        let typalign: Vec<String> = vec!["i".into(); names.len()];
        let typstorage: Vec<String> = vec!["p".into(); names.len()];
        let typnotnull: Vec<bool> = vec![false; names.len()];
        let typndims: Vec<i32> = vec![0; names.len()];
        let typcollation: Vec<i32> = vec![0; names.len()];
        let empty_txt: Vec<Option<String>> = vec![None; names.len()];
        let empty_txt_s: Vec<String> = vec![String::new(); names.len()];

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
            Series::new("typowner".into(), typowner).into(),
            Series::new("typlen".into(), typlen).into(),
            Series::new("typbyval".into(), typbyval).into(),
            Series::new("typispreferred".into(), typispreferred).into(),
            Series::new("typisdefined".into(), typisdefined).into(),
            Series::new("typsubscript".into(), zeros_i32.clone()).into(),
            Series::new("typinput".into(), zeros_i32.clone()).into(),
            Series::new("typoutput".into(), zeros_i32.clone()).into(),
            Series::new("typreceive".into(), zeros_i32.clone()).into(),
            Series::new("typsend".into(), zeros_i32.clone()).into(),
            Series::new("typmodin".into(), zeros_i32.clone()).into(),
            Series::new("typmodout".into(), zeros_i32.clone()).into(),
            Series::new("typanalyze".into(), zeros_i32.clone()).into(),
            Series::new("typalign".into(), typalign).into(),
            Series::new("typstorage".into(), typstorage).into(),
            Series::new("typnotnull".into(), typnotnull).into(),
            Series::new("typndims".into(), typndims).into(),
            Series::new("typcollation".into(), typcollation).into(),
            Series::new("typdefaultbin".into(), empty_txt.clone()).into(),
            Series::new("typdefault".into(), empty_txt_s).into(),
            Series::new("typacl".into(), empty_txt).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgType)); }
