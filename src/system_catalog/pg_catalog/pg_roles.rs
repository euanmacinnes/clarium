use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgRoles;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "rolname", coltype: ColType::Text },
    ColumnDef { name: "rolsuper", coltype: ColType::Boolean },
    ColumnDef { name: "rolinherit", coltype: ColType::Boolean },
    ColumnDef { name: "rolcreaterole", coltype: ColType::Boolean },
    ColumnDef { name: "rolcreatedb", coltype: ColType::Boolean },
    ColumnDef { name: "rolcanlogin", coltype: ColType::Boolean },
    ColumnDef { name: "rolreplication", coltype: ColType::Boolean },
    ColumnDef { name: "rolconnlimit", coltype: ColType::Integer },
    ColumnDef { name: "rolpassword", coltype: ColType::Text },
    ColumnDef { name: "rolvaliduntil", coltype: ColType::Text },
    ColumnDef { name: "rolbypassrls", coltype: ColType::Boolean },
];

impl SystemTable for PgRoles {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_roles" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        let oid: Vec<i32> = vec![10];
        let rolname: Vec<String> = vec!["postgres".into()];
        let rolsuper: Vec<bool> = vec![true];
        let rolinherit: Vec<bool> = vec![true];
        let rolcreaterole: Vec<bool> = vec![true];
        let rolcreatedb: Vec<bool> = vec![true];
        let rolcanlogin: Vec<bool> = vec![true];
        let rolreplication: Vec<bool> = vec![false];
        let rolconnlimit: Vec<i32> = vec![-1];
        // Represent NULL text columns as empty Vec<Option<String>> when needed; here we can use empty string or keep text and allow empty.
        let rolpassword: Vec<String> = vec![String::new()];
        let rolvaliduntil: Vec<String> = vec![String::new()];
        let rolbypassrls: Vec<bool> = vec![true];

        DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("rolname".into(), rolname).into(),
            Series::new("rolsuper".into(), rolsuper).into(),
            Series::new("rolinherit".into(), rolinherit).into(),
            Series::new("rolcreaterole".into(), rolcreaterole).into(),
            Series::new("rolcreatedb".into(), rolcreatedb).into(),
            Series::new("rolcanlogin".into(), rolcanlogin).into(),
            Series::new("rolreplication".into(), rolreplication).into(),
            Series::new("rolconnlimit".into(), rolconnlimit).into(),
            Series::new("rolpassword".into(), rolpassword).into(),
            Series::new("rolvaliduntil".into(), rolvaliduntil).into(),
            Series::new("rolbypassrls".into(), rolbypassrls).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgRoles)); }
