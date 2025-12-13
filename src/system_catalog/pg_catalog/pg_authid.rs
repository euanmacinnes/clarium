use polars::prelude::DataFrame;
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use super::role_common::synthesize_core_roles;

pub struct PgAuthId;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "rolname", coltype: ColType::Text },
    ColumnDef { name: "rolsuper", coltype: ColType::Boolean },
    ColumnDef { name: "rolinherit", coltype: ColType::Boolean },
    ColumnDef { name: "rolcreaterole", coltype: ColType::Boolean },
    ColumnDef { name: "rolcreatedb", coltype: ColType::Boolean },
    ColumnDef { name: "rolcanlogin", coltype: ColType::Boolean },
    ColumnDef { name: "rolreplication", coltype: ColType::Boolean },
    ColumnDef { name: "rolbypassrls", coltype: ColType::Boolean },
    ColumnDef { name: "rolconnlimit", coltype: ColType::Integer },
    ColumnDef { name: "rolpassword", coltype: ColType::Text },
    ColumnDef { name: "rolvaliduntil", coltype: ColType::Text },
];

impl SystemTable for PgAuthId {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_authid" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        let rows = synthesize_core_roles();
        Some(rows.to_df())
    }
}

pub fn register() { registry::register(Box::new(PgAuthId)); }
