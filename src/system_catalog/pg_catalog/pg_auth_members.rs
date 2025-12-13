use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgAuthMembers;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "roleid", coltype: ColType::Integer },
    ColumnDef { name: "member", coltype: ColType::Integer },
    ColumnDef { name: "grantor", coltype: ColType::Integer },
    ColumnDef { name: "admin_option", coltype: ColType::Boolean },
];

impl SystemTable for PgAuthMembers {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_auth_members" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        // No role memberships defined yet in RBAC surface; return empty with correct columns.
        let roleid: Vec<i32> = Vec::new();
        let member: Vec<i32> = Vec::new();
        let grantor: Vec<i32> = Vec::new();
        let admin_option: Vec<bool> = Vec::new();
        DataFrame::new(vec![
            Series::new("roleid".into(), roleid).into(),
            Series::new("member".into(), member).into(),
            Series::new("grantor".into(), grantor).into(),
            Series::new("admin_option".into(), admin_option).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgAuthMembers)); }
