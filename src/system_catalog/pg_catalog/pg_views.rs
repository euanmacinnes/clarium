use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use crate::tprintln;

pub struct PgViews;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "schemaname", coltype: ColType::Text },
    ColumnDef { name: "viewname", coltype: ColType::Text },
    ColumnDef { name: "definition", coltype: ColType::Text },
];

impl SystemTable for PgViews {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_views" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        // Build by merging user-created views and system views registry
        let mut schemaname: Vec<String> = Vec::new();
        let mut viewname: Vec<String> = Vec::new();
        let mut definition: Vec<String> = Vec::new();

        // 1) User-created views live under <db>/<schema>/*.view. Enumerate those from shared helper.
        let vmetas = crate::system_catalog::shared::enumerate_views(store);
        for vm in vmetas.into_iter() {
            schemaname.push(vm.schema.clone());
            viewname.push(vm.view.clone());
            definition.push(vm.def_sql.clone());
        }

        // 2) Add system views loaded from .system registry, schema must be pg_catalog
        for v in crate::system_views::list_views().into_iter() {
            if v.schema.as_str() != "pg_catalog" { continue; }
            schemaname.push(v.schema);
            viewname.push(v.name);
            definition.push(v.sql);
        }

        tprintln!("[loader] pg_views built: rows={} cols=3", schemaname.len());
        DataFrame::new(vec![
            Series::new("schemaname".into(), schemaname).into(),
            Series::new("viewname".into(), viewname).into(),
            Series::new("definition".into(), definition).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgViews)); }
