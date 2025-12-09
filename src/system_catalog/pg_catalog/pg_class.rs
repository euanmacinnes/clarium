use polars::prelude::{DataFrame};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use crate::system_catalog::shared::*;

pub struct PgClass;

// Subset of columns we expose elsewhere in the system currently
const COLS: &[ColumnDef] = &[
    ColumnDef { name: "relname", coltype: ColType::Text },
    ColumnDef { name: "nspname", coltype: ColType::Text },
    ColumnDef { name: "relkind", coltype: ColType::Text },
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "relnamespace", coltype: ColType::Integer },
    ColumnDef { name: "relpartbound", coltype: ColType::Text },
];

impl SystemTable for PgClass {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_class" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        // Reuse the legacy builder for now to avoid logic duplication.
        build_pg_class_df(store)
    }
}

/// Build pg_catalog.pg_class as a DataFrame using current store metadata.
/// Kept as a shared helper so both legacy router and class-based registry can reuse it.
fn build_pg_class_df(store: &SharedStore) -> Option<polars::prelude::DataFrame> {
    use polars::prelude::{DataFrame, Series, NamedFrom};
    let metas = enumerate_tables(store);
    let vmetas = enumerate_views(store);
    let idxs = enumerate_vector_indexes(store);
    let graphs = enumerate_graphs(store);
    let mut relname: Vec<String> = Vec::new();
    let mut nspname: Vec<String> = Vec::new();
    let mut relkind: Vec<String> = Vec::new();
    let mut oid: Vec<i32> = Vec::new();
    let mut relnamespace: Vec<i32> = Vec::new();
    let mut relpartbound: Vec<Option<String>> = Vec::new();

    // Map schema names to namespace OIDs (matching pg_namespace)
    let pg_catalog_oid: i32 = 11;
    let information_schema_oid: i32 = 13211;
    let public_oid: i32 = 2200;
    let ns_oid_for = |schema: &str| -> i32 {
        match schema {
            "pg_catalog" => pg_catalog_oid,
            "information_schema" => information_schema_oid,
            "public" => public_oid,
            _ => public_oid,
        }
    };

    for m in metas.iter() {
        relname.push(m.table.clone());
        nspname.push(m.schema.clone());
        relkind.push("r".to_string());
        oid.push(get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table));
        relnamespace.push(ns_oid_for(&m.schema));
        relpartbound.push(None);
    }
    for v in vmetas.iter() {
        relname.push(v.view.clone());
        nspname.push(v.schema.clone());
        relkind.push("v".to_string());
        oid.push(get_or_assign_view_oid(&v.file, &v.db, &v.schema, &v.view));
        relnamespace.push(ns_oid_for(&v.schema));
        relpartbound.push(None);
    }
    // Vector indexes as relkind 'i'
    for x in idxs.iter() {
        relname.push(x.name.clone());
        nspname.push(x.schema.clone());
        relkind.push("i".to_string());
        oid.push(get_or_assign_vindex_oid(&x.file, &x.db, &x.schema, &x.name));
        relnamespace.push(ns_oid_for(&x.schema));
        relpartbound.push(None);
    }
    // Graph catalogs – expose as views (relkind 'v') for client compatibility
    for g in graphs.iter() {
        relname.push(g.name.clone());
        nspname.push(g.schema.clone());
        relkind.push("v".to_string());
        oid.push(get_or_assign_graph_oid(&g.file, &g.db, &g.schema, &g.name));
        relnamespace.push(ns_oid_for(&g.schema));
        relpartbound.push(None);
    }
    DataFrame::new(vec![
        Series::new("relname".into(), relname).into(),
        Series::new("nspname".into(), nspname).into(),
        Series::new("relkind".into(), relkind).into(),
        Series::new("oid".into(), oid).into(),
        Series::new("relnamespace".into(), relnamespace).into(),
        Series::new("relpartbound".into(), relpartbound).into(),
    ]).ok()
}


pub fn register() { registry::register(Box::new(PgClass)); }
