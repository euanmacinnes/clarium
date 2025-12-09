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
    // added per reconciliation
    ColumnDef { name: "reltype", coltype: ColType::Integer },
    ColumnDef { name: "reloftype", coltype: ColType::Integer },
    ColumnDef { name: "relowner", coltype: ColType::Integer },
    ColumnDef { name: "relam", coltype: ColType::Integer },
    ColumnDef { name: "relfilenode", coltype: ColType::Integer },
    ColumnDef { name: "reltablespace", coltype: ColType::Integer },
    ColumnDef { name: "relpages", coltype: ColType::Integer },
    ColumnDef { name: "reltuples", coltype: ColType::Text },
    ColumnDef { name: "relallvisible", coltype: ColType::Integer },
    ColumnDef { name: "reltoastrelid", coltype: ColType::Integer },
    ColumnDef { name: "relhasindex", coltype: ColType::Boolean },
    ColumnDef { name: "relisshared", coltype: ColType::Boolean },
    ColumnDef { name: "relpersistence", coltype: ColType::Text },
    ColumnDef { name: "relnatts", coltype: ColType::Integer },
    ColumnDef { name: "relchecks", coltype: ColType::Integer },
    ColumnDef { name: "relhasrules", coltype: ColType::Boolean },
    ColumnDef { name: "relhastriggers", coltype: ColType::Boolean },
    ColumnDef { name: "relhassubclass", coltype: ColType::Boolean },
    ColumnDef { name: "relrowsecurity", coltype: ColType::Boolean },
    ColumnDef { name: "relforcerowsecurity", coltype: ColType::Boolean },
    ColumnDef { name: "relispopulated", coltype: ColType::Boolean },
    ColumnDef { name: "relreplident", coltype: ColType::Text },
    ColumnDef { name: "relispartition", coltype: ColType::Boolean },
    ColumnDef { name: "relrewrite", coltype: ColType::Integer },
    ColumnDef { name: "relfrozenxid", coltype: ColType::Text },
    ColumnDef { name: "relminmxid", coltype: ColType::Text },
    ColumnDef { name: "relacl", coltype: ColType::Text },
    ColumnDef { name: "reloptions", coltype: ColType::Text },
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
    let rows = relname.len();
    // Defaults for added columns
    let zeros_i32: Vec<i32> = vec![0; rows];
    let falses: Vec<bool> = vec![false; rows];
    let empty_txt: Vec<Option<String>> = vec![None; rows];
    let empty_txt_s: Vec<String> = vec![String::new(); rows];
    let relpersistence: Vec<String> = vec!["p".into(); rows];
    let relreplident: Vec<String> = vec!["n".into(); rows];

    DataFrame::new(vec![
        Series::new("relname".into(), relname).into(),
        Series::new("nspname".into(), nspname).into(),
        Series::new("relkind".into(), relkind).into(),
        Series::new("oid".into(), oid).into(),
        Series::new("relnamespace".into(), relnamespace).into(),
        Series::new("relpartbound".into(), relpartbound).into(),
        Series::new("reltype".into(), zeros_i32.clone()).into(),
        Series::new("reloftype".into(), zeros_i32.clone()).into(),
        Series::new("relowner".into(), vec![10; rows]).into(),
        Series::new("relam".into(), zeros_i32.clone()).into(),
        Series::new("relfilenode".into(), zeros_i32.clone()).into(),
        Series::new("reltablespace".into(), zeros_i32.clone()).into(),
        Series::new("relpages".into(), zeros_i32.clone()).into(),
        Series::new("reltuples".into(), empty_txt_s.clone()).into(),
        Series::new("relallvisible".into(), zeros_i32.clone()).into(),
        Series::new("reltoastrelid".into(), zeros_i32.clone()).into(),
        Series::new("relhasindex".into(), falses.clone()).into(),
        Series::new("relisshared".into(), falses.clone()).into(),
        Series::new("relpersistence".into(), relpersistence).into(),
        Series::new("relnatts".into(), zeros_i32.clone()).into(),
        Series::new("relchecks".into(), zeros_i32.clone()).into(),
        Series::new("relhasrules".into(), falses.clone()).into(),
        Series::new("relhastriggers".into(), falses.clone()).into(),
        Series::new("relhassubclass".into(), falses.clone()).into(),
        Series::new("relrowsecurity".into(), falses.clone()).into(),
        Series::new("relforcerowsecurity".into(), falses.clone()).into(),
        Series::new("relispopulated".into(), falses.clone()).into(),
        Series::new("relreplident".into(), relreplident).into(),
        Series::new("relispartition".into(), falses.clone()).into(),
        Series::new("relrewrite".into(), zeros_i32.clone()).into(),
        Series::new("relfrozenxid".into(), empty_txt_s.clone()).into(),
        Series::new("relminmxid".into(), empty_txt_s).into(),
        Series::new("relacl".into(), empty_txt.clone()).into(),
        Series::new("reloptions".into(), empty_txt).into(),
    ]).ok()
}


pub fn register() { registry::register(Box::new(PgClass)); }
