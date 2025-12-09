use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::system_catalog::shared::{enumerate_tables,get_or_assign_table_oid};
use crate::storage::SharedStore;
use crate::tprintln;

pub struct PgConstraint;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "conrelid", coltype: ColType::Integer },
    ColumnDef { name: "conname", coltype: ColType::Text },
    ColumnDef { name: "contype", coltype: ColType::Text },
    ColumnDef { name: "conkey", coltype: ColType::Text }, // array string like {1,2}
    ColumnDef { name: "conindid", coltype: ColType::Integer },
    // added per reconciliation
    ColumnDef { name: "connamespace", coltype: ColType::Integer },
    ColumnDef { name: "condeferrable", coltype: ColType::Boolean },
    ColumnDef { name: "condeferred", coltype: ColType::Boolean },
    ColumnDef { name: "convalidated", coltype: ColType::Boolean },
    ColumnDef { name: "contypid", coltype: ColType::Integer },
    ColumnDef { name: "conparentid", coltype: ColType::Integer },
    ColumnDef { name: "confrelid", coltype: ColType::Integer },
    ColumnDef { name: "confupdtype", coltype: ColType::Text },
    ColumnDef { name: "confdeltype", coltype: ColType::Text },
    ColumnDef { name: "confmatchtype", coltype: ColType::Text },
    ColumnDef { name: "conislocal", coltype: ColType::Boolean },
    ColumnDef { name: "coninhcount", coltype: ColType::Integer },
    ColumnDef { name: "connoinherit", coltype: ColType::Boolean },
    ColumnDef { name: "confkey", coltype: ColType::Text },
    ColumnDef { name: "conpfeqop", coltype: ColType::Text },
    ColumnDef { name: "conppeqop", coltype: ColType::Text },
    ColumnDef { name: "conffeqop", coltype: ColType::Text },
    ColumnDef { name: "conexclop", coltype: ColType::Text },
    ColumnDef { name: "conbin", coltype: ColType::Text },
];

impl SystemTable for PgConstraint {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_constraint" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }    
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let metas = enumerate_tables(store);
        let mut conrelid: Vec<i32> = Vec::new();
        let mut conname: Vec<String> = Vec::new();
        let mut contype: Vec<String> = Vec::new();
        let mut conkey: Vec<String> = Vec::new();
        let mut conindid: Vec<i32> = Vec::new();
        let mut oid: Vec<i32> = Vec::new();
        // added fields
        let mut connamespace: Vec<i32> = Vec::new();
        let mut condeferrable: Vec<bool> = Vec::new();
        let mut condeferred: Vec<bool> = Vec::new();
        let mut convalidated: Vec<bool> = Vec::new();
        let mut contypid: Vec<i32> = Vec::new();
        let mut conparentid: Vec<i32> = Vec::new();
        let mut confrelid: Vec<i32> = Vec::new();
        let mut confupdtype: Vec<String> = Vec::new();
        let mut confdeltype: Vec<String> = Vec::new();
        let mut confmatchtype: Vec<String> = Vec::new();
        let mut conislocal: Vec<bool> = Vec::new();
        let mut coninhcount: Vec<i32> = Vec::new();
        let mut connoinherit: Vec<bool> = Vec::new();
        let mut confkey: Vec<String> = Vec::new();
        let mut conpfeqop: Vec<String> = Vec::new();
        let mut conppeqop: Vec<String> = Vec::new();
        let mut conffeqop: Vec<String> = Vec::new();
        let mut conexclop: Vec<String> = Vec::new();
        let mut conbin: Vec<Option<String>> = Vec::new();

        let mut constraint_oid = 20000i32;

        // namespace OIDs mapping similar to pg_class
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
            let table_oid = get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table);
            if m.has_primary_marker {
                let mut pk_columns: Vec<i32> = Vec::new();
                for (col_idx, (cname, _dtype)) in m.cols.iter().enumerate() {
                    if cname == "_time" || cname == "PRIMARY" { continue; }
                    if cname == "id" || cname == "record_id" || cname.ends_with("_id") {
                        pk_columns.push((col_idx + 1) as i32);
                        break;
                    }
                }
                if pk_columns.is_empty() {
                    for (col_idx, (cname, _dtype)) in m.cols.iter().enumerate() {
                        if cname != "_time" && cname != "PRIMARY" {
                            pk_columns.push((col_idx + 1) as i32);
                            break;
                        }
                    }
                }
                if !pk_columns.is_empty() {
                    let conkey_str = {
                        let nums: Vec<String> = pk_columns.iter().map(|n| n.to_string()).collect();
                        format!("{{{}}}", nums.join(","))
                    };
                    conrelid.push(table_oid);
                    conname.push(format!("{}_pkey", m.table));
                    contype.push("p".to_string());
                    conkey.push(conkey_str);
                    conindid.push(0);
                    oid.push(constraint_oid);
                    // fill added columns for PK defaults
                    connamespace.push(ns_oid_for(&m.schema));
                    condeferrable.push(false);
                    condeferred.push(false);
                    convalidated.push(true);
                    contypid.push(0);
                    conparentid.push(0);
                    confrelid.push(0);
                    confupdtype.push(String::new());
                    confdeltype.push(String::new());
                    confmatchtype.push(String::new());
                    conislocal.push(true);
                    coninhcount.push(0);
                    connoinherit.push(false);
                    confkey.push("{}".to_string());
                    conpfeqop.push("{}".to_string());
                    conppeqop.push("{}".to_string());
                    conffeqop.push("{}".to_string());
                    conexclop.push("{}".to_string());
                    conbin.push(None);
                    constraint_oid += 1;
                }
            }
        }

        tprintln!("[loader] pg_constraint built: rows={}", oid.len());
        DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("conrelid".into(), conrelid).into(),
            Series::new("conname".into(), conname).into(),
            Series::new("contype".into(), contype).into(),
            Series::new("conkey".into(), conkey).into(),
            Series::new("conindid".into(), conindid).into(),
            Series::new("connamespace".into(), connamespace).into(),
            Series::new("condeferrable".into(), condeferrable).into(),
            Series::new("condeferred".into(), condeferred).into(),
            Series::new("convalidated".into(), convalidated).into(),
            Series::new("contypid".into(), contypid).into(),
            Series::new("conparentid".into(), conparentid).into(),
            Series::new("confrelid".into(), confrelid).into(),
            Series::new("confupdtype".into(), confupdtype).into(),
            Series::new("confdeltype".into(), confdeltype).into(),
            Series::new("confmatchtype".into(), confmatchtype).into(),
            Series::new("conislocal".into(), conislocal).into(),
            Series::new("coninhcount".into(), coninhcount).into(),
            Series::new("connoinherit".into(), connoinherit).into(),
            Series::new("confkey".into(), confkey).into(),
            Series::new("conpfeqop".into(), conpfeqop).into(),
            Series::new("conppeqop".into(), conppeqop).into(),
            Series::new("conffeqop".into(), conffeqop).into(),
            Series::new("conexclop".into(), conexclop).into(),
            Series::new("conbin".into(), conbin).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgConstraint)); }
