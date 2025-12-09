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

        let mut constraint_oid = 20000i32;

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
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgConstraint)); }
