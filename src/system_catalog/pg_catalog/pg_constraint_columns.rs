use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::system_catalog::shared::{enumerate_tables,get_or_assign_table_oid};
use crate::storage::SharedStore;
use crate::tprintln;

pub struct PgConstraintColumns;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "conrelid", coltype: ColType::Integer },
    ColumnDef { name: "conname", coltype: ColType::Text },
    ColumnDef { name: "contype", coltype: ColType::Text },
    ColumnDef { name: "attnum", coltype: ColType::Integer },
    ColumnDef { name: "ord", coltype: ColType::Integer },
    ColumnDef { name: "conindid", coltype: ColType::Integer },
];

impl SystemTable for PgConstraintColumns {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_constraint_columns" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let metas = enumerate_tables(store);
        let mut oid: Vec<i32> = Vec::new();
        let mut conrelid: Vec<i32> = Vec::new();
        let mut conname: Vec<String> = Vec::new();
        let mut contype: Vec<String> = Vec::new();
        let mut attnum: Vec<i32> = Vec::new();
        let mut ord: Vec<i32> = Vec::new();
        let mut conindid: Vec<i32> = Vec::new();

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
                    let cname_val = format!("{}_pkey", m.table);
                    for (position, col_num) in pk_columns.iter().enumerate() {
                        oid.push(constraint_oid);
                        conrelid.push(table_oid);
                        conname.push(cname_val.clone());
                        contype.push("p".to_string());
                        attnum.push(*col_num);
                        ord.push((position + 1) as i32);
                        conindid.push(0);
                    }
                    constraint_oid += 1;
                }
            }
        }

        tprintln!("[loader] pg_constraint_columns built: rows={}", oid.len());
        DataFrame::new(vec![
            Series::new("oid".into(), oid).into(),
            Series::new("conrelid".into(), conrelid).into(),
            Series::new("conname".into(), conname).into(),
            Series::new("contype".into(), contype).into(),
            Series::new("attnum".into(), attnum).into(),
            Series::new("ord".into(), ord).into(),
            Series::new("conindid".into(), conindid).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgConstraintColumns)); }
