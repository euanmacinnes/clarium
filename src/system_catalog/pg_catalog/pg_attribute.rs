use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::system_catalog::shared::{enumerate_tables, enumerate_views, get_or_assign_table_oid, get_or_assign_view_oid};
use crate::storage::SharedStore;
use crate::tprintln;

pub struct PgAttribute;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "attrelid", coltype: ColType::Integer },
    ColumnDef { name: "attname", coltype: ColType::Text },
    ColumnDef { name: "attnum", coltype: ColType::Integer },
    ColumnDef { name: "attisdropped", coltype: ColType::Boolean },
];

impl SystemTable for PgAttribute {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_attribute" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let metas = enumerate_tables(store);
        let vmetas = enumerate_views(store);
        let mut attrelid: Vec<i32> = Vec::new();
        let mut attname: Vec<String> = Vec::new();
        let mut attnum: Vec<i32> = Vec::new();
        let mut attisdropped: Vec<bool> = Vec::new();

        for m in metas.iter() {
            let table_oid = get_or_assign_table_oid(&m.dir, &m.db, &m.schema, &m.table);
            let mut col_num = 1i32;
            for (cname, _dtype) in m.cols.iter() {
                if cname == "PRIMARY" { continue; }
                attrelid.push(table_oid);
                attname.push(cname.clone());
                attnum.push(col_num);
                attisdropped.push(false);
                col_num += 1;
            }
        }

        // Add attributes for system/user views based on registry metadata
        for v in vmetas.iter() {
            let view_oid = get_or_assign_view_oid(&v.file, &v.db, &v.schema, &v.view);
            let mut col_num = 1i32;
            // We don't have column dtypes here; pg_attribute in our subset only exposes names and ordinals
            if let Ok(text) = std::fs::read_to_string(&v.file) {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(cols) = json.get("columns").and_then(|x| x.as_array()) {
                        for c in cols.iter() {
                            if let Some(name) = c.get("name").and_then(|x| x.as_str()) {
                                attrelid.push(view_oid);
                                attname.push(name.to_string());
                                attnum.push(col_num);
                                attisdropped.push(false);
                                col_num += 1;
                            }
                        }
                    }
                }
            }
        }

        tprintln!("[loader] pg_attribute built: rows={}", attrelid.len());
        DataFrame::new(vec![
            Series::new("attrelid".into(), attrelid).into(),
            Series::new("attname".into(), attname).into(),
            Series::new("attnum".into(), attnum).into(),
            Series::new("attisdropped".into(), attisdropped).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgAttribute)); }
