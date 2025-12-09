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
    // added per reconciliation
    ColumnDef { name: "atttypid", coltype: ColType::Integer },
    ColumnDef { name: "attstattarget", coltype: ColType::Integer },
    ColumnDef { name: "attlen", coltype: ColType::Integer },
    ColumnDef { name: "attndims", coltype: ColType::Integer },
    ColumnDef { name: "attcacheoff", coltype: ColType::Integer },
    ColumnDef { name: "atttypmod", coltype: ColType::Integer },
    ColumnDef { name: "attbyval", coltype: ColType::Boolean },
    ColumnDef { name: "attalign", coltype: ColType::Text },
    ColumnDef { name: "attstorage", coltype: ColType::Text },
    ColumnDef { name: "attcompression", coltype: ColType::Text },
    ColumnDef { name: "attnotnull", coltype: ColType::Boolean },
    ColumnDef { name: "atthasdef", coltype: ColType::Boolean },
    ColumnDef { name: "atthasmissing", coltype: ColType::Boolean },
    ColumnDef { name: "attidentity", coltype: ColType::Text },
    ColumnDef { name: "attgenerated", coltype: ColType::Text },
    ColumnDef { name: "attislocal", coltype: ColType::Boolean },
    ColumnDef { name: "attinhcount", coltype: ColType::Integer },
    ColumnDef { name: "attcollation", coltype: ColType::Integer },
    ColumnDef { name: "attacl", coltype: ColType::Text },
    ColumnDef { name: "attoptions", coltype: ColType::Text },
    ColumnDef { name: "attfdwoptions", coltype: ColType::Text },
    ColumnDef { name: "attmissingval", coltype: ColType::Text },
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

        let rows = attrelid.len();
        tprintln!("[loader] pg_attribute built: rows={}", rows);
        // defaults for added columns
        let zeros_i32: Vec<i32> = vec![0; rows];
        let falses: Vec<bool> = vec![false; rows];
        let empty_txt: Vec<Option<String>> = vec![None; rows];
        let empty_txt_s: Vec<String> = vec![String::new(); rows];
        let attalign: Vec<String> = vec!["i".into(); rows];
        let attstorage: Vec<String> = vec!["p".into(); rows];
        let attcompression: Vec<Option<String>> = vec![None; rows];
        let attidentity: Vec<String> = vec!["".into(); rows];
        let attgenerated: Vec<String> = vec!["".into(); rows];

        DataFrame::new(vec![
            Series::new("attrelid".into(), attrelid).into(),
            Series::new("attname".into(), attname).into(),
            Series::new("attnum".into(), attnum).into(),
            Series::new("attisdropped".into(), attisdropped).into(),
            Series::new("atttypid".into(), zeros_i32.clone()).into(),
            Series::new("attstattarget".into(), zeros_i32.clone()).into(),
            Series::new("attlen".into(), zeros_i32.clone()).into(),
            Series::new("attndims".into(), zeros_i32.clone()).into(),
            Series::new("attcacheoff".into(), zeros_i32.clone()).into(),
            Series::new("atttypmod".into(), zeros_i32.clone()).into(),
            Series::new("attbyval".into(), falses.clone()).into(),
            Series::new("attalign".into(), attalign).into(),
            Series::new("attstorage".into(), attstorage).into(),
            Series::new("attcompression".into(), attcompression).into(),
            Series::new("attnotnull".into(), falses.clone()).into(),
            Series::new("atthasdef".into(), falses.clone()).into(),
            Series::new("atthasmissing".into(), falses.clone()).into(),
            Series::new("attidentity".into(), attidentity).into(),
            Series::new("attgenerated".into(), attgenerated).into(),
            Series::new("attislocal".into(), falses.clone()).into(),
            Series::new("attinhcount".into(), zeros_i32.clone()).into(),
            Series::new("attcollation".into(), zeros_i32.clone()).into(),
            Series::new("attacl".into(), empty_txt.clone()).into(),
            Series::new("attoptions".into(), empty_txt.clone()).into(),
            Series::new("attfdwoptions".into(), empty_txt.clone()).into(),
            Series::new("attmissingval".into(), empty_txt_s).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgAttribute)); }
