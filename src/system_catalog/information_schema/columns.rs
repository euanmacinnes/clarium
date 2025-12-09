use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;
use std::path::PathBuf;

pub struct IColumns;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "table_schema", coltype: ColType::Text },
    ColumnDef { name: "table_name", coltype: ColType::Text },
    ColumnDef { name: "column_name", coltype: ColType::Text },
    ColumnDef { name: "ordinal_position", coltype: ColType::Integer },
    ColumnDef { name: "data_type", coltype: ColType::Text },
    ColumnDef { name: "is_nullable", coltype: ColType::Text },
    ColumnDef { name: "udt_name", coltype: ColType::Text },
];

impl SystemTable for IColumns {
    fn schema(&self) -> &'static str { "information_schema" }
    fn name(&self) -> &'static str { "columns" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, store: &SharedStore) -> Option<DataFrame> {
        let mut schema_col: Vec<String> = Vec::new();
        let mut table_col: Vec<String> = Vec::new();
        let mut col_name: Vec<String> = Vec::new();
        let mut ord_pos: Vec<i32> = Vec::new();
        let mut data_type: Vec<String> = Vec::new();
        let mut is_null: Vec<String> = Vec::new();
        let mut udt_name: Vec<String> = Vec::new();

        // 1) Real user tables
        let root = store.root_path();
        if let Ok(dbs) = std::fs::read_dir(&root) {
            for db_ent in dbs.flatten() {
                let db_path = db_ent.path(); if !db_path.is_dir() { continue; }
                if let Ok(sd) = std::fs::read_dir(&db_path) {
                    for schema_dir in sd.flatten() {
                        let sch_path = schema_dir.path(); if !sch_path.is_dir() { continue; }
                        let schema_name = schema_dir.file_name().to_string_lossy().to_string();
                        if schema_name.starts_with('.') { continue; }
                        if let Ok(td) = std::fs::read_dir(&sch_path) {
                            for tentry in td.flatten() {
                                let tp: PathBuf = tentry.path();
                                if tp.is_dir() {
                                    let sj = tp.join("schema.json");
                                    let mut tname = tentry.file_name().to_string_lossy().to_string();
                                    if tname.ends_with(".time") {
                                        tname.truncate(tname.len() - 5);
                                    }
                                    // Build columns either from schema.json or synthesize minimal for data-only tables
                                    let mut cols: Vec<(String, String)> = Vec::new();
                                    if sj.exists() {
                                        // parse schema.json
                                        if let Ok(text) = std::fs::read_to_string(&sj) {
                                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let mut is_time_table = false;
                                                if let Some(serde_json::Value::String(tt)) = json.get("tableType") { is_time_table = tt.eq_ignore_ascii_case("time"); }
                                                if let serde_json::Value::Object(obj) = json {
                                                    for (k, v) in obj.into_iter() {
                                                        if k == "PRIMARY" || k == "primaryKey" || k == "partitions" || k == "locks" || k == "tableType" { continue; }
                                                        if let serde_json::Value::String(s) = v {
                                                            cols.push((k, s));
                                                        } else if let serde_json::Value::Object(m) = v {
                                                            if let Some(serde_json::Value::String(t)) = m.get("type") {
                                                                cols.push((k, t.clone()));
                                                            }
                                                        }
                                                    }
                                                }
                                                // Only register _time for time tables per schema.json single source of truth
                                                if is_time_table && !cols.iter().any(|(n, _)| n == "_time") {
                                                    cols.insert(0, ("_time".into(), "int64".into()));
                                                }
                                            }
                                        }
                                    }
                                    if !cols.is_empty() {
                                        let mut ord = 1i32;
                                        for (cname, ctype) in cols {
                                            schema_col.push(schema_name.clone());
                                            table_col.push(tname.clone());
                                            col_name.push(cname);
                                            ord_pos.push(ord);
                                            let (dt, udt) = map_dtype(&ctype);
                                            data_type.push(dt.to_string());
                                            udt_name.push(udt.to_string());
                                            is_null.push("YES".to_string());
                                            ord += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2) Registry system tables
        for t in registry::all() {
            let mut ord = 1i32;
            for col in t.columns() {
                schema_col.push(t.schema().to_string());
                table_col.push(t.name().to_string());
                col_name.push(col.name.to_string());
                ord_pos.push(ord);
                let (dt, udt) = match col.coltype {
                    ColType::BigInt => ("bigint", "int8"),
                    ColType::Integer => ("integer", "int4"),
                    ColType::Boolean => ("boolean", "bool"),
                    ColType::Text => ("text", "text"),
                };
                data_type.push(dt.to_string());
                is_null.push("YES".to_string());
                udt_name.push(udt.to_string());
                ord += 1;
            }
        }

        DataFrame::new(vec![
            Series::new("table_schema".into(), schema_col).into(),
            Series::new("table_name".into(), table_col).into(),
            Series::new("column_name".into(), col_name).into(),
            Series::new("ordinal_position".into(), ord_pos).into(),
            Series::new("data_type".into(), data_type).into(),
            Series::new("is_nullable".into(), is_null).into(),
            Series::new("udt_name".into(), udt_name).into(),
        ]).ok()
    }
}

fn map_dtype(dtype: &str) -> (&'static str, &'static str) {
    match dtype.to_lowercase().as_str() {
        "int64" | "i64" => ("bigint", "int8"),
        "int32" | "i32" | "int" | "integer" => ("integer", "int4"),
        "float" | "float64" | "f64" | "double" => ("double precision", "float8"),
        "bool" | "boolean" => ("boolean", "bool"),
        "timestamp" | "datetime" => ("timestamp", "timestamp"),
        other => {
            // default to text for unknowns
            if other.eq("text") { ("text", "text") } else { ("text", "text") }
        }
    }
}

pub fn register() { registry::register(Box::new(IColumns)); }
