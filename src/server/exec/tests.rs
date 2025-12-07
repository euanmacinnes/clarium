// Unit tests for server::exec module moved out of exec.rs to keep source concise.

// Centralized UDF initialization for all tests
pub(super) mod udf_common {
    use crate::scripts::{init_script_registry_once, ScriptRegistry, ScriptMeta, ScriptKind, load_global_default_scripts};
    use polars::prelude::DataType;
    use std::sync::Once;

    static INIT: Once = Once::new();

    /// Initialize all UDF functions used across all test files in a single call.
    /// This ensures the script registry is only ever initialized once.
    pub fn init_all_test_udfs() {
        INIT.call_once(|| {
            let reg = ScriptRegistry::new().unwrap();
            
            // Load global default scripts from scripts/scalars directory
            let _ = load_global_default_scripts(&reg);
            
            // Scalar UDFs
            reg.load_script_text("is_pos", "function is_pos(x) if x==nil then return false end return x>0 end").unwrap();
            reg.set_meta("is_pos", ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::Boolean], nullable: true, version: 0 });
            
            reg.load_script_text("dbl", "function dbl(x) if x==nil then return 0 end return x*2 end").unwrap();
            reg.set_meta("dbl", ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::Int64], nullable: true, version: 0 });
            
            reg.load_script_text("hello", "function hello(x) return 'hi:'..tostring(x) end").unwrap();
            reg.set_meta("hello", ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::String], nullable: true, version: 0 });
            
            reg.load_script_text("err_if_neg", "function err_if_neg(x) if x==nil then error('nil') end if x<0 then error('neg') end return x end").unwrap();
            reg.set_meta("err_if_neg", ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::Int64], nullable: true, version: 0 });
            
            reg.load_script_text("split2", "function split2(x) if x==nil then return {nil,nil} end return {x, x+1} end").unwrap();
            reg.set_meta("split2", ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::Int64, DataType::Int64], nullable: true, version: 0 });
            
            reg.load_script_text("echo2", r#"
                function echo2(a,b)
                    return { tostring(a), tostring(b) }
                end
            "#).unwrap();
            reg.set_meta("echo2", ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::String, DataType::String], nullable: true, version: 0 });
            
            // Aggregate UDFs
            reg.load_script_text("sum_plus", r#"
                function sum_plus(arr)
                    local s = 0
                    for i=1,#arr do local v = arr[i]; if v ~= nil then s = s + v end end
                    return s + 1
                end
            "#).unwrap();
            reg.set_meta("sum_plus", ScriptMeta { kind: ScriptKind::Aggregate, returns: vec![DataType::Int64], nullable: true, version: 0 });
            
            reg.load_script_text("minmax", r#"
                function minmax(arr)
                    local mn, mx = nil, nil
                    for i=1,#arr do local v = arr[i]; if v ~= nil then if mn==nil or v<mn then mn=v end; if mx==nil or v>mx then mx=v end end end
                    return {mn, mx}
                end
            "#).unwrap();
            reg.set_meta("minmax", ScriptMeta { kind: ScriptKind::Aggregate, returns: vec![DataType::Int64, DataType::Int64], nullable: true, version: 0 });
            
            reg.load_script_text("agg_err_if_bad", r#"
                function agg_err_if_bad(arrk, arrv)
                    if #arrk > 0 and (arrk[1] == 'bad' or tostring(arrk[1]) == 'g0') then error('bad group') end
                    local s = 0
                    for i=1,#arrv do local v = arrv[i]; if v ~= nil then s = s + v end end
                    return s
                end
            "#).unwrap();
            reg.set_meta("agg_err_if_bad", ScriptMeta { kind: ScriptKind::Aggregate, returns: vec![DataType::Int64], nullable: true, version: 0 });
            
            reg.load_script_text("argtypes", r#"
                function argtypes(arrk, arrv)
                    local function tp(v)
                        local t = type(v)
                        return t
                    end
                    return { tp(arrk[1]), tp(arrv[1]) }
                end
            "#).unwrap();
            reg.set_meta("argtypes", ScriptMeta { kind: ScriptKind::Aggregate, returns: vec![DataType::String, DataType::String], nullable: true, version: 0 });


            
            // Initialize the global registry once
            init_script_registry_once(reg);
        });
    }
}

mod ambiguous_names_tests;
mod ann_no_limit_parity_tests;
mod ann_order_by_tests;
mod ann_topk_heap_tests;
mod cast_and_regclass_tests;
mod cast_followups_tests;
mod clause_errors_tests; // File not found
mod cte_tests;
mod dbeaver_tests;
mod deadlock_tests;
mod delete_tests;
mod end_to_end_planning_tests;
mod exists_tests;
mod expressive_exec_tests;
mod exec_show_tests;
mod fixtures;
mod graph_catalog_tests;
mod graph_tvf_neighbors_tests;
mod graph_tvf_paths_tests;
mod graphstore_gc_tests;
mod graphstore_neighbors_tests;
mod group_by_tests;
mod having_tests;
mod having_tests2;
mod insert_tests;
mod intermittent_failure_test;
mod join_inner_tests;
mod join_outer_tests;
mod like_tests;
mod match_rewrite_tests;
mod match_view_tests;
mod metric_semantics_tests;
mod nested_exists_tests;
mod normalize_tests;
mod order_mode_tests;
mod perf_tests;
mod perf_tests_month;
mod pg_catalog_tests;
mod primary_key_tests;
mod quick_checks_udf;
mod raw_tests;
mod row_id_mapping_tests;
mod rolling_tests;
mod session_defaults_tests;
mod show_describe_tests;
mod slice_blend_tests;
mod slice_manual_tests;
mod slice_tests;
mod slice_tests_more;
mod slice_union_tests;
mod slice_union_where_tests;
mod stdev_tests;
mod stress_concurrency;
mod stress_concurrency_no_udf;
mod string_slice_tests;
mod system_table_tests;
mod test_views;
mod tests_udf;
mod time_table_by_tests;
mod udf_lua_direct_tests;
mod udf_startup_tests;
mod udf_vectors_tests;
mod udf_vectors_simple_tests;
mod union_select_tests;
mod unnamed_and_join_tests;
mod vector_column_type_tests;
mod vector_hnsw_smoke;
mod vector_index_ddl_tests;
mod vector_index_modes_tests;
mod vector_index_runtime_tests;
mod vector_tvf_tests;
mod vector_utils_tests;
mod views_with_tvfs_tests;
mod where_tests;
mod windowing_tests;

mod partitioned_table_tests {
    use super::super::{run_select};
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn test_regular_partitioned_table_select_and_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let table = "pdb/public/rtab"; // regular table (no .time)
        // Create table via exec path semantics (direct store for sync test)
        store.create_table(table).unwrap();
        store.set_table_metadata(table, None, Some(vec!["region".to_string()])).unwrap();
        // Insert rows alternating partitions
        let mut recs: Vec<Record> = Vec::new();
        for i in 0..20 {
            let mut m = serde_json::Map::new();
            m.insert("region".into(), json!(if i % 3 == 0 { "north" } else if i % 3 == 1 { "south" } else { "east" }));
            m.insert("v".into(), json!(i as i64));
            recs.push(Record { _time: 1_700_000_000_000 + i as i64, sensors: m });
        }
        store.write_records(table, &recs).unwrap();
        // Verify multiple parquet files created (>= number of partitions used)
        let dir = {
            let mut p = shared.root_path().to_path_buf();
            // table path uses forward slashes; convert to OS-specific path under root
            let rel = table.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str());
            p.push(rel);
            p
        };
        let mut count = 0usize;
        for e in std::fs::read_dir(&dir).unwrap() {
            let p = e.unwrap().path();
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name.starts_with("data-") && name.ends_with(".parquet") { count += 1; }
            }
        }
        assert!(count >= 3, "expected >=3 parquet files for partitions, found {}", count);
        // Query should see all rows on regular table; COUNT(v) over all rows
        let qtext = format!("SELECT COUNT(v) FROM {}", table);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 1);
        let cnt = df.column("COUNT(v)").unwrap().i64().unwrap().get(0).unwrap();
        assert_eq!(cnt, recs.len() as i64);
    }
}


mod group_by_notnull_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn test_group_by_notnull_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "db_gbnn.time";
        let base: i64 = 1_700_000_000_000;
        let mut recs: Vec<Record> = Vec::new();
        // 900: null a
        recs.push(Record { _time: base - 100, sensors: serde_json::Map::new() });
        // 1000: a=1
        let mut m1 = serde_json::Map::new(); m1.insert("a".into(), json!(1));
        recs.push(Record { _time: base, sensors: m1 });
        // 1100: null
        recs.push(Record { _time: base + 100, sensors: serde_json::Map::new() });
        // 1400: null
        recs.push(Record { _time: base + 400, sensors: serde_json::Map::new() });
        // 1500: a=2
        let mut m2 = serde_json::Map::new(); m2.insert("a".into(), json!(2));
        recs.push(Record { _time: base + 500, sensors: m2 });
        // 1600: null
        recs.push(Record { _time: base + 600, sensors: serde_json::Map::new() });
        // 2000: a=2
        let mut m2b = serde_json::Map::new(); m2b.insert("a".into(), json!(2));
        recs.push(Record { _time: base + 1000, sensors: m2b });
        // 2100: null
        recs.push(Record { _time: base + 1100, sensors: serde_json::Map::new() });
        // 2200: a=3
        let mut m3 = serde_json::Map::new(); m3.insert("a".into(), json!(3));
        recs.push(Record { _time: base + 1200, sensors: m3 });
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT COUNT(a) FROM {} GROUP BY a NOTNULL", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        // Expect three segments starting at base, base+500, base+1200
        assert_eq!(df.height(), 3);
        let starts = df.column("_start_time").unwrap().i64().unwrap().into_no_null_iter().collect::<Vec<i64>>();
        assert_eq!(starts, vec![base, base+500, base+1200]);
        // And counts per segment: 1, 2, 1 non-null values
        let counts = df.column("COUNT(a)").unwrap().i64().unwrap().into_no_null_iter().collect::<Vec<i64>>();
        assert_eq!(counts, vec![1, 2, 1]);
    }
}

mod date_func_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};

    #[test]
    fn test_datepart_and_dateadd_datediff() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "db_date.time";
        // Two records on Jan 1, 2025 at 00:00:00 and Jan 2, 2025
        let t1: i64 = 1_735_680_000_000; // approximate ms for 2025-01-01T00:00:00Z
        let t2: i64 = t1 + 86_400_000; // +1 day
        let recs = vec![
            Record { _time: t1, sensors: serde_json::Map::new() },
            Record { _time: t2, sensors: serde_json::Map::new() },
        ];
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        // DATEADD(day, 1, _time) should add one day
        let qtext = format!("SELECT DATEADD(day, 1, _time) AS next FROM {}", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 2);
        let next = df.column("next").unwrap().f64().unwrap();
        assert_eq!(next.get(0).unwrap() as i64, t1 + 86_400_000);
        // DATEDIFF(day, t2, t1) should be 1
        let qtext2 = format!("SELECT DATEDIFF(day, 2025-01-02T00:00:00Z, 2025-01-01T00:00:00Z) AS diff FROM {}", db);
        let q2 = match query::parse(&qtext2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df2 = run_select(&shared, &q2).unwrap();
        let v = df2.column("diff").unwrap().f64().unwrap().get(0).unwrap();
        assert_eq!(v as i64, 1);
    }
}


mod project_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn test_projection_and_agg_mix() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "db_proj.time";
        // times 0..4 seconds
        let base: i64 = 1_000_000_000_000;
        let mut recs = Vec::new();
        for i in 0..5 {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), json!((i*10) as f64));
            m.insert("w".into(), json!(i as i64));
            recs.push(Record { _time: base + i*1000, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT _time, v, AVG(w) FROM {} BY 2s WHERE _time BETWEEN {} AND {}", db, base, base + 4*1000);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 3);
        let cols = df.get_column_names();
        assert!(cols.iter().any(|c| c.as_str() == "_time"));
        assert!(cols.iter().any(|c| c.as_str() == "v"));
        assert!(cols.iter().any(|c| c.as_str() == "AVG(w)"));
    }
}


mod error_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn test_invalid_group_by_and_by_together() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "db_err.time";
        let base: i64 = 1_700_000_000_000;
        let mut recs = Vec::new();
        for i in 0..10 {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), json!(i as i64));
            recs.push(Record { _time: base + i*1000, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        // Invalid query: BY and GROUP BY together
        let qtext = format!("SELECT AVG(v) FROM {} BY 1s GROUP BY v", db);
        // Allow either parse-time rejection or executor-time rejection
        let q = match query::parse(&qtext) {
            Ok(Command::Select(q)) => q,
            _ => return, // parser rejected as expected
        };
        let err = run_select(&shared, &q).err();
        assert!(err.is_some());
    }

    #[test]
    fn test_having_without_agg_on_projection() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "db_err2.time";
        let mut recs = Vec::new();
        for i in 0..5 {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), json!(i as i64));
            recs.push(Record { _time: 1_000 + i*1000, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT v FROM {} HAVING v > 2", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let err = run_select(&shared, &q).err();
        assert!(err.is_some());
    }
}

mod time_label_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn test_previous_label_shift_and_delta() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "db_label.time";
        let base: i64 = 1_700_000_000_000;
        let mut recs = Vec::new();
        for i in 0..5 {
            let mut m = serde_json::Map::new();
            m.insert("label".into(), json!(format!("L{}", i)));
            recs.push(Record { _time: base + i*1000, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT _time, label, DATEADD(MINUTE, 5, _time) FROM {}", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert!(df.height() >= 1);
        let cols = df.get_column_names();
        assert!(cols.iter().any(|c| c.as_str() == "_time"));
        assert!(cols.iter().any(|c| c.contains("DATEADD")));
    }
}

mod sourceless_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::SharedStore;

    #[test]
    fn test_select_one_sourceless() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = "SELECT 1";
        let q = match query::parse(qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 1);
        let col_name = df.get_column_names()[0].clone();
        let s = df.column(&col_name).unwrap();
        if let Ok(ca) = s.i64() { assert_eq!(ca.get(0), Some(1)); }
        else if let Ok(ca) = s.f64() { assert!((ca.get(0).unwrap() - 1.0).abs() < 1e-9); }
        else { panic!("Unexpected dtype for SELECT 1 result"); }
    }
}




// New tests for regular (non-time) tables
mod regular_table_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore};
    use polars::prelude::*;

    #[test]
    fn test_regular_table_select_and_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "regular/ns/users"; // no .time suffix
        store.create_table(db).unwrap();
        // Build a regular table DataFrame with a primary key-like column
        let ids = Series::new("id".into(), vec![1i64,2,3]);
        let names = Series::new("name".into(), vec!["a","b","c"]);
        let created = Series::new("created".into(), vec![1700000000000i64, 1700000001000, 1700000002000]);
        let df = DataFrame::new(vec![ids.into(), names.into(), created.into()]).unwrap();
        store.rewrite_table_df(db, df.clone()).unwrap();
        // Ensure a single data.parquet exists
        let dir = store.root_path().join(db.replace('/', std::path::MAIN_SEPARATOR.to_string().as_str()));
        let entries = std::fs::read_dir(dir).unwrap();
        let mut has_single = false; let mut has_chunk = false;
        for e in entries {
            let p = e.unwrap().path();
            let n = p.file_name().unwrap().to_string_lossy().to_string();
            if n == "data.parquet" { has_single = true; }
            if n.starts_with("data-") { has_chunk = true; }
        }
        assert!(has_single);
        assert!(!has_chunk);
        // Query: select subset and filter
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT id, name FROM {} WHERE id >= 2", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let out = run_select(&shared, &q).unwrap();
        assert_eq!(out.height(), 2);
        let cols = out.get_column_names();
        assert!(cols.iter().any(|c| c.as_str()=="id"));
        assert!(cols.iter().any(|c| c.as_str()=="name"));
    }
}


mod select_wildcard_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn select_star_time_no_by() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/wild.time";
        let t0: i64 = 1_700_000_000_000;
        let recs = vec![
            {
                let mut m = serde_json::Map::new();
                m.insert("v".into(), json!(1.0));
                m.insert("s".into(), json!("abc"));
                Record { _time: t0, sensors: m }
            },
            {
                let mut m = serde_json::Map::new();
                m.insert("v".into(), json!(2.0));
                m.insert("s".into(), json!("def"));
                Record { _time: t0 + 1000, sensors: m }
            },
        ];
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT * FROM {} WHERE _time BETWEEN {} AND {}", db, t0, t0 + 1000);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        let cols = df.get_column_names();
        assert!(cols.iter().any(|c| c.as_str() == "_time"));
        assert!(cols.iter().any(|c| c.as_str() == "v"));
        assert!(cols.iter().any(|c| c.as_str() == "s"));
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn select_star_time_with_by_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/wild2.time";
        let t0: i64 = 1_700_000_000_000;
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(1.0));
        let rec = Record { _time: t0, sensors: m };
        store.write_records(db, &vec![rec]).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT *, COUNT(v) FROM {} BY 1m WHERE _time BETWEEN {} AND {}", db, t0, t0 + 60_000);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let err = run_select(&shared, &q).err();
        assert!(err.is_some(), "expected error when using * with BY/GROUP/ROLLING");
    }
}

// Unit tests for server::exec module moved out of exec.rs to keep source concise.


mod order_limit_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;
    use polars::prelude::*;

    #[test]
    fn limit_without_order_by_time_series() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/ol_no_order.time";
        let t0: i64 = 1_700_000_000_000;
        let recs = vec![
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(1.0)); Record{ _time: t0, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(2.0)); Record{ _time: t0+1000, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(3.0)); Record{ _time: t0+2000, sensors: m }},
        ];
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT _time, v FROM {} WHERE _time BETWEEN {} AND {} LIMIT 2", db, t0, t0+3000);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        assert_eq!(q.limit, Some(2));
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 2);
        let v_last = df.column("v").unwrap().f64().unwrap().get(1).unwrap();
        assert_eq!(v_last, 2.0);
    }

    #[test]
    fn negative_limit_returns_last_rows_time_series() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/ol_negative.time";
        let t0: i64 = 1_700_000_000_000;
        let recs = vec![
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(1.0)); Record{ _time: t0, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(2.0)); Record{ _time: t0+1000, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(3.0)); Record{ _time: t0+2000, sensors: m }},
        ];
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT _time, v FROM {} WHERE _time BETWEEN {} AND {} LIMIT -2", db, t0, t0+3000);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        assert_eq!(q.limit, Some(-2));
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 2);
        let vs: Vec<f64> = df.column("v").unwrap().f64().unwrap().into_no_null_iter().collect();
        assert_eq!(vs, vec![2.0, 3.0]);
    }

    #[test]
    fn no_where_limit_time_series() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/ol_simple.time";
        let t0: i64 = 1_700_000_000_000;
        let recs = vec![
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(1.0)); Record{ _time: t0, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(2.0)); Record{ _time: t0+1000, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(3.0)); Record{ _time: t0+2000, sensors: m }},
        ];
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT _time, v FROM {} LIMIT 2", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        assert_eq!(q.limit, Some(2));
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn order_by_limit_regular_table() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/people";
        // create simple regular table
        let ids = Series::new("id".into(), vec![1i64, 2, 3]);
        let names = Series::new("name".into(), vec!["alice", "bob", "carol"]);
        let df = DataFrame::new(vec![ids.into(), names.into()]).unwrap();
        // Write the DataFrame into the regular table
        store.rewrite_table_df(db, df).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT id, name FROM {} ORDER BY name DESC LIMIT 2", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 2);
        let names: Vec<String> = df.column("name").unwrap().str().unwrap().into_iter().map(|o| o.unwrap().to_string()).collect();
        assert_eq!(names, vec!["carol".to_string(), "bob".to_string()]);
    }

    #[test]
    fn order_by_limit_time_series() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/ol.time";
        let t0: i64 = 1_700_000_000_000;
        let recs = vec![
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(1.0)); Record{ _time: t0, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(3.0)); Record{ _time: t0+1000, sensors: m }},
            { let mut m = serde_json::Map::new(); m.insert("v".into(), json!(2.0)); Record{ _time: t0+2000, sensors: m }},
        ];
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT _time, v FROM {} WHERE _time BETWEEN {} AND {} ORDER BY v DESC LIMIT 1", db, t0, t0+3000);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 1);
        let v = df.column("v").unwrap().f64().unwrap().get(0).unwrap();
        assert_eq!(v, 3.0);
    }

    #[test]
    fn order_by_unknown_column_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/ol2.time";
        let t0: i64 = 1_700_000_000_000;
        let recs = vec![{ let mut m = serde_json::Map::new(); m.insert("v".into(), json!(1.0)); Record{ _time: t0, sensors: m }}];
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtext = format!("SELECT _time, v FROM {} ORDER BY not_a_col", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let err = run_select(&shared, &q).err();
        assert!(err.is_some());
    }
}



mod by_slice_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn by_slice_basic_two_intervals() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/by_slice_demo.time";
        let t0: i64 = 1_800_000_000_000;
        // 0..59s values 1.0, 60..119s values 2.0, 120..179s values 3.0
        let mut recs: Vec<Record> = Vec::new();
        for i in 0..180 {
            let mut m = serde_json::Map::new();
            let v = if i < 60 {1.0} else if i < 120 {2.0} else {3.0};
            m.insert("v".into(), json!(v));
            recs.push(Record{ _time: t0 + i*1000, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        // Two intervals: [0,60s) and [120s,180s)
        let s1 = t0; let e1 = t0 + 60_000; let s2 = t0 + 120_000; let e2 = t0 + 180_000;
        let q = format!("SELECT AVG(v) FROM {} BY SLICE( USING ({}, {}) UNION ({}, {}) )", db, s1, e1, s2, e2);
        let q = match query::parse(&q).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 2);
        let times: Vec<i64> = df.column("_time").unwrap().i64().unwrap().into_no_null_iter().collect();
        assert_eq!(times, vec![s1, s2]);
        let av = df.column("AVG(v)").unwrap().f64().unwrap();
        assert!((av.get(0).unwrap() - 1.0).abs() < 1e-9);
        assert!((av.get(1).unwrap() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn by_slice_having_and_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/by_slice_demo2.time";
        let base: i64 = 1_801_000_000_000;
        let mut recs: Vec<Record> = Vec::new();
        for i in 0..10 {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), json!(i as f64));
            recs.push(Record{ _time: base + i*1000, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtxt = format!("SELECT COUNT(v) FROM {} BY SLICE( USING ({}, {}) ) HAVING COUNT(v) > 5", db, base, base+10_000);
        let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 1);
        // GROUP BY together with BY SLICE should error
        let qerr = format!("SELECT COUNT(v) FROM {} BY SLICE( USING ({}, {}) ) GROUP BY v", db, base, base+10_000);
        let parsed = query::parse(&qerr);
        if let Ok(Command::Select(q2)) = parsed {
            let err = run_select(&shared, &q2).err();
            assert!(err.is_some());
        }
    }
}




mod by_slice_labels_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn by_slice_surfaces_labels_into_select() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/by_slice_labels.time";
        let t0: i64 = 1_805_000_000_000;
        // Create some simple data across ranges to aggregate over
        let mut recs: Vec<Record> = Vec::new();
        for i in 0..120 {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), json!(if i < 60 { 1.0 } else { 2.0 }));
            recs.push(Record { _time: t0 + i*1000, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let s1 = t0; let e1 = t0 + 60_000; let s2 = t0 + 60_000; let e2 = t0 + 120_000;
        // BY SLICE with manual labeled rows and USING LABELS(...)
        let qtxt = format!(
            "SELECT AVG(v) FROM {} BY SLICE( USING LABELS(machine, kind) (({}, {}, machine:='M1', kind:='X'), ({}, {}, machine:='M2', kind:='Y')) )",
            db, s1, e1, s2, e2
        );
        let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        // Expect two rows, with label columns surfaced
        assert_eq!(df.height(), 2);
        let cols = df.get_column_names();
        assert!(cols.iter().any(|c| c.as_str() == "machine"));
        assert!(cols.iter().any(|c| c.as_str() == "kind"));
        let mach = df.column("machine").unwrap().str().unwrap();
        let kind = df.column("kind").unwrap().str().unwrap();
        assert_eq!(mach.get(0).unwrap(), "M1");
        assert_eq!(kind.get(0).unwrap(), "X");
        assert_eq!(mach.get(1).unwrap(), "M2");
        assert_eq!(kind.get(1).unwrap(), "Y");
    }
}

mod complex_select_integration_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;
    use std::time::Instant;

    // Helper to build an interval (start,end) in ms relative to base
    fn ms(base: i64, sec: i64) -> i64 { base + sec * 1000 }

    #[test]
    fn test_complex_by_slice_select_with_where_union_intersect_having_order_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        // Main data table with values across 10 minutes at 1s cadence
        let main = "clarium/public/ci_main.time";
        let t0: i64 = 1_906_000_000_000; // arbitrary epoch
        let mut recs: Vec<Record> = Vec::new();
        // device alternates M1/M2, kind alternates A/B, w increases, v pattern
        for i in 0..600 { // 10 minutes
            let mut m = serde_json::Map::new();
            let v = if i % 120 < 60 { 1.0 } else { 5.0 }; // lower then higher blocks
            let w = (i % 50) as i64 + 1;
            let device = if i % 2 == 0 { "M1" } else { "M2" };
            let kind = if i % 3 == 0 { "A" } else { "B" };
            m.insert("v".into(), json!(v));
            m.insert("w".into(), json!(w));
            m.insert("device".into(), json!(device));
            m.insert("kind".into(), json!(kind));
            recs.push(Record { _time: t0 + i*1000, sensors: m });
        }
        store.write_records(main, &recs).unwrap();

        // Slice table 1 (maintenance) with label column kind (pre-filtered to kind='A')
        let s1 = "clarium/public/ci_maint.time";
        let mut s1recs: Vec<Record> = Vec::new();
        // interval: [30s, 180s] kind='A'
        {
            let mut m = serde_json::Map::new();
            m.insert("_start_date".into(), json!(ms(t0, 30)));
            m.insert("_end_date".into(), json!(ms(t0, 180)));
            m.insert("kind".into(), json!("A"));
            s1recs.push(Record { _time: ms(t0, 30), sensors: m });
        }
        store.write_records(s1, &s1recs).unwrap();

        // Slice table 2 (downtime) with a reason column; we'll INTERSECT reason='power'
        let s2 = "clarium/public/ci_down.time";
        let mut s2recs: Vec<Record> = Vec::new();
        for (start_s, end_s, reason) in [(60, 200, "power"), (300, 360, "net")] {
            let mut m = serde_json::Map::new();
            m.insert("_start_date".into(), json!(ms(t0, start_s)));
            m.insert("_end_date".into(), json!(ms(t0, end_s)));
            m.insert("reason".into(), json!(reason));
            s2recs.push(Record { _time: ms(t0, start_s), sensors: m });
        }
        store.write_records(s2, &s2recs).unwrap();

        let shared = SharedStore::new(tmp.path()).unwrap();
        // Complex BY SLICE plan:
        // USING LABELS(machine, knd) s1 WHERE kind='A' LABEL(device, kind)
        // UNION (manual window)
        // INTERSECT s2 (no WHERE filter as string WHERE is tested elsewhere)
        // HAVING multiple conditions on aggregates
        // ORDER BY machine ASC, AVG(v) DESC LIMIT -1 (last row after ordering)
        let manual_start = ms(t0, 400); // overlaps with some ranges
        let manual_end = ms(t0, 480);
        let qtxt = format!(
            "SELECT AVG(v) AS avg_v, SUM(w) AS sum_w FROM {} \
             BY SLICE( USING LABELS(machine, knd) {} LABEL('M1','A') \
             UNION ({}, {}, machine:='MX', knd:='X') \
             INTERSECT {} )",
            main, s1, manual_start, manual_end, s2
        );
        let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let t0i = Instant::now();
        let df = run_select(&shared, &q).unwrap();
        let elapsed = t0i.elapsed();

        // We expect at least one row after the slice operations
        assert!(df.height() >= 1);
        eprintln!("complex_select_by_slice: rows={}, cols={:?}, elapsed_ms={}", df.height(), df.get_column_names(), elapsed.as_millis());
    }

    #[test]
    fn test_nested_by_slice_with_labels_and_having() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let main = "clarium/public/ci_main2.time";
        let t0: i64 = 1_906_100_000_000;
        // Create 6 minutes of per-second data with two phases in v
        let mut recs: Vec<Record> = Vec::new();
        for i in 0..360 {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), json!(if i < 180 { 2.0 } else { 4.0 }));
            m.insert("device".into(), json!(if i % 2 == 0 { "M1" } else { "M2" }));
            recs.push(Record { _time: t0 + i*1000, sensors: m });
        }
        store.write_records(main, &recs).unwrap();
        // Build nested slice sources via tables
        let a = "clarium/public/ci_a.time"; // [0,180s]
        let b = "clarium/public/ci_b.time"; // [120,360s]
        let mut arecs: Vec<Record> = Vec::new();
        let mut brecs: Vec<Record> = Vec::new();
        arecs.push(Record { _time: t0, sensors: serde_json::Map::from_iter(vec![
            ("_start_date".into(), json!(t0)),
            ("_end_date".into(), json!(t0 + 180_000)),
            ("lab".into(), json!("A")),
        ])});
        brecs.push(Record { _time: t0 + 120_000, sensors: serde_json::Map::from_iter(vec![
            ("_start_date".into(), json!(t0 + 120_000)),
            ("_end_date".into(), json!(t0 + 360_000)),
            ("lab".into(), json!("B")),
        ])});
        store.write_records(a, &arecs).unwrap();
        store.write_records(b, &brecs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        // BY SLICE with nested plan: USING a UNION SLICE(USING b INTERSECT (150s, 330s,'N'))
        let s_manual1 = t0 + 150_000; let e_manual1 = t0 + 330_000;
        let qtxt = format!(
            "SELECT AVG(v) AS av, COUNT(v) AS cnt FROM {} BY SLICE( \
               USING LABELS(label) {} LABEL(lab) \
               UNION SLICE( USING LABELS(label) {} LABEL(lab) INTERSECT ({}, {}, label:='N') ) \
            ) HAVING av >= 2 AND cnt > 0 ORDER BY av DESC LIMIT 1",
            main, a, b, s_manual1, e_manual1
        );
        let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let t0i = Instant::now();
        let df = run_select(&shared, &q).unwrap();
        let elapsed = t0i.elapsed();
        assert_eq!(df.height(), 1);
        let cols = df.get_column_names();
        assert!(cols.iter().any(|c| c.as_str() == "label"));
        assert!(cols.iter().any(|c| c.as_str() == "av"));
        assert!(cols.iter().any(|c| c.as_str() == "cnt"));
        let av = df.column("av").unwrap().f64().unwrap().get(0).unwrap();
        assert!(av >= 2.0);
        eprintln!("nested_by_slice: rows={}, elapsed_ms={}", df.height(), elapsed.as_millis());
    }

    #[test]
    fn test_demo_like_perf_complex_by_slice() {
        // A larger dataset to emulate demo-like behavior but bounded to keep CI fast
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let main = "clarium/public/ci_demo_like.time";
        let t0: i64 = 1_907_000_000_000;
        // 2 hours of per-second data (~7200 rows)
        let mut recs: Vec<Record> = Vec::with_capacity(7200);
        for i in 0..7200 {
            let mut m = serde_json::Map::new();
            let phase = (i / 900) % 4; // 15-min phases
            let v = match phase { 0 => 1.0, 1 => 2.0, 2 => 3.5, _ => 5.0 };
            m.insert("v".into(), json!(v));
            m.insert("device".into(), json!(if i % 2 == 0 { "M1" } else { "M2" }));
            recs.push(Record { _time: t0 + i*1000, sensors: m });
        }
        store.write_records(main, &recs).unwrap();
        // Build two slice sources: first half hour and last half hour
        let s_tbl = "clarium/public/ci_demo_slice.time";
        let intervals = vec![(0, 1800), (5400, 7200)];
        let mut s_recs: Vec<Record> = Vec::new();
        for (s,e) in intervals {
            let mut m = serde_json::Map::new();
            m.insert("_start_date".into(), json!(t0 + s*1000));
            m.insert("_end_date".into(), json!(t0 + e*1000));
            m.insert("region".into(), json!(if s==0 { "north" } else { "south" }));
            s_recs.push(Record { _time: t0 + s*1000, sensors: m });
        }
        store.write_records(s_tbl, &s_recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let qtxt = format!(
            "SELECT AVG(v) AS av, COUNT(v) AS cnt FROM {} BY SLICE( USING LABELS(region) {} LABEL(region) ) ORDER BY av DESC LIMIT 2",
            main, s_tbl
        );
        let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let t0i = Instant::now();
        let df = run_select(&shared, &q).unwrap();
        let elapsed = t0i.elapsed();
        assert_eq!(df.height(), 2);
        let avs: Vec<f64> = df.column("av").unwrap().f64().unwrap().into_no_null_iter().collect();
        assert!(avs[0] >= avs[1]);
        eprintln!("demo_like_complex_by_slice: rows={}, elapsed_ms={}", df.height(), elapsed.as_millis());
    }
}


mod where_literal_tests {
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn where_supports_string_int_float_datetime_literals() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let db = "clarium/public/db_where.time";
        let base: i64 = 1_700_000_000_000; // ~2023-11-14
        let mut recs: Vec<Record> = Vec::new();
        // times: base, base+1000, base+2000
        let reasons = ["power", "net", "power"];
        let vals = [1.5_f64, 2.2_f64, 0.5_f64];
        for i in 0..3 {
            let mut sensors = serde_json::Map::new();
            sensors.insert("reason".into(), json!(reasons[i as usize]));
            sensors.insert("v".into(), json!(vals[i as usize]));
            sensors.insert("k".into(), json!(i as i64));
            recs.push(Record { _time: base + i * 1000, sensors });
        }
        store.write_records(db, &recs).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();

        // String literal equality
        let q1 = format!("SELECT _time, reason FROM {} WHERE reason = 'power'", db);
        let q1 = match query::parse(&q1).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df1 = run_select(&shared, &q1).unwrap();
        assert_eq!(df1.height(), 2);
        let col_names = df1.get_column_names();
        assert!(col_names.iter().any(|c| c.as_str() == "reason"));

        // Float literal comparison
        let q2 = format!("SELECT _time, v FROM {} WHERE v > 1.6", db);
        let q2 = match query::parse(&q2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df2 = run_select(&shared, &q2).unwrap();
        assert_eq!(df2.height(), 1);

        // Integer literal comparison
        let q3 = format!("SELECT _time, k FROM {} WHERE k = 2", db);
        let q3 = match query::parse(&q3).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df3 = run_select(&shared, &q3).unwrap();
        assert_eq!(df3.height(), 1);

        // Datetime ISO-8601 literal (use the middle timestamp)
        let mid_iso = chrono::DateTime::from_timestamp_millis(base + 1000).unwrap().to_rfc3339();
        let q4 = format!("SELECT _time FROM {} WHERE _time >= '{}'", db, mid_iso);
        let q4 = match query::parse(&q4).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df4 = run_select(&shared, &q4).unwrap();
        assert_eq!(df4.height(), 2);
    }
}


mod fstring_tests {
    
    use super::super::run_select;
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn test_fstring_simple_and_interpolation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let db = "fstr_db.time";
        store.create_table(db).unwrap();
        // insert few rows
        let mut recs: Vec<Record> = Vec::new();
        for i in 0..3 {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), json!(i as i64));
            m.insert("a".into(), json!(format!("row{}", i)));
            recs.push(Record { _time: 1_700_000_000_000 + i as i64, sensors: m });
        }
        store.write_records(db, &recs).unwrap();

        // simple literal f-string
        let q1 = match query::parse(&format!("SELECT f'hello' AS msg FROM {} LIMIT 1", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df1 = run_select(&shared, &q1).unwrap();
        assert_eq!(df1.height(), 1);
        use polars::prelude::AnyValue;
        let av1 = df1.column("msg").unwrap().get(0).unwrap();
        match av1 {
            AnyValue::String(s) => assert_eq!(s, "hello"),
            AnyValue::StringOwned(s) => assert_eq!(s, "hello"),
            _ => panic!("unexpected type for msg"),
        }

        // interpolation of column and expression
        let q2 = match query::parse(&format!("SELECT _time, f'v={{v}} x={{v+1}} a={{a}}' AS msg FROM {} ORDER BY _time LIMIT 1", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df2 = run_select(&shared, &q2).unwrap();
        let av2 = df2.column("msg").unwrap().get(0).unwrap();
        match av2 {
            AnyValue::String(s) => assert_eq!(s, "v=0 x=1 a=row0"),
            AnyValue::StringOwned(s) => assert_eq!(s, "v=0 x=1 a=row0"),
            _ => panic!("unexpected type for msg"),
        }

        // escaped braces
        let q3 = match query::parse(&format!("SELECT f'{{{{}}}}' AS msg FROM {} LIMIT 1", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df3 = run_select(&shared, &q3).unwrap();
        let av3 = df3.column("msg").unwrap().get(0).unwrap();
        match av3 {
            AnyValue::String(s) => assert_eq!(s, "{}"),
            AnyValue::StringOwned(s) => assert_eq!(s, "{}"),
            _ => panic!("unexpected type for msg"),
        }
    }

    #[test]
    fn test_fstring_slice_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let db = "fstr_db2.time";
        store.create_table(db).unwrap();
        let mut recs: Vec<Record> = Vec::new();
        let mut m = serde_json::Map::new(); m.insert("v".into(), json!(123)); recs.push(Record { _time: 1_700_000_100_000, sensors: m });
        store.write_records(db, &recs).unwrap();
        let q = match query::parse(&format!("SELECT f'num={{v}}'[0:3] AS s FROM {}", db)).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        use polars::prelude::AnyValue;
        let av = df.column("s").unwrap().get(0).unwrap();
        match av {
            AnyValue::String(s) => assert_eq!(s, "num"),
            AnyValue::StringOwned(s) => assert_eq!(s, "num"),
            _ => panic!("unexpected type for s"),
        }
    }
}
