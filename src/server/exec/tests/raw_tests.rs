use crate::server::query::{self, Command};
use crate::server::data_context::DataContext;
use crate::server::exec::select_stages::from_where::from_where as stage_from_where;
use crate::server::exec::select_stages::project_select::project_select as stage_project_select;
use crate::storage::{Record, SharedStore, Store};
use polars::prelude::*;
use serde_json::json;
use crate::tprintln;

// Targeted tests that exercise individual SELECT stages directly to aid debugging

#[test]
fn stage_from_where_then_project_time_table_simple() {
    // Build a tiny time-series table with _time and sensor 'v'
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/raw_stage.time";
    let base: i64 = 1_800_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..3 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!((i + 1) as f64));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // SELECT _time, v with a WHERE range to ensure filtering path is exercised
    let qtxt = format!("SELECT _time, v FROM {} WHERE _time BETWEEN {} AND {}", db, base, base + 2000);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };

    // DataContext with defaults derived from db path
    let (def_db, def_schema) = ("clarium".to_string(), "public".to_string());
    let mut ctx = DataContext::with_defaults(def_db, def_schema);

    // FROM/WHERE stage
    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where should succeed");

    // Inspect columns produced by FROM/WHERE and ensure resolution works for unqualified names
    let cols = df_from.get_column_names();
    tprintln!("from_where columns: {:?}", cols);
    // Resolution through DataContext for unqualified names must succeed (suffix or exact)
    let _resolved_time = ctx.resolve_column(&df_from, "_time").expect("_time should resolve by suffix or exact");
    let _resolved_v = ctx.resolve_column(&df_from, "v").expect("v should resolve by suffix or exact");

    // Now project via SELECT stage
    let df_proj = stage_project_select(df_from, &q, &mut ctx).expect("project_select should succeed");
    let out_cols = df_proj.get_column_names();
    // _time normalized to unqualified label, and sensor column surfaced without prefix
    assert!(out_cols.iter().any(|c| c.as_str() == "_time"));
    assert!(out_cols.iter().any(|c| c.as_str() == "v"));
    // And resulted rows should be 3 (BETWEEN inclusive on endpoints)
    assert_eq!(df_proj.height(), 3);
}

#[test]
fn stage_from_where_then_project_regular_table_simple() {
    // Regular (non-time) table with id/name columns
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/people"; // no .time suffix
    store.create_table(db).unwrap();
    let ids = Series::new("id".into(), vec![1i64, 2, 3]);
    let names = Series::new("name".into(), vec!["alice", "bob", "carol"]);
    let df = DataFrame::new(vec![ids.into(), names.into()]).unwrap();
    store.rewrite_table_df(db, df).unwrap();

    let qtxt = format!("SELECT id, name FROM {}", db);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };

    let (def_db, def_schema) = ("clarium".to_string(), "public".to_string());
    let mut ctx = DataContext::with_defaults(def_db, def_schema);

    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where should succeed");
    let cols = df_from.get_column_names();
    assert!(cols.iter().any(|c| c.ends_with(".id")));
    assert!(cols.iter().any(|c| c.ends_with(".name")));
    // Resolution should work by suffix for both columns
    ctx.resolve_column(&df_from, "id").expect("id resolves");
    ctx.resolve_column(&df_from, "name").expect("name resolves");

    let df_proj = stage_project_select(df_from, &q, &mut ctx).expect("project_select should succeed");
    let out_cols = df_proj.get_column_names();
    assert!(out_cols.iter().any(|c| c.as_str() == "id"));
    assert!(out_cols.iter().any(|c| c.as_str() == "name"));
    assert_eq!(df_proj.height(), 3);
}


#[test]
fn stage_from_where_with_udf_predicate() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    // Build a small time-series table with column 'v' containing [-1, 0, 1]
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/udf_where.time";
    let base: i64 = 1_900_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for (i, v) in [-1i64, 0, 1].into_iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(v));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // WHERE uses UDF predicate
    let qtxt = format!("SELECT _time, v FROM {} WHERE is_pos(v)", db);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };

    let (def_db, def_schema) = ("clarium".to_string(), "public".to_string());
    let mut ctx = DataContext::with_defaults(def_db, def_schema);
    // Capture registry snapshot for this query context
    let reg = crate::scripts::get_script_registry().expect("registry should be initialized");
    ctx.script_registry = Some(reg.snapshot().unwrap());

    // FROM/WHERE should filter to only v = 1
    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where with UDF WHERE should succeed");
    assert_eq!(df_from.height(), 1);
    // Ensure resolution still works
    ctx.resolve_column(&df_from, "_time").expect("_time resolves");
    ctx.resolve_column(&df_from, "v").expect("v resolves");

    // Project
    let df_proj = stage_project_select(df_from, &q, &mut ctx).expect("project_select should succeed");
    let out_cols = df_proj.get_column_names();
    assert!(out_cols.iter().any(|c| c.as_str() == "_time"));
    assert!(out_cols.iter().any(|c| c.as_str() == "v"));
    assert_eq!(df_proj.height(), 1);
}

#[test]
fn stage_project_select_with_udf_in_select() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    // Build a small time table with v = [1, 2, 3]
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/udf_select.time";
    let base: i64 = 1_900_100_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..3 {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!((i as i64) + 1));
        recs.push(Record { _time: base + (i as i64) * 1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // SELECT projects UDF result
    let qtxt = format!("SELECT dbl(v) AS y FROM {}", db);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };

    let (def_db, def_schema) = ("clarium".to_string(), "public".to_string());
    let mut ctx = DataContext::with_defaults(def_db, def_schema);
    // Capture registry snapshot for this query context
    let reg = crate::scripts::get_script_registry().expect("registry should be initialized");
    ctx.script_registry = Some(reg.snapshot().unwrap());

    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where should succeed");
    // Project will compute UDF in SELECT list
    let df_proj = stage_project_select(df_from, &q, &mut ctx).expect("project_select with UDF should succeed");
    let out_cols = df_proj.get_column_names();
    assert!(out_cols.iter().any(|c| c.as_str() == "y"));
    assert_eq!(df_proj.height(), 3);
}


// --- Additional RAW tests mirroring complex_select_integration_tests but using direct stage calls ---
use crate::server::exec::select_stages::by_or_groupby::by_or_groupby as stage_by_or_groupby;
use crate::server::exec::select_stages::order_limit::order_limit as stage_order_limit;

// helper
fn ms(base: i64, sec: i64) -> i64 { base + sec * 1000 }

// Raw-test version of group_by_multi_cols_with_select_projection targeting BY/GROUP stage outputs
#[test]
fn raw_group_by_multi_cols_with_select_projection_by_stage() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/db_group_multi_raw.time";
    let base: i64 = 1_700_003_000_000;
    let rows = vec![
        (0, "A", "R1", 1.0),
        (1, "A", "R1", 2.0),
        (2, "B", "R2", 10.0),
        (3, "B", "R2", 20.0),
    ];
    let mut recs: Vec<Record> = Vec::new();
    for (i, dev, reg, v) in rows {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(v));
        m.insert("device".into(), json!(dev));
        m.insert("region".into(), json!(reg));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    let qtext = format!("SELECT device, region, SUM(v) FROM {} GROUP BY device, region", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium".to_string(), "public".to_string());

    // FROM stage
    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // BY/GROUP stage
    let df_by = stage_by_or_groupby(&shared, df_from, &q, &mut ctx).expect("by_or_groupby ok");

    assert_eq!(df_by.height(), 2);
    let names = df_by.get_column_names();
    assert!(names.iter().any(|c| c.as_str()=="device"));
    assert!(names.iter().any(|c| c.as_str()=="region"));
    assert!(names.iter().any(|c| c.as_str()=="SUM(v)"));

    let device = df_by.column("device").unwrap().str().unwrap();
    let region = df_by.column("region").unwrap().str().unwrap();
    let sumv = df_by.column("SUM(v)").unwrap().f64().unwrap();

    for i in 0..df_by.height() {
        let d = device.get(i).unwrap().to_string();
        let r = region.get(i).unwrap().to_string();
        if d == "A" && r == "R1" { assert!((sumv.get(i).unwrap() - 3.0).abs() < 1e-9); }
        else if d == "B" && r == "R2" { assert!((sumv.get(i).unwrap() - 30.0).abs() < 1e-9); }
        else { panic!("unexpected group"); }
    }
}

// Raw-test full pipeline: FROM -> BY/GROUP -> PROJECT; mirrors select projection expectations
#[test]
fn raw_group_by_multi_cols_with_select_projection_full() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let db = "clarium/public/db_group_multi_raw2.time";
    let base: i64 = 1_700_003_000_000;
    let rows = vec![
        (0, "A", "R1", 1.0),
        (1, "A", "R1", 2.0),
        (2, "B", "R2", 10.0),
        (3, "B", "R2", 20.0),
    ];
    let mut recs: Vec<Record> = Vec::new();
    for (i, dev, reg, v) in rows {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(v));
        m.insert("device".into(), json!(dev));
        m.insert("region".into(), json!(reg));
        recs.push(Record { _time: base + i*1000, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    let qtext = format!("SELECT device, region, SUM(v) FROM {} GROUP BY device, region", db);
    let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium".to_string(), "public".to_string());

    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    let df_by = stage_by_or_groupby(&shared, df_from, &q, &mut ctx).expect("by_or_groupby ok");
    let df_proj = stage_project_select(df_by, &q, &mut ctx).expect("project_select ok");

    assert_eq!(df_proj.height(), 2);
    let names = df_proj.get_column_names();
    assert!(names.iter().any(|c| c.as_str()=="device"));
    assert!(names.iter().any(|c| c.as_str()=="region"));
    assert!(names.iter().any(|c| c.as_str()=="SUM(v)"));
    assert!(names.iter().any(|c| c.as_str()=="_start_time"));
    assert!(names.iter().any(|c| c.as_str()=="_end_time"));

    let device = df_proj.column("device").unwrap().str().unwrap();
    let region = df_proj.column("region").unwrap().str().unwrap();
    let sumv = df_proj.column("SUM(v)").unwrap().f64().unwrap();
    for i in 0..df_proj.height() {
        let d = device.get(i).unwrap().to_string();
        let r = region.get(i).unwrap().to_string();
        if d == "A" && r == "R1" { assert!((sumv.get(i).unwrap() - 3.0).abs() < 1e-9); }
        else if d == "B" && r == "R2" { assert!((sumv.get(i).unwrap() - 30.0).abs() < 1e-9); }
        else { panic!("unexpected group"); }
    }
}

#[test]
fn raw_stage_complex_by_slice_direct() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    // Main data table across 10 minutes at 1s cadence
    let main = "clarium/public/ci_main_raw.time";
    let t0: i64 = 1_906_000_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..600 {
        let mut m = serde_json::Map::new();
        let v = if i % 120 < 60 { 1.0 } else { 5.0 };
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

    // Slice tables
    let s1 = "clarium/public/ci_maint_raw.time";
    let mut s1recs: Vec<Record> = Vec::new();
    {
        let mut m = serde_json::Map::new();
        m.insert("_start_date".into(), json!(ms(t0, 30)));
        m.insert("_end_date".into(), json!(ms(t0, 180)));
        m.insert("kind".into(), json!("A"));
        s1recs.push(Record { _time: ms(t0, 30), sensors: m });
    }
    store.write_records(s1, &s1recs).unwrap();

    let s2 = "clarium/public/ci_down_raw.time";
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

    let manual_start = ms(t0, 400);
    let manual_end = ms(t0, 480);
    let qtxt = format!(
        "SELECT AVG(v) AS avg_v, SUM(w) AS sum_w FROM {} \
         BY SLICE( USING LABELS(machine, knd) {} LABEL('M1','A') \
         UNION ({}, {}, machine:='MX', knd:='X') \
         INTERSECT {} )",
        main, s1, manual_start, manual_end, s2
    );
    tprintln!("FINAL SQL: {}", qtxt);
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };

    // Stage: FROM/WHERE
    let mut ctx = DataContext::with_defaults("clarium".to_string(), "public".to_string());
    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    tprintln!("raw_stage_complex_by_slice_direct: FROM rows={}", df_from.height());
    // Exact expectations: base table has 600 rows (10 minutes at 1s)
    assert_eq!(df_from.height(), 600);
    // Check base properties deterministically
    let tcol = ctx.resolve_column(&df_from, "_time").expect("_time resolves");
    let tmin = df_from.column(&tcol).unwrap().i64().unwrap().get(0).unwrap();
    let tmax = df_from.column(&tcol).unwrap().i64().unwrap().get(599).unwrap();
    assert_eq!(tmin, t0);
    assert_eq!(tmax, t0 + 599_000);
    // Spot-check 'v' pattern
    let vcol = ctx.resolve_column(&df_from, "v").expect("v resolves");
    let v_ca = df_from.column(&vcol).unwrap().f64().unwrap();
    assert_eq!(v_ca.get(0).unwrap(), 1.0);
    assert_eq!(v_ca.get(60).unwrap(), 5.0);
    assert_eq!(v_ca.get(120).unwrap(), 1.0);

    // Stage: BY/GROUP BY (BY SLICE plan)
    let df_by = stage_by_or_groupby(&shared, df_from.clone(), &q, &mut ctx).expect("by_or_groupby ok");
    tprintln!("raw_stage_complex_by_slice_direct: BY rows={}", df_by.height());
    // Expect single row due to UNION+INTERSECT collapsing to [60s,180s)
    assert_eq!(df_by.height(), 1);
    let cols = df_by.get_column_names();
    tprintln!("BY STAGE cols={:?}", cols);
    // Dump first row values for debugging
    for c in &cols { let s = df_by.column(c).unwrap(); let v = s.get(0); tprintln!("  col {} => {:?}", c, v); }
    // At BY stage, aggregate columns are named by function (e.g., AVG(v)/SUM(w)); aliases are applied in PROJECT later
    assert!(cols.iter().any(|c| c.as_str() == "AVG(v)") || cols.iter().any(|c| c.as_str() == "avg_v"));
    assert!(cols.iter().any(|c| c.as_str() == "SUM(w)") || cols.iter().any(|c| c.as_str() == "sum_w"));
    let cols_vec = df_by.get_column_names();
    let avg_name = if cols_vec.iter().any(|c| c.as_str() == "avg_v") { "avg_v".to_string() } else if cols_vec.iter().any(|c| c.as_str() == "AVG(v)") { "AVG(v)".to_string() } else {
        cols_vec.iter().find(|c| c.as_str().starts_with("AVG(")).map(|c| c.to_string()).unwrap_or("AVG(v)".to_string())
    };
    let sum_name = if cols_vec.iter().any(|c| c.as_str() == "sum_w") { "sum_w".to_string() } else if cols_vec.iter().any(|c| c.as_str() == "SUM(w)") { "SUM(w)".to_string() } else {
        cols_vec.iter().find(|c| c.as_str().starts_with("SUM(")).map(|c| c.to_string()).unwrap_or("SUM(w)".to_string())
    };
    tprintln!("Resolved avg_name='{}', sum_name='{}'", avg_name, sum_name);
    let av = df_by.column(&avg_name).unwrap().cast(&DataType::Float64).unwrap().f64().unwrap().get(0).unwrap();
    let sw = df_by.column(&sum_name).unwrap().cast(&DataType::Float64).unwrap().f64().unwrap().get(0).unwrap();
    // Expected avg and sum computed analytically
    assert!((av - 3.0).abs() < 1e-9, "avg_v expected 3.0 got {}", av);
    assert!((sw - 2960.0).abs() < 1e-9, "sum_w expected 2960 got {}", sw);

    // Stage: PROJECT SELECT (should preserve aggregates and labels)
    let df_proj = stage_project_select(df_by.clone(), &q, &mut ctx).expect("project_select ok");
    assert_eq!(df_proj.height(), 1);
    assert_eq!(df_proj.column("avg_v").unwrap().f64().unwrap().get(0).unwrap(), av);
    assert_eq!(df_proj.column("sum_w").unwrap().f64().unwrap().get(0).unwrap(), sw);

    // Stage: ORDER/LIMIT (no ORDER/LIMIT in query, so no-op)
    let df_ord = stage_order_limit(df_proj.clone(), &q, &mut ctx).expect("order_limit ok");
    assert_eq!(df_ord.height(), 1);
}

#[test]
fn raw_stage_nested_by_slice_direct() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let main = "clarium/public/ci_main2_raw.time";
    let t0: i64 = 1_906_100_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..360 { // 6 minutes
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(if i < 180 { 2.0 } else { 4.0 }));
        m.insert("device".into(), json!(if i % 2 == 0 { "M1" } else { "M2" }));
        recs.push(Record { _time: t0 + i*1000, sensors: m });
    }
    store.write_records(main, &recs).unwrap();

    let a = "clarium/public/ci_a_raw.time"; // [0,180s]
    let b = "clarium/public/ci_b_raw.time"; // [120,360s]
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
    let s_manual1 = t0 + 150_000; let e_manual1 = t0 + 330_000;
    let qtxt = format!(
        "SELECT AVG(v) AS av, COUNT(v) AS cnt FROM {} BY SLICE( \
           USING LABELS(label) {} LABEL(lab) \
           UNION SLICE( USING LABELS(label) {} LABEL(lab) INTERSECT ({}, {}, label:='N') ) \
        ) HAVING av >= 2 AND cnt > 0 ORDER BY av DESC LIMIT 1",
        main, a, b, s_manual1, e_manual1
    );
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };

    let mut ctx = DataContext::with_defaults("clarium".to_string(), "public".to_string());

    // FROM/WHERE
    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    assert!(df_from.height() >= 1);
    // BY/GROUP BY (applies HAVING inside)
    let df_by = stage_by_or_groupby(&shared, df_from.clone(), &q, &mut ctx).expect("by_or_groupby ok");
    tprintln!("raw_stage_nested_by_slice_direct: BY rows={}", df_by.height());
    let cols = df_by.get_column_names();
    tprintln!("BY STAGE (nested) cols={:?}", cols);
    for r in 0..df_by.height().min(5) {
        let mut row_parts: Vec<String> = Vec::new();
        for c in &cols { let v = df_by.column(c).unwrap().get(r); row_parts.push(format!("{}={:?}", c, v)); }
        tprintln!("  row {}: {}", r, row_parts.join(", "));
    }
    // With current UNION semantics (coalesce regardless of labels), expect a single merged interval
    assert_eq!(df_by.height(), 1);
    // Verify expected aggregates for merged [0s,330s) window: AVG=(2*180 + 4*150)/330 = 960/330 ≈ 2.90909, COUNT=330
    let ncols = df_by.get_column_names();
    let by_avg = if ncols.iter().any(|c| c.as_str() == "av") { "av".to_string() } else if ncols.iter().any(|c| c.as_str() == "AVG(v)") { "AVG(v)".to_string() } else {
        ncols.iter().find(|c| c.as_str().starts_with("AVG(")).map(|c| c.to_string()).unwrap_or("AVG(v)".to_string())
    };
    let by_cnt = if ncols.iter().any(|c| c.as_str() == "cnt") { "cnt".to_string() } else if ncols.iter().any(|c| c.as_str() == "COUNT(v)") { "COUNT(v)".to_string() } else {
        ncols.iter().find(|c| c.as_str().starts_with("COUNT(")).map(|c| c.to_string()).unwrap_or("COUNT(v)".to_string())
    };
    let av = df_by.column(&by_avg).unwrap().cast(&DataType::Float64).unwrap().f64().unwrap().get(0).unwrap();
    assert!((av - (960.0/330.0)).abs() < 1e-6, "expected av ≈ {} got {}", 960.0/330.0, av);
    let cnt_val = match df_by.column(&by_cnt).unwrap().i64() {
        Ok(ca) => ca.get(0).unwrap(),
        Err(_) => {
            let s_cast = df_by.column(&by_cnt).unwrap().cast(&DataType::Int64).unwrap();
            s_cast.i64().unwrap().get(0).unwrap()
        }
    };
    assert_eq!(cnt_val, 330);
    // Label should carry from LHS (A) due to left-to-right propagation
    let lab = df_by.column("label").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(lab, "A");

    // PROJECT SELECT
    let df_proj = stage_project_select(df_by.clone(), &q, &mut ctx).expect("project_select ok");
    assert_eq!(df_proj.height(), 1);

    // ORDER/LIMIT is a no-op effectively since only one row
    let df_ord = stage_order_limit(df_proj.clone(), &q, &mut ctx).expect("order_limit ok");
    assert_eq!(df_ord.height(), 1);
    let top_av = df_ord.column("av").unwrap().f64().unwrap().get(0).unwrap();
    assert!((top_av - (960.0/330.0)).abs() < 1e-6);
}


#[test]
fn raw_stage_order_limit_and_having_after_by() {
    // Compose a dataset to exercise BY with labels and HAVING, then ORDER/LIMIT
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let main = "clarium/public/ci_main2.time";
    let t0: i64 = 1_906_100_000_000;
    let mut recs: Vec<Record> = Vec::new();
    for i in 0..120 { // 2 minutes
        let mut m = serde_json::Map::new();
        m.insert("v".into(), json!(if i < 60 { 2.0 } else { 4.0 }));
        m.insert("device".into(), json!(if i % 2 == 0 { "M1" } else { "M2" }));
        recs.push(Record { _time: t0 + i*1000, sensors: m });
    }
    store.write_records(main, &recs).unwrap();

    // Slices tables with labels
    let a = "clarium/public/ci_a.time"; // [0,60s]
    let b = "clarium/public/ci_b.time"; // [30,120s]
    let mut arecs: Vec<Record> = Vec::new();
    let mut brecs: Vec<Record> = Vec::new();
    arecs.push(Record { _time: t0, sensors: serde_json::Map::from_iter(vec![
        ("_start_date".into(), json!(t0)),
        ("_end_date".into(), json!(t0 + 60_000)),
        ("label".into(), json!("A")),
    ])});
    brecs.push(Record { _time: t0 + 30_000, sensors: serde_json::Map::from_iter(vec![
        ("_start_date".into(), json!(t0 + 30_000)),
        ("_end_date".into(), json!(t0 + 120_000)),
        ("label".into(), json!("B")),
    ])});
    store.write_records(a, &arecs).unwrap();
    store.write_records(b, &brecs).unwrap();

    let qtxt = format!(
        "SELECT AVG(v) AS avg_v, SUM(v*10) AS sum_w, device AS machine, device AS knd FROM {} \
         BY SLICE( USING LABELS(label) {} LABEL(label) UNION SLICE( USING LABELS(label) {} LABEL(label) ) ) ",
        main, a, b
    );
    let q = match query::parse(&qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };

    let mut ctx = DataContext::with_defaults("clarium".to_string(), "public".to_string());

    // Stage: FROM/WHERE
    let df_from = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    tprintln!("FROM rows={} cols={:?}", df_from.height(), df_from.get_column_names());

    // Stage: BY OR GROUP BY
    let df_by = stage_by_or_groupby(&shared, df_from.clone(), &q, &mut ctx).expect("by_or_groupby ok");
    tprintln!("BY rows={} cols={:?}", df_by.height(), df_by.get_column_names());
    let cols = df_by.get_column_names();
    for c in &cols { let s = df_by.column(c).unwrap(); let v = s.get(0); tprintln!("  col {} => {:?}", c, v); }
    // At BY stage, aggregate columns are named by function (e.g., AVG(v)/SUM(...)); aliases are applied in PROJECT later
    assert!(cols.iter().any(|c| c.as_str() == "AVG(v)") || cols.iter().any(|c| c.as_str() == "avg_v") || cols.iter().any(|c| c.starts_with("AVG(")));
    assert!(cols.iter().any(|c| c.as_str() == "SUM(w)") || cols.iter().any(|c| c.as_str() == "sum_w") || cols.iter().any(|c| c.starts_with("SUM(")));
    let cols_vec = df_by.get_column_names();
    let avg_name = if cols_vec.iter().any(|c| c.as_str() == "avg_v") { "avg_v".to_string() } else if cols_vec.iter().any(|c| c.as_str() == "AVG(v)") { "AVG(v)".to_string() } else {
        cols_vec.iter().find(|c| c.as_str().starts_with("AVG(")).map(|c| c.to_string()).unwrap_or("AVG(v)".to_string())
    };
    let sum_name = if cols_vec.iter().any(|c| c.as_str() == "sum_w") { "sum_w".to_string() } else if cols_vec.iter().any(|c| c.as_str() == "SUM(w)") { "SUM(w)".to_string() } else {
        cols_vec.iter().find(|c| c.as_str().starts_with("SUM(")).map(|c| c.to_string()).unwrap_or("SUM(w)".to_string())
    };
    tprintln!("Resolved avg_name='{}', sum_name='{}'", avg_name, sum_name);
    let av = df_by.column(&avg_name).unwrap().cast(&DataType::Float64).unwrap().f64().unwrap().get(0).unwrap();
    let sw = df_by.column(&sum_name).unwrap().cast(&DataType::Float64).unwrap().f64().unwrap().get(0).unwrap();
    // Expected avg and sum computed analytically
    assert!((av - 3.0).abs() < 1e-9, "avg_v expected 3.0 got {}", av);
    assert!((sw - 3600.0).abs() < 1e-9, "sum_w expected 3600 got {}", sw);

    // Stage: PROJECT SELECT (should preserve aggregates and labels)
    let df_proj = stage_project_select(df_by.clone(), &q, &mut ctx).expect("project_select ok");
    assert_eq!(df_proj.height(), 1);
    assert_eq!(df_proj.column("avg_v").unwrap().f64().unwrap().get(0).unwrap(), av);
    assert_eq!(df_proj.column("sum_w").unwrap().f64().unwrap().get(0).unwrap(), sw);
    // Stage: ORDER/LIMIT (no ORDER/LIMIT in query, so no-op)
    let df_ord = stage_order_limit(df_proj.clone(), &q, &mut ctx).expect("order_limit ok");
    assert_eq!(df_ord.height(), 1);
}


// --- NEW RAW TEST: UDF aggregate group-by single and multi with stage-by-stage diagnostics ---
use crate::system;

#[test]
fn raw_test_aggregate_udf_group_by_single_and_multi() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    // Mirror tests_udf::test_aggregate_udf_group_by_single_and_multi but call stages directly
    let prev = system::get_strict_projection();
    system::set_strict_projection(false);

    // Build table with key k and value v
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    // Use the same table name/path pattern as the canonical test for parity
    let db = "udf_agg.time";
    store.create_table(db).unwrap();
    let rows = vec![
        ("a", 1i64), ("a", 2), ("a", 3),
        ("b", 10), ("b", 20),
    ];
    let mut recs: Vec<Record> = Vec::new();
    for (i, (k, v)) in rows.into_iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("k".into(), json!(k));
        m.insert("v".into(), json!(v));
        recs.push(Record { _time: 1_700_200_000_000 + i as i64, sensors: m });
    }
    store.write_records(db, &recs).unwrap();

    // Get the global registry
    let reg = crate::scripts::get_script_registry().expect("registry should be initialized");

    // Execute all stages inside the session-local Lua registry to mirror engine behavior
    crate::scripts::with_session_registry(&reg, || {
        // Query 1: single-return aggregate
        let q1txt = format!("SELECT k, sum_plus(v) AS sp FROM {} GROUP BY k ORDER BY k", db);
        let q1 = match query::parse(&q1txt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let mut ctx1 = DataContext::with_defaults("clarium".to_string(), "public".to_string());

        // Stage FROM
        let df1_from = stage_from_where(&shared, &q1, &mut ctx1).expect("q1 from_where ok");
        tprintln!("Q1 FROM rows={} cols={:?}", df1_from.height(), df1_from.get_column_names());
        // Stage BY/GROUP
        let df1_by = stage_by_or_groupby(&shared, df1_from.clone(), &q1, &mut ctx1).expect("q1 by_or_groupby ok");
        tprintln!("Q1 BY rows={} cols={:?}", df1_by.height(), df1_by.get_column_names());
        for r in 0..df1_by.height() { let k = df1_by.column("k").unwrap().get(r); let sp = df1_by.column("sp").or_else(|_| df1_by.column("SUM_PLUS(v)")); let sp = sp.unwrap().get(r); tprintln!("  BY r{}: k={:?} sp={:?}", r, k, sp); }
        // Stage PROJECT (to honor alias 'sp' definitively)
        let df1_proj = stage_project_select(df1_by.clone(), &q1, &mut ctx1).expect("q1 project ok");
        // Stage ORDER/LIMIT (parity with exec_select, even if no-op)
        let df1_final = stage_order_limit(df1_proj.clone(), &q1, &mut ctx1).expect("q1 order_limit ok");
        tprintln!("Q1 PROJ rows={} cols={:?}", df1_proj.height(), df1_proj.get_column_names());
        // Validate results: a->7, b->31 (ORDER BY k ensures row0='a', row1='b')
        assert_eq!(df1_final.height(), 2);
        let spseries = df1_final.column("sp").or_else(|_| df1_final.column("SUM_PLUS(v)"));
        let spseries = spseries.unwrap();
        let v0 = match spseries.get(0).unwrap() { AnyValue::Int64(v) => v, AnyValue::Float64(v) => v as i64, AnyValue::UInt64(v) => v as i64, other => panic!("unexpected type for sp[0]: {:?}", other) };
        let v1 = match spseries.get(1).unwrap() { AnyValue::Int64(v) => v, AnyValue::Float64(v) => v as i64, AnyValue::UInt64(v) => v as i64, other => panic!("unexpected type for sp[1]: {:?}", other) };
        assert_eq!(v0, 7);
        assert_eq!(v1, 31);

        // Query 2: multi-return aggregate with HAVING and ORDER
        let q2txt = format!("SELECT k, minmax(v) AS mm FROM {} GROUP BY k ORDER BY k", db);
        let q2 = match query::parse(&q2txt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let mut ctx2 = DataContext::with_defaults("clarium".to_string(), "public".to_string());
        let df2_from = stage_from_where(&shared, &q2, &mut ctx2).expect("q2 from_where ok");
        tprintln!("Q2 FROM rows={} cols={:?}", df2_from.height(), df2_from.get_column_names());
        let df2_by = stage_by_or_groupby(&shared, df2_from.clone(), &q2, &mut ctx2).expect("q2 by_or_groupby ok");
        tprintln!("Q2 BY rows={} cols={:?}", df2_by.height(), df2_by.get_column_names());
        for r in 0..df2_by.height() { let k = df2_by.column("k").unwrap().get(r); let c0 = df2_by.column("mm_0").or_else(|_| df2_by.column("MINMAX_0(v)")); let c1 = df2_by.column("mm_1").or_else(|_| df2_by.column("MINMAX_1(v)")); tprintln!("  BY r{}: k={:?} mm_0={:?} mm_1={:?}", r, k, c0.unwrap().get(r), c1.unwrap().get(r)); }
        let df2_proj = stage_project_select(df2_by.clone(), &q2, &mut ctx2).expect("q2 project ok");
        let _df2_final = stage_order_limit(df2_proj.clone(), &q2, &mut ctx2).expect("q2 order_limit ok");
        tprintln!("Q2 PROJ rows={} cols={:?}", df2_proj.height(), df2_proj.get_column_names());
        assert!(df2_proj.get_column_names().iter().any(|c| c.as_str()=="mm_0"));
        assert!(df2_proj.get_column_names().iter().any(|c| c.as_str()=="mm_1"));

        // Query 3: HAVING on multi-return aggregate
        let q3txt = format!("SELECT k, minmax(v) AS mm FROM {} GROUP BY k HAVING mm_0 > 2 ORDER BY k", db);
        let q3 = match query::parse(&q3txt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let mut ctx3 = DataContext::with_defaults("clarium".to_string(), "public".to_string());
        let df3_from = stage_from_where(&shared, &q3, &mut ctx3).expect("q3 from_where ok");
        let df3_by = stage_by_or_groupby(&shared, df3_from.clone(), &q3, &mut ctx3).expect("q3 by_or_groupby ok");
        tprintln!("Q3 BY rows={} cols={:?}", df3_by.height(), df3_by.get_column_names());
        tprintln!("df3 {} ", df3_by);
        for r in 0..df3_by.height() { let k = df3_by.column("k").unwrap().get(r); let c0 = df3_by.column("mm_0").or_else(|_| df3_by.column("MINMAX_0(v)")); tprintln!("  BY r{}: k={:?} mm_0={:?}", r, k, c0.unwrap().get(r)); }
        let df3_proj = stage_project_select(df3_by.clone(), &q3, &mut ctx3).expect("q3 project ok");
        // Apply ORDER/LIMIT and HAVING exactly like run_select does
        let df3_order = stage_order_limit(df3_proj.clone(), &q3, &mut ctx3).expect("q3 order_limit ok");
        let df3_final = crate::server::exec::select_stages::having::apply_having_with_validation(df3_order, q3.having_clause.as_ref().unwrap(), &ctx3).expect("q3 having ok");
        // Expect only group 'b'
        assert_eq!(df3_final.height(), 1);
        let konly = df3_final.column("k").unwrap().get(0).unwrap();
        match konly { AnyValue::String(s) => assert_eq!(s, "b"), AnyValue::StringOwned(s) => assert_eq!(s, "b"), _ => panic!("unexpected") }
    });

    // restore strictness
    system::set_strict_projection(prev);
}



