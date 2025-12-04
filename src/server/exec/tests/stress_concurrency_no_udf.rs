//! Concurrency and complexity stress tests without using any UDFs
//!
//! This mirrors the UDF stress test but only uses built-in expressions and aggregates
//! to exercise the executor under concurrent load without involving the Lua registry.

use super::super::run_select;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use crate::system;
use polars::prelude::AnyValue;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::tprintln;

fn seed_stress_table(tmp: &tempfile::TempDir, name: &str, groups: usize, rows_per_group: usize) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    store.create_table(name).unwrap();
    let mut recs: Vec<Record> = Vec::with_capacity(groups * rows_per_group);
    let mut t: i64 = 1_900_000_000_000;
    for g in 0..groups {
        let label = format!("g{}", g);
        for r in 0..rows_per_group {
            let mut m = serde_json::Map::new();
            m.insert("k".into(), json!(label));
            // Mix positive/negative and some nulls
            let v = if r % 17 == 0 { None } else { Some(((r as i64) % 50) - 25) };
            match v { Some(v) => { m.insert("v".into(), json!(v)); }, None => { m.insert("v".into(), serde_json::Value::Null); } }
            m.insert("s".into(), json!(format!("row{}_{}", g, r)));
            recs.push(Record { _time: t, sensors: m });
            t += 1_000;
        }
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn stress_parallel_queries_no_udf() {
    let prev_strict = system::get_strict_projection();
    system::set_strict_projection(false);
    let prev_null = system::get_null_on_error();
    system::set_null_on_error(true);

    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/stress_no_udf.time";
    let shared = seed_stress_table(&tmp, db, 8, 500); // 8 groups x 500 rows = 4000 rows

    let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8).min(16);
    let iters_per_thread = 30usize;

    let failures: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    thread::scope(|scope| {
        for _ in 0..threads {
            let shared = shared.clone();
            let failures = failures.clone();
            scope.spawn(move || {
                // Each thread needs its own TLS setting (thread-local storage doesn't inherit from parent)
                system::set_strict_projection(false);
                system::set_null_on_error(true);
                for j in 0..iters_per_thread {
                    // Alternate between several query shapes, all without UDFs
                    let sel = match j % 5 {
                        0 => format!("SELECT * FROM {} WHERE v > 0 ORDER BY _time LIMIT 5", db),
                        1 => format!("SELECT v + v AS y FROM {} ORDER BY _time LIMIT 3", db),
                        2 => format!("SELECT k, SUM(v) AS s FROM {} GROUP BY k ORDER BY k", db),
                        3 => format!("SELECT k, MIN(v) AS mn, MAX(v) AS mx FROM {} GROUP BY k HAVING mx > -1 ORDER BY k", db),
                        _ => format!("SELECT k, COUNT(v) AS c FROM {} GROUP BY k ORDER BY k", db),
                    };
                    let q = match query::parse(&sel) { Ok(Command::Select(q)) => q, _ => unreachable!() };
                    let res = run_select(&shared, &q);
                    if let Err(e) = res {
                        let mut f = failures.lock().unwrap();
                        f.push(format!("query failed: {} => {}", sel, e));
                        break;
                    } else if j % 5 == 1 {
                        // Light sanity check for arithmetic result type
                        let df = res.unwrap();
                        let v = df.column("y").ok().and_then(|c| c.get(0).ok()).unwrap_or(AnyValue::Null);
                        match v { AnyValue::Int64(_) | AnyValue::Float64(_) | AnyValue::Null => {}, _ => {
                            let mut f = failures.lock().unwrap();
                            f.push("unexpected type for arithmetic result".to_string());
                            break;
                        }}
                    }
                }
            });
        }
    });

    let fails = failures.lock().unwrap();
    if !fails.is_empty() {
        tprintln!("[stress_no_udf] observed {} failure(s)", fails.len());
        for m in fails.iter().take(20) { tprintln!(" - {}", m); }
        panic!("stress_parallel_queries_no_udf encountered failures");
    }

    // restore flags
    system::set_strict_projection(prev_strict);
    system::set_null_on_error(prev_null);
}

#[test]
fn stress_complex_no_udf_queries() {
    // Sequential complexity stress without UDFs
    let prev_strict = system::get_strict_projection();
    system::set_strict_projection(false);
    let prev_null = system::get_null_on_error();
    system::set_null_on_error(true);

    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/stress2_no_udf.time";
    let shared = seed_stress_table(&tmp, db, 4, 800);

    for i in 0..100usize {
        let qtxt = match i % 6 {
            0 => format!("SELECT k, MIN(v) AS mn, MAX(v) AS mx FROM {} GROUP BY k HAVING mx > 10 ORDER BY k", db),
            1 => format!("SELECT CONCAT('hi:', s) AS z FROM {} ORDER BY _time LIMIT 10", db),
            2 => format!("SELECT k, SUM(v) AS s FROM {} GROUP BY k ORDER BY k LIMIT 3", db),
            3 => format!("SELECT * FROM {} WHERE (v + 1) > 0 ORDER BY _time LIMIT 7", db),
            4 => format!("SELECT k, COUNT(*) AS c FROM {} GROUP BY k", db),
            _ => format!("SELECT v+v+v AS y FROM {} ORDER BY _time LIMIT 5", db),
        };
        let q = match query::parse(&qtxt) { Ok(Command::Select(q)) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        // Spot checks
        if i % 6 == 1 {
            assert!(df.get_column_names().iter().any(|c| c.as_str()=="z"));
        }
        if i % 6 == 0 {
            assert!(df.get_column_names().iter().any(|c| c.as_str()=="mn"));
            assert!(df.get_column_names().iter().any(|c| c.as_str()=="mx"));
        }
    }

    system::set_strict_projection(prev_strict);
    system::set_null_on_error(prev_null);
}



