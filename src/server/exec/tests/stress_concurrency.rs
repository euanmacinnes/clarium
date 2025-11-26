//! Concurrency and UDF complexity stress tests
//!
//! These tests attempt to surface potential thread-safety or shared state issues by:
//! - Running many queries in parallel threads sharing a common store
//! - Exercising scalar and aggregate UDFs, including multi-return, predicates, arithmetic, and HAVING
//! - Using a single global registry shared across all threads

use super::super::run_select;
use crate::query::{self, Command};
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
fn stress_parallel_queries_udf() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    let prev_strict = system::get_strict_projection();
    system::set_strict_projection(false);
    let prev_null = system::get_null_on_error();
    system::set_null_on_error(true);

    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/stress.time";
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
                    // Alternate between several query shapes
                    let sel = match j % 5 {
                        0 => format!("SELECT * FROM {} WHERE is_pos(v) ORDER BY _time LIMIT 5", db),
                        1 => format!("SELECT dbl(v)+dbl(v) AS y FROM {} ORDER BY _time LIMIT 3", db),
                        2 => format!("SELECT k, sum_plus(v) AS s FROM {} GROUP BY k ORDER BY k", db),
                        3 => format!("SELECT k, minmax(v) AS mm FROM {} GROUP BY k HAVING mm_0 > -1 ORDER BY k", db),
                        _ => format!("SELECT k, agg_err_if_bad(k, v) AS s FROM {} GROUP BY k ORDER BY k", db),
                    };
                    let q = match query::parse(&sel) { Ok(Command::Select(q)) => q, _ => unreachable!() };
                    let res = run_select(&shared, &q);
                    if let Err(e) = res {
                        // When null_on_error=true, even agg_err_if_bad should not error; record any failure
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
        tprintln!("[stress] observed {} failure(s)", fails.len());
        for m in fails.iter().take(20) { tprintln!(" - {}", m); }
        panic!("stress_parallel_queries_udf encountered failures");
    }

    // restore flags
    system::set_strict_projection(prev_strict);
    system::set_null_on_error(prev_null);
}

#[test]
fn stress_complex_udf_queries() {
    // Initialize all test UDFs once
    super::udf_common::init_all_test_udfs();
    
    // Sequential complexity stress: long-running mix to catch leaks/state issues
    let prev_strict = system::get_strict_projection();
    system::set_strict_projection(false);
    let prev_null = system::get_null_on_error();
    system::set_null_on_error(true);

    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/stress2.time";
    let shared = seed_stress_table(&tmp, db, 4, 800);

    for i in 0..100usize {
        let qtxt = match i % 6 {
            0 => format!("SELECT k, minmax(v) AS mm FROM {} GROUP BY k HAVING mm_1 > 10 ORDER BY k", db),
            1 => format!("SELECT CONCAT(hello(s), '-', dbl(v)) AS z FROM {} ORDER BY _time LIMIT 10", db),
            2 => format!("SELECT k, sum_plus(v) AS s FROM {} GROUP BY k ORDER BY k LIMIT 3", db),
            3 => format!("SELECT * FROM {} WHERE is_pos(dbl(v)) ORDER BY _time LIMIT 7", db),
            4 => format!("SELECT k, agg_err_if_bad(k, v) AS s FROM {} GROUP BY k", db),
            _ => format!("SELECT dbl(v)+dbl(v)+dbl(v) AS y FROM {} ORDER BY _time LIMIT 5", db),
        };
        let q = match query::parse(&qtxt) { Ok(Command::Select(q)) => q, _ => unreachable!() };
        tprintln!("SQL {} ", qtxt);
        let df = run_select(&shared, &q).unwrap();
        // Spot checks
        if i % 6 == 1 {
            assert!(df.get_column_names().iter().any(|c| c.as_str()=="z"));
        }
        if i % 6 == 0 {
            assert!(df.get_column_names().iter().any(|c| c.as_str()=="mm_0"));
            assert!(df.get_column_names().iter().any(|c| c.as_str()=="mm_1"));
        }
    }

    system::set_strict_projection(prev_strict);
    system::set_null_on_error(prev_null);
}



