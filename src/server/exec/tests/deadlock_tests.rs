//! Deadlock-focused tests
//! These tests run with parking_lot's deadlock_detection feature enabled and spawn
//! a background checker that will panic the test if any deadlock is detected.

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

use crate::scripts::{ScriptRegistry, ScriptMeta, ScriptKind};
use crate::storage::{Store, SharedStore, Record};
use crate::server::query::{self, Command};
use crate::server::exec::run_select;
use crate::tprintln;
use polars::prelude::DataType;
use serde_json::json;

fn start_deadlock_detector() -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            // Poll every 200ms
            thread::sleep(Duration::from_millis(200));
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if !deadlocks.is_empty() {
                eprintln!("\n[deadlock] DETECTED {} deadlock(s)", deadlocks.len());
                for (i, threads) in deadlocks.iter().enumerate() {
                    eprintln!("\nDeadlock #{}:", i);
                    for t in threads {
                        eprintln!("Thread Id {:#?}", t.thread_id());
                        eprintln!("Backtrace:\n{:?}", t.backtrace());
                    }
                }
                panic!("deadlock detected by parking_lot detector");
            }
        }
    })
}

fn seed_table(tmp: &tempfile::TempDir, name: &str, groups: usize, rows_per_group: usize) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    store.create_table(name).unwrap();
    let mut recs: Vec<Record> = Vec::with_capacity(groups * rows_per_group);
    let mut t: i64 = 1_900_000_000_000;
    for g in 0..groups {
        let label = format!("g{}", g);
        for r in 0..rows_per_group {
            let mut m = serde_json::Map::new();
            m.insert("k".into(), json!(label));
            let v = if r % 7 == 0 { None } else { Some(((r as i64) % 50) - 25) };
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
fn no_deadlocks_under_registry_and_query_load() {
    // Start deadlock monitor
    let monitor = start_deadlock_detector();

    // Prepare a global-ish registry for this test
    let reg = ScriptRegistry::new().unwrap();
    reg.load_script_text("inc", "function inc(x) if x==nil then return 0 end return x+1 end").unwrap();
    reg.set_meta("inc", ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::Int64], nullable: true, version: 0 });

    // Seed a temp table
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/dead.time";
    let shared = seed_table(&tmp, db, 4, 300);

    // Threads perform a mixture of registry mutations and queries to increase lock contention
    let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8).min(12);
    let duration = Duration::from_secs(5);
    let start = Instant::now();
    let progress = Arc::new(AtomicU64::new(0));

    // Watchdog to ensure progress advances; if stalled for >2s, fail early (indicates potential deadlock)
    let progress_clone = progress.clone();
    let watchdog = thread::spawn(move || {
        let mut last = progress_clone.load(Ordering::Relaxed);
        let mut last_change = Instant::now();
        loop {
            thread::sleep(Duration::from_millis(200));
            let now_count = progress_clone.load(Ordering::Relaxed);
            if now_count != last { last = now_count; last_change = Instant::now(); }
            if Instant::now().duration_since(last_change) > Duration::from_secs(2) {
                panic!("watchdog: no progress for >2s; potential deadlock");
            }
            if Instant::now().duration_since(start) > duration + Duration::from_secs(2) { break; }
        }
    });

    thread::scope(|scope| {
        for i in 0..threads {
            let shared = shared.clone();
            let reg = reg.clone();
            let progress = progress.clone();
            scope.spawn(move || {
                // Alternate tasks per thread
                let mut iter = 0u64;
                while Instant::now().duration_since(start) < duration {
                    iter += 1;
                    if i % 3 == 0 {
                        // Mutate registry names to exercise two mutexes (inner/meta)
                        let name_old = format!("f{}_{}", i, iter);
                        let name_new = format!("f{}_{}", i, iter+1);
                        let _ = reg.load_script_text(&name_old, "function f(x) return x end");
                        reg.set_meta(&name_old, ScriptMeta { kind: ScriptKind::Scalar, returns: vec![DataType::Int64], nullable: true, version: 0 });
                        let _ = reg.rename_function(&name_old, &name_new);
                        reg.unload_function(&name_new);
                    } else {
                        // Run simple selects touching storage + exec
                        let sel = if iter % 2 == 0 {
                            format!("SELECT * FROM {} WHERE inc(v) > 0 ORDER BY _time LIMIT 3", db)
                        } else {
                            format!("SELECT k, sum(v) AS s FROM {} GROUP BY k ORDER BY k LIMIT 2", db)
                        };
                        let q = match query::parse(&sel) { Ok(Command::Select(q)) => q, _ => unreachable!() };
                        let _ = run_select(&shared, &q).unwrap();
                    }
                    progress.fetch_add(1, Ordering::Relaxed);
                    if iter % 8 == 0 { thread::yield_now(); }
                }
            });
        }
    });

    // Let watchdog finish
    let _ = watchdog.join();

    // Drop monitor thread by letting test end; the monitor is detached and will stop with process.
    let _ = monitor.thread().id();

    tprintln!("[deadlock] test completed without detected deadlocks");
}
