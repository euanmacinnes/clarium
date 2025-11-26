use timeline::server::exec::{execute_query, execute_query2};
use timeline::storage::{SharedStore, Record};
use serde_json::json;
use std::time::Instant;

fn gen_sine_value(minute_of_day: i64) -> f64 {
    let two_pi = std::f64::consts::PI * 2.0;
    let angle = two_pi * (minute_of_day as f64) / 1440.0; // daily oscillation
    angle.sin()
}

#[tokio::test]
async fn test_integration_monthly_minute_sine_create_query_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Dynamically create database
    let db = "sine_month.time";
    let t_db_create = Instant::now();
    let _ = execute_query(&shared, &format!("CREATE TIME TABLE {}", db)).await.unwrap();
    tprintln!(
        "integration(db create): db={}, elapsed_ms={}",
        db,
        t_db_create.elapsed().as_millis()
    );

    // Generate 30 days of per-minute sine values
    let raw_base_ms: i64 = 1_700_000_000_000; // arbitrary epoch start
    let minutes_per_day: i64 = 1440;
    let day_ms: i64 = 86_400_000;
    let base_ms: i64 = (raw_base_ms / day_ms) * day_ms; // align to midnight boundary to have exact daily buckets
    let total_days: i64 = 30;

    let t_ingest = Instant::now();
    for d in 0..total_days {
        let day_start = base_ms + d * day_ms;
        let t_gen = Instant::now();
        let mut recs: Vec<Record> = Vec::with_capacity(minutes_per_day as usize);
        for m in 0..minutes_per_day {
            let t = day_start + m * 60_000;
            let v = gen_sine_value(m);
            let mut sensors = serde_json::Map::new();
            sensors.insert("sine".into(), json!(v));
            recs.push(Record { _time: t, sensors });
        }
        let gen_ms = t_gen.elapsed().as_millis();
        let t_write = Instant::now();
        {
            let guard = shared.0.lock();
            guard.write_records(db, &recs).unwrap();
        }
        let write_ms = t_write.elapsed().as_millis();
        tprintln!(
            "integration(ingest day): day_index={}, records={}, gen_ms={}, write_ms={}",
            d,
            recs.len(),
            gen_ms,
            write_ms
        );
    }
    tprintln!(
        "integration(ingest sine per-minute for {} days): elapsed_ms={}",
        total_days,
        t_ingest.elapsed().as_millis()
    );

    // Query 1: retrieve full range and ensure row count matches 30 * 1440
    let end_ms = base_ms + total_days * day_ms - 60_000; // last minute timestamp
    let q1 = format!(
        "SELECT _time, sine FROM {} WHERE _time BETWEEN {} AND {}",
        db, base_ms, end_ms
    );
    let t_q1 = Instant::now();
    let res1 = execute_query2(&shared, &q1).await.unwrap();
    let rows1 = res1.as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(rows1, (total_days * minutes_per_day) as usize);
    tprintln!(
        "integration(query full retrieval): rows={}, elapsed_ms={}",
        rows1,
        t_q1.elapsed().as_millis()
    );

    // Query 2: BY 1d aggregation and check count per day and average near zero
    let q2 = format!(
        "SELECT AVG(sine), COUNT(sine) FROM {} BY 1d WHERE _time BETWEEN {} AND {}",
        db, base_ms, end_ms
    );
    let t_q2 = Instant::now();
    let res2 = execute_query2(&shared, &q2).await.unwrap();
    let rows2 = res2.as_array().unwrap();
    assert_eq!(rows2.len(), total_days as usize, "expected one row per day");
    for row in rows2 {
        let avg = row.get("AVG(sine)").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let cnt = row.get("COUNT(sine)").and_then(|v| v.as_i64()).unwrap_or(0);
        // count should be exactly 1440 (per minute per day)
        assert_eq!(cnt, minutes_per_day, "COUNT(sine) per day must be 1440");
        // average of one full day sine should be ~0
        assert!(avg.abs() < 1e-6, "daily AVG(sine) expected ~0, got {}", avg);
    }
    tprintln!(
        "integration(query BY 1d): days={}, elapsed_ms={}",
        rows2.len(),
        t_q2.elapsed().as_millis()
    );

    // Dynamically delete database
    let t_del = Instant::now();
    let _ = execute_query(&shared, &format!("DATABASE DELETE {}", db)).await.unwrap();
    tprintln!(
        "integration(db delete): db={}, elapsed_ms={}",
        db,
        t_del.elapsed().as_millis()
    );

    // Post-delete: a SELECT should return zero rows (or error). We tolerate either behavior.
    let t_post = Instant::now();
    let sel_after_delete = execute_query2(&shared, &format!("SELECT COUNT(sine) FROM {} BY 1d", db)).await;
    let post_ms = t_post.elapsed().as_millis();
    match sel_after_delete {
        Ok(val) => {
            let arr_len = val.as_array().map(|a| a.len()).unwrap_or(0);
            tprintln!("integration(post-delete select ok): rows={}, elapsed_ms={}", arr_len, post_ms);
            assert_eq!(arr_len, 0, "expected zero rows after deletion");
        }
        Err(e) => {
            tprintln!("integration(post-delete select error): err={}, elapsed_ms={}", e, post_ms);
        }
    }
}
