use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_docs(tmp: &tempfile::TempDir, name: &str, n: usize) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    // Deterministic 3-dim vectors along x-axis with id starting at 1
    for i in 0..n {
        let id = (i + 1) as i64;
        let x = (i as f32) / 100.0;
        let v = format!("{:.2},0.0,0.0", x);
        let mut m = serde_json::Map::new();
        m.insert("id".into(), json!(id));
        m.insert("body_embed".into(), json!(v));
        recs.push(Record { _time: id, sensors: m });
    }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

#[test]
fn ann_heap_topk_matches_exact_limit() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/docs";
    let shared = seed_docs(&tmp, db, 100);

    // Exact baseline: compute l2 to query [0.31,0,0] in projection, LIMIT 5
    let exact_sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
                     SELECT id, vec_l2(body_embed, (SELECT v FROM q)) AS d \
                     FROM clarium/public/docs \
                     ORDER BY d ASC LIMIT 5";
    let exact_q = match query::parse(exact_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_exact = run_select(&shared, &exact_q).unwrap();
    let exp_ids: Vec<i64> = df_exact.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();

    // ANN heap-based exact compute path: ORDER BY vec_l2(...) USING ANN LIMIT 5
    let ann_sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
                   SELECT id \
                   FROM clarium/public/docs \
                   ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN \
                   LIMIT 5";
    let ann_q = match query::parse(ann_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_ann = run_select(&shared, &ann_q).unwrap();
    let ann_ids: Vec<i64> = df_ann.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();
    assert_eq!(exp_ids, ann_ids);
}

#[test]
fn ann_heap_topk_desc_cosine_matches_exact_limit() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/docs";
    let shared = seed_docs(&tmp, db, 50);

    // Exact cosine DESC baseline with LIMIT 3
    let exact_sql = "WITH q AS (SELECT to_vec('[0.20,0,0]') v) \
                     SELECT id, cosine_sim(body_embed, (SELECT v FROM q)) AS cs \
                     FROM clarium/public/docs \
                     ORDER BY cs DESC LIMIT 3";
    let exact_q = match query::parse(exact_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_exact = run_select(&shared, &exact_q).unwrap();
    let exp_ids: Vec<i64> = df_exact.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();

    let ann_sql = "WITH q AS (SELECT to_vec('[0.20,0,0]') v) \
                   SELECT id \
                   FROM clarium/public/docs \
                   ORDER BY cosine_sim(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN \
                   LIMIT 3";
    let ann_q = match query::parse(ann_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_ann = run_select(&shared, &ann_q).unwrap();
    let ann_ids: Vec<i64> = df_ann.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();
    assert_eq!(exp_ids, ann_ids);
}
