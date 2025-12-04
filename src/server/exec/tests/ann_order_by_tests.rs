use crate::server::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

fn seed_docs(tmp: &tempfile::TempDir, name: &str) -> SharedStore {
    let store = Store::new(tmp.path()).unwrap();
    let mut recs: Vec<Record> = Vec::new();
    // Deterministic 3-dim vectors along x-axis
    let rows = vec![
        (1, "0.1,0.0,0.0"),
        (2, "0.2,0.0,0.0"),
        (3, "0.3,0.0,0.0"),
        (4, "0.4,0.0,0.0"),
    ];
    for (i, v) in rows { let mut m = serde_json::Map::new(); m.insert("id".into(), json!(i)); m.insert("body_embed".into(), json!(v)); recs.push(Record { _time: i as i64, sensors: m }); }
    store.write_records(name, &recs).unwrap();
    SharedStore::new(tmp.path()).unwrap()
}

fn write_vindex(store: &SharedStore, name: &str, table: &str, column: &str, metric: &str, dim: i32) {
    let mut p = store.0.lock().root_path().clone();
    let local = name.replace('/', std::path::MAIN_SEPARATOR_STR);
    p.push(local);
    p.set_extension("vindex");
    let vf = serde_json::json!({
        "version": 1,
        "name": name,
        "qualified": name,
        "table": table,
        "column": column,
        "algo": "hnsw",
        "metric": metric,
        "dim": dim,
        "params": {"M": 32, "ef_build": 200, "ef_search": 64},
        "status": {"state": "ready"},
        "created_at": "2025-01-01T00:00:00Z"
    });
    std::fs::create_dir_all(p.parent().unwrap()).ok();
    std::fs::write(&p, serde_json::to_string_pretty(&vf).unwrap()).unwrap();
}

#[test]
fn ann_orders_like_exact_and_honors_limit() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/docs";
    let shared = seed_docs(&tmp, db);
    write_vindex(&shared, "clarium/public/idx_docs_body", db, "body_embed", "l2", 3);

    // Exact baseline: compute l2 to query [0.31,0,0] in projection
    let exact_sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
                     SELECT id, vec_l2(body_embed, (SELECT v FROM q)) AS d \
                     FROM clarium/public/docs \
                     ORDER BY d ASC LIMIT 2";
    let exact_q = match query::parse(exact_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_exact = run_select(&shared, &exact_q).unwrap();
    let exp_first = df_exact.column("id").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();

    // ANN: ORDER BY vec_l2(col, q) USING ANN
    let ann_sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
                   SELECT id \
                   FROM clarium/public/docs \
                   ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN \
                   LIMIT 2";
    let ann_q = match query::parse(ann_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_ann = run_select(&shared, &ann_q).unwrap();
    let ann_first = df_ann.column("id").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
    assert_eq!(exp_first, ann_first);
    assert_eq!(df_ann.height(), 2);
}

#[test]
fn ann_respects_desc_and_ignores_second_key_for_ann() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/docs";
    let shared = seed_docs(&tmp, db);
    write_vindex(&shared, "clarium/public/idx_docs_body", db, "body_embed", "l2", 3);

    // Exact baseline with two keys: distance ASC then id DESC
    let exact_sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
                     SELECT id, vec_l2(body_embed, (SELECT v FROM q)) AS d \
                     FROM clarium/public/docs \
                     ORDER BY d ASC, id DESC LIMIT 3";
    let exact_q = match query::parse(exact_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_exact = run_select(&shared, &exact_q).unwrap();
    let exp_ids: Vec<i64> = df_exact.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();

    // ANN with same keys: first key handled by ANN, second key (id DESC) applied by exact
    let ann_sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
                   SELECT id \
                   FROM clarium/public/docs \
                   ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN, id DESC \
                   LIMIT 3";
    let ann_q = match query::parse(ann_sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df_ann = run_select(&shared, &ann_q).unwrap();
    let ann_ids: Vec<i64> = df_ann.column("id").unwrap().i64().unwrap().into_no_null_iter().collect();
    assert_eq!(exp_ids, ann_ids);
}

#[test]
fn ann_hint_exact_forces_exact_and_missing_index_fallback() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/docs";
    let shared = seed_docs(&tmp, db);
    // No index present: should fallback to exact without error
    let sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
               SELECT id \
               FROM clarium/public/docs \
               ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN \
               LIMIT 1";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 1);

    // Now create an index but force EXACT
    write_vindex(&shared, "clarium/public/idx_docs_body", db, "body_embed", "l2", 3);
    let sql2 = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
               SELECT id \
               FROM clarium/public/docs \
               ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING EXACT \
               LIMIT 1";
    let q2 = match query::parse(sql2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(df2.height(), 1);
}

#[test]
fn ann_metric_dim_mismatch_fallback_and_rhs_subquery() {
    super::udf_common::init_all_test_udfs();
    let tmp = tempfile::tempdir().unwrap();
    let db = "clarium/public/docs";
    let shared = seed_docs(&tmp, db);
    // cosine index but L2 order: mismatch -> fallback to exact but still succeed
    write_vindex(&shared, "clarium/public/idx_docs_body", db, "body_embed", "cosine", 3);
    let sql = "WITH q AS (SELECT to_vec('[0.31,0,0]') v) \
               SELECT id \
               FROM clarium/public/docs \
               ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN \
               LIMIT 3";
    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&shared, &q).unwrap();
    assert_eq!(df.height(), 3);

    // Dim mismatch: index dim=3, query vector len=2 -> fallback
    let sql2 = "WITH q AS (SELECT to_vec('[0.31,0]') v) \
                SELECT id \
                FROM clarium/public/docs \
                ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN \
                LIMIT 2";
    let q2 = match query::parse(sql2).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&shared, &q2).unwrap();
    assert_eq!(df2.height(), 2);
}
