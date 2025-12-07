use std::path::PathBuf;

use crate::ident::{
    normalize_identifier,
    QueryDefaults,
    qualify_regular_ident,
    qualify_time_ident,
    to_local_path,
    is_kv_address,
};

// Micro-unit tests for identifier normalization and qualification logic.

#[test]
fn normalize_identifier_handles_quotes_and_case() {
    assert_eq!(normalize_identifier("Foo"), "foo");
    assert_eq!(normalize_identifier("  Foo  "), "foo");
    assert_eq!(normalize_identifier("\"MiXeD\""), "MiXeD");
    assert_eq!(normalize_identifier("\" spaced name \""), " spaced name ");
    assert_eq!(normalize_identifier("simple_name"), "simple_name");
}

#[test]
fn qualify_regular_ident_bare_and_partial() {
    let d = QueryDefaults::new("Clarium", "Public"); // ensure defaults are normalized
    // Bare table
    assert_eq!(
        qualify_regular_ident("People", &d),
        "clarium/public/people"
    );
    // schema.table
    assert_eq!(
        qualify_regular_ident("Sales.People", &d),
        "clarium/sales/people"
    );
    // db.schema.table
    assert_eq!(
        qualify_regular_ident("Acme.Sales.People", &d),
        "acme/sales/people"
    );
    // slash-separated inputs
    assert_eq!(
        qualify_regular_ident("acme/sales/People", &d),
        "acme/sales/people"
    );
    // extra segments after first 2 are preserved with '/'
    assert_eq!(
        qualify_regular_ident("acme/sales/dir/sub/people", &d),
        "acme/sales/dir/sub/people"
    );
}

#[test]
fn qualify_time_ident_appends_time_suffix_correctly() {
    let d = QueryDefaults::new("clarium", "public");
    // Bare base -> add .time
    assert_eq!(
        qualify_time_ident("sensors", &d),
        "clarium/public/sensors.time"
    );
    // base.time as dotted pair
    assert_eq!(
        qualify_time_ident("sensors.time", &d),
        "clarium/public/sensors.time"
    );
    // schema.base.time
    assert_eq!(
        qualify_time_ident("metrics.sensors.time", &d),
        "clarium/metrics/sensors.time"
    );
    // db.schema.base (no .time) -> appended
    assert_eq!(
        qualify_time_ident("acme.metrics.sensors", &d),
        "acme/metrics/sensors.time"
    );
    // slash separated with missing pieces
    assert_eq!(
        qualify_time_ident("metrics/sensors", &d),
        "clarium/metrics/sensors.time"
    );
    assert_eq!(
        qualify_time_ident("acme/metrics/sensors", &d),
        "acme/metrics/sensors.time"
    );
}

#[test]
fn qualify_ident_respects_changed_defaults() {
    let d1 = QueryDefaults::new("clarium", "public");
    let d2 = QueryDefaults::new("prod", "sales");
    assert_eq!(qualify_regular_ident("t1", &d1), "clarium/public/t1");
    assert_eq!(qualify_regular_ident("t1", &d2), "prod/sales/t1");
    assert_eq!(qualify_time_ident("t2", &d1), "clarium/public/t2.time");
    assert_eq!(qualify_time_ident("t2", &d2), "prod/sales/t2.time");
}

#[test]
fn qualify_ident_mixed_separators_and_whitespace() {
    let d = QueryDefaults::new("CLARIUM", "PUBLIC");
    assert_eq!(
        qualify_regular_ident("  sales.people  ", &d),
        "clarium/sales/people"
    );
    assert_eq!(
        qualify_time_ident("  sales/ people  ", &d),
        "clarium/sales/people.time"
    );
}

#[test]
fn to_local_path_sanitizes_segments() {
    let root = PathBuf::from("C:/tmp/root");
    let p = to_local_path(&root, "clarium/public/sensors.time");
    assert!(p.ends_with("clarium/public/sensors.time"), "{:?}", p);
    // tolerate relative and unsafe segments
    let p2 = to_local_path(&root, "./clarium/../clarium/public/./sensors.time");
    assert!(p2.ends_with("clarium/public/sensors.time"), "{:?}", p2);
    // backslashes are normalized
    let p3 = to_local_path(&root, "clarium\\public\\s.time");
    assert!(p3.ends_with("clarium/public/s.time"), "{:?}", p3);
}

#[test]
fn kv_address_detection() {
    assert!(is_kv_address("clarium.store.blob.key1"));
    assert!(!is_kv_address("clarium.public.t1"));
}

#[test]
fn qualify_time_ident_edge_cases() {
    let d = QueryDefaults::new("clarium", "public");
    // trailing .time token with only base
    assert_eq!(qualify_time_ident("base.time", &d), "clarium/public/base.time");
    // dotted three-part ending with .time
    assert_eq!(qualify_time_ident("db.sc.base.time", &d), "db/sc/base.time");
    // empty/whitespace becomes defaults with just .time
    assert_eq!(qualify_time_ident("", &d), "clarium/public/.time");
}
