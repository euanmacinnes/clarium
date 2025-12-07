use crate::server::exec::exec_helpers::{
    qualify_identifier_with_defaults,
    qualify_identifier_regular_table_with_defaults,
    normalize_query_with_defaults,
};

#[test]
fn qualify_wrappers_match_ident_logic() {
    let q1 = qualify_identifier_with_defaults("metrics.sensors", "Clarium", "Public");
    assert_eq!(q1, "clarium/public/metrics.sensors.time");
    let q2 = qualify_identifier_regular_table_with_defaults("Metrics.Sensors", "CLARIUM", "PUBLIC");
    assert_eq!(q2, "clarium/public/metrics.sensors");
    // Already fully qualified should be preserved (normalized)
    let q3 = qualify_identifier_with_defaults("acme/metrics/sensors.time", "clarium", "public");
    assert_eq!(q3, "acme/metrics/sensors.time");
}

#[test]
fn normalize_query_with_defaults_handles_drop_and_rename() {
    // DROP TABLE
    let s = normalize_query_with_defaults("DROP TABLE people", "clarium", "public");
    assert_eq!(s, "DROP TABLE clarium/public/people");
    let s2 = normalize_query_with_defaults("DROP TABLE IF EXISTS people", "clarium", "public");
    assert_eq!(s2, "DROP TABLE IF EXISTS clarium/public/people");

    // RENAME TABLE
    let r = normalize_query_with_defaults("RENAME TABLE people TO folks", "clarium", "public");
    assert_eq!(r, "RENAME TABLE clarium/public/people TO clarium/public/folks");
    let r2 = normalize_query_with_defaults("RENAME TABLE clarium.public.people TO sales.People", "clarium", "public");
    assert_eq!(r2, "RENAME TABLE clarium/public/people TO clarium/sales/people");
}
