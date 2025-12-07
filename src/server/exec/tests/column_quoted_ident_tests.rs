use crate::server::data_context::DataContext;
use crate::server::exec::select_stages::from_where::from_where as stage_from_where;
use crate::server::query::{self, Command};
use crate::storage::{Store, SharedStore, Record};
use serde_json::json;

#[test]
fn resolve_alias_quoted_column_with_dot_inside() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let tpath = "clarium/public/qt.time";
    // write a few points with sensor key containing a dot: "x.y"
    let base: i64 = 1_700_123_000_000;
    for i in 0..2i64 {
        let mut m = serde_json::Map::new();
        m.insert("x.y".into(), json!((i + 1) as i64));
        m.insert("v".into(), json!((i + 10) as i64));
        store.write_records(tpath, &vec![Record { _time: base + i * 1000, sensors: m }]).unwrap();
    }
    let qtxt = "SELECT a._time FROM clarium.public.qt.time a";
    let q = match query::parse(qtxt).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let mut ctx = DataContext::with_defaults("clarium", "public");
    let df = stage_from_where(&shared, &q, &mut ctx).expect("from_where ok");
    // Ensure the DataFrame contains aliased columns a._time, a.v, and a.x.y
    let cols = df.get_column_names();
    assert!(cols.iter().any(|c| c.as_str() == "a._time"));
    assert!(cols.iter().any(|c| c.as_str() == "a.v"));
    assert!(cols.iter().any(|c| c.as_str() == "a.x.y"));

    // Now resolve quoted column alias."x.y"
    let resolved = ctx.resolve_column(&df, "a.\"x.y\"").unwrap();
    assert_eq!(resolved, "a.x.y");

    // Also ensure case-insensitive alias reference works
    let resolved2 = ctx.resolve_column(&df, "A.V").unwrap();
    assert_eq!(resolved2, "a.v");
}
