#[cfg(test)]
mod tests {
    use super::super::super::{run_select};
    use crate::server::query::{self, Command};
    use crate::storage::{Store, SharedStore, Record};
    use serde_json::json;

    #[test]
    fn test_where_like_basic_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let db = "ldb.time";
        // Write sample rows with a string column 'city'
        let base: i64 = 1_700_000_000_000;
        let cities = vec!["London", "Paris", "New York", "Newcastle", "Lagos", "Newton"];
        let mut recs: Vec<Record> = Vec::new();
        for (i, c) in cities.iter().enumerate() {
            let mut m = serde_json::Map::new();
            m.insert("city".into(), json!(*c));
            recs.push(Record { _time: base + i as i64, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        // Query with LIKE 'New%'
        let qtext = format!("SELECT COUNT(city) FROM {} WHERE city LIKE 'New%'", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 1);
        let cnt = df.column("COUNT(city)").unwrap().i64().unwrap().get(0).unwrap();
        // New York, Newcastle, Newton: 3
        assert_eq!(cnt, 3);
    }

    #[test]
    fn test_where_not_like_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let db = "ldb2.time";
        // Write sample rows with 'name'
        let base: i64 = 1_700_000_100_000;
        let names = vec!["alpha", "beta", "gamma", "delta", "omega", "gala"];
        let mut recs: Vec<Record> = Vec::new();
        for (i, n) in names.iter().enumerate() {
            let mut m = serde_json::Map::new();
            m.insert("name".into(), json!(*n));
            recs.push(Record { _time: base + i as i64, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        // NOT LIKE '%ma' should exclude only strings ending with 'ma' (gamma)
        let qtext = format!("SELECT COUNT(name) FROM {} WHERE name NOT LIKE '%ma'", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        assert_eq!(df.height(), 1);
        let cnt = df.column("COUNT(name)").unwrap().i64().unwrap().get(0).unwrap();
        assert_eq!(cnt, 5);
    }

    #[test]
    fn test_where_like_underscore_single_char() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::new(tmp.path()).unwrap();
        let shared = SharedStore::new(tmp.path()).unwrap();
        let db = "ldb3.time";
        // strings: a1b, a2b, a12b
        let base: i64 = 1_700_000_200_000;
        let vals = vec!["a1b", "a2b", "a12b", "acb"];
        let mut recs: Vec<Record> = Vec::new();
        for (i, v) in vals.iter().enumerate() {
            let mut m = serde_json::Map::new();
            m.insert("s".into(), json!(*v));
            recs.push(Record { _time: base + i as i64, sensors: m });
        }
        store.write_records(db, &recs).unwrap();
        // LIKE 'a_b' should match a1b, a2b, acb but not a12b
        let qtext = format!("SELECT COUNT(s) FROM {} WHERE s LIKE 'a_b'", db);
        let q = match query::parse(&qtext).unwrap() { Command::Select(q) => q, _ => unreachable!() };
        let df = run_select(&shared, &q).unwrap();
        let cnt = df.column("COUNT(s)").unwrap().i64().unwrap().get(0).unwrap();
        assert_eq!(cnt, 3);
    }
}



