use super::*;
use std::fs;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_root() -> std::path::PathBuf {
    let mut base = std::env::temp_dir();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let pid = std::process::id();
    base.push(format!("clarium_tests_{}_{}", pid, ts));
    base
}

fn write_file(path: &std::path::Path, text: &str) {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent).unwrap(); }
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(text.as_bytes()).unwrap();
}

#[test]
fn show_scripts_lists_scalar_and_aggregate() {
    // Arrange: temp db root with one db/schema and two scripts
    let root = unique_temp_root();
    fs::create_dir_all(&root).unwrap();
    let db = "db1";
    let schema = "public";
    let scripts_root = scripts_dir_for(&root, db, schema);
    let scalar_path = scripts_root.join("scalars").join("hello.lua");
    let agg_path = scripts_root.join("aggregates").join("sum.lua");
    write_file(&scalar_path, "function hello(x) return x end");
    write_file(&agg_path, "function sum(x,y) return x+y end");

    let store = crate::storage::SharedStore::new(root.to_string_lossy().as_ref()).unwrap();

    // Act: execute SHOW SCRIPTS through the public API
    let rt = tokio::runtime::Runtime::new().unwrap();
    let val = rt.block_on(crate::server::exec::execute_query(&store, "SHOW SCRIPTS")).unwrap();

    // Assert: find both entries
    let arr = match val { serde_json::Value::Array(a) => a, _ => panic!("SHOW SCRIPTS did not return an array: {:?}", val) };
    let mut has_scalar = false;
    let mut has_agg = false;
    for row in &arr {
        if let serde_json::Value::Object(m) = row {
            let dbv = m.get("db").and_then(|v| v.as_str()).unwrap_or("");
            let scv = m.get("schema").and_then(|v| v.as_str()).unwrap_or("");
            let name = m.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let kind = m.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            if dbv == db && scv == schema && name == "hello" && kind == "scalar" { has_scalar = true; }
            if dbv == db && scv == schema && name == "sum" && kind == "aggregate" { has_agg = true; }
        }
    }

    if !(has_scalar && has_agg) {
        // Fallback diagnostics: scan filesystem directly as the SHOW implementation would
        let sdir = scripts_dir_for(&root, db, schema);
        let mut expected: Vec<serde_json::Value> = Vec::new();
        for sub in ["scalars", "aggregates"] {
            let subd = sdir.join(sub);
            if subd.exists() {
                for ent in fs::read_dir(&subd).unwrap().flatten() {
                    let p = ent.path();
                    if p.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") {
                        let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                        let kind = if sub == "aggregates" { "aggregate" } else { "scalar" };
                        expected.push(serde_json::json!({"db": db, "schema": schema, "name": name, "kind": kind}));
                    }
                }
            }
        }
        panic!(
            "SHOW SCRIPTS mismatch.\nExpected entries (from filesystem): {:#?}\nActual rows: {:#?}",
            expected, arr
        );
    }

    // Cleanup best-effort
    let _ = fs::remove_dir_all(&root);
}
