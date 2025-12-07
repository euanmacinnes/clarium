use super::super::execute_query;
use crate::lua_bc::{LuaBytecodeCache, DEFAULT_DB, DEFAULT_KV_STORE};
use crate::storage::{Store, SharedStore};

fn kv_keys_with_prefix(shared: &SharedStore, prefix: &str) -> Vec<String> {
    let kv = shared.kv_store(DEFAULT_DB, DEFAULT_KV_STORE);
    let mut keys: Vec<String> = kv
        .keys()
        .into_iter()
        .filter(|k| k.starts_with(prefix))
        .collect();
    keys.sort();
    keys
}

#[tokio::test]
async fn test_lua_bc_compile_and_persist() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    // Ensure default scripts KV exists by touching it
    let _ = shared.kv_store(DEFAULT_DB, DEFAULT_KV_STORE);

    let cache = LuaBytecodeCache::global();
    let name = "test_fn_compile";
    // Lua chunk that returns a function
    let source = r#"return function(a) return a + 1 end"#;

    let bytes1 = cache
        .get_or_compile(&shared, DEFAULT_DB, DEFAULT_KV_STORE, name, source, true)
        .expect("compile bytecode");

    // KV must contain the exact entry
    let abi = LuaBytecodeCache::abi_salt();
    let hash = LuaBytecodeCache::source_hash(&abi, true, source);
    let key = LuaBytecodeCache::kv_key(&abi, &crate::scripts::ScriptRegistry::norm(name), &hash);

    let kv = shared.kv_store(DEFAULT_DB, DEFAULT_KV_STORE);
    let kbytes = kv.get_bytes(&key).expect("kv has bytecode");
    assert_eq!(bytes1.len(), kbytes.len());
    assert_eq!(&*bytes1, &kbytes);
}

#[tokio::test]
async fn test_lua_bc_invalidate_and_warm_load_from_kv() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let cache = LuaBytecodeCache::global();
    let name = "test_fn_invalidate";
    let source = r#"return function(x) return x * 2 end"#;

    let b1 = cache
        .get_or_compile(&shared, DEFAULT_DB, DEFAULT_KV_STORE, name, source, true)
        .expect("compile");
    // Invalidate L1
    let removed = cache.invalidate_name(name);
    assert!(removed >= 1);
    // Fetch again; should succeed by hydrating from KV and be byte-equal
    let b2 = cache
        .get_or_compile(&shared, DEFAULT_DB, DEFAULT_KV_STORE, name, source, true)
        .expect("load from kv");
    assert_eq!(&*b1, &*b2);
}

#[tokio::test]
async fn test_clear_script_cache_name_with_persistent() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();

    let cache = LuaBytecodeCache::global();
    let name = "my_named_script";
    let source = r#"return function(s) return tostring(s) end"#;
    // Compile and persist
    let _ = cache
        .get_or_compile(&shared, DEFAULT_DB, DEFAULT_KV_STORE, name, source, true)
        .expect("compile");

    let abi = LuaBytecodeCache::abi_salt();
    let prefix = format!("lua.bc/{}/{}/", abi, crate::scripts::ScriptRegistry::norm(name));
    // Ensure KV has at least one key for this name
    let keys_before = kv_keys_with_prefix(&shared, &prefix);
    assert!(!keys_before.is_empty(), "expected kv entries for script");

    // Execute DDL to clear by name with persistent
    let res = execute_query(
        &shared,
        &format!("CLEAR SCRIPT CACHE NAME {} WITH PERSISTENT", name),
    )
    .await
    .unwrap();
    assert_eq!(res["status"], serde_json::json!("ok"));
    assert!(res["l1_cleared"].as_u64().unwrap_or(0) >= 1);
    assert!(res["l2_deleted"].as_u64().unwrap_or(0) >= 1);

    // KV should no longer contain entries under this prefix
    let keys_after = kv_keys_with_prefix(&shared, &prefix);
    assert!(keys_after.is_empty(), "expected kv entries deleted");

    // Subsequent get_or_compile should recreate new KV entry
    let _ = cache
        .get_or_compile(&shared, DEFAULT_DB, DEFAULT_KV_STORE, name, source, true)
        .expect("recompile");
    let keys_re = kv_keys_with_prefix(&shared, &prefix);
    assert!(!keys_re.is_empty());
}

#[tokio::test]
async fn test_clear_script_cache_all_nonpersistent_keeps_kv() {
    let tmp = tempfile::tempdir().unwrap();
    let _store = Store::new(tmp.path()).unwrap();
    let shared = SharedStore::new(tmp.path()).unwrap();
    let cache = LuaBytecodeCache::global();

    let name1 = "x_clear_all_1";
    let name2 = "x_clear_all_2";
    let source1 = r#"return function() return 1 end"#;
    let source2 = r#"return function() return 2 end"#;
    let _ = cache.get_or_compile(&shared, DEFAULT_DB, DEFAULT_KV_STORE, name1, source1, true).unwrap();
    let _ = cache.get_or_compile(&shared, DEFAULT_DB, DEFAULT_KV_STORE, name2, source2, true).unwrap();

    let abi = LuaBytecodeCache::abi_salt();
    let prefix1 = format!("lua.bc/{}/{}/", abi, crate::scripts::ScriptRegistry::norm(name1));
    let prefix2 = format!("lua.bc/{}/{}/", abi, crate::scripts::ScriptRegistry::norm(name2));
    assert!(!kv_keys_with_prefix(&shared, &prefix1).is_empty());
    assert!(!kv_keys_with_prefix(&shared, &prefix2).is_empty());

    // CLEAR SCRIPT CACHE (no ALL, no PERSISTENT) currently maps to CurrentSchema scope and persistent=false
    let res = execute_query(&shared, "CLEAR SCRIPT CACHE").await.unwrap();
    assert_eq!(res["status"], serde_json::json!("ok"));
    assert!(res["l1_cleared"].as_u64().unwrap_or(0) >= 2);
    assert_eq!(res["persistent"], serde_json::json!(false));

    // KV entries should still exist
    assert!(!kv_keys_with_prefix(&shared, &prefix1).is_empty());
    assert!(!kv_keys_with_prefix(&shared, &prefix2).is_empty());
}
