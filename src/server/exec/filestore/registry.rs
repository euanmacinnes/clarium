//! Filestore registry and config persistence utilities.
//! Stores per-filestore configuration entries in the in-memory KV under info namespace.
//! Keep APIs thin; DDL layers can call these helpers.

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::storage::{KvValue, SharedStore};

use super::config::FilestoreConfig;
use super::kv::Keys;

/// Registry entry stored under `Keys::info_registry(db, fs)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FilestoreRegistryEntry {
    pub name: String,
    pub config: FilestoreConfig,
    pub config_version: u32,
    pub created_at: i64,
    pub updated_at: i64,
}

impl FilestoreRegistryEntry {
    pub fn new(name: &str, config: FilestoreConfig) -> Self {
        let now = Utc::now().timestamp();
        Self { name: name.to_string(), config, config_version: 1, created_at: now, updated_at: now }
    }
}

/// Update payload for `alter_filestore_entry` with all fields optional.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct FilestoreConfigUpdate {
    pub security_check_enabled: Option<bool>,
    pub acl_url: Option<Option<String>>,
    pub acl_auth_header: Option<Option<String>>,
    pub acl_timeout_ms: Option<Option<u64>>,
    pub acl_cache_ttl_allow_ms: Option<Option<u64>>,
    pub acl_cache_ttl_deny_ms: Option<Option<u64>>,
    pub acl_fail_open: Option<Option<bool>>,
    pub git_remote: Option<Option<String>>,
    pub git_branch: Option<Option<String>>,
    pub git_mode: Option<Option<String>>,
    pub git_backend: Option<Option<String>>,
    pub git_push_backend: Option<Option<String>>,
    pub lfs_patterns: Option<Option<String>>,
    pub html_description_max_bytes: Option<Option<usize>>,
}

/// Save (create or overwrite) a registry entry for a filestore.
pub fn save_filestore_entry(store: &SharedStore, database: &str, fs: &str, entry: &FilestoreRegistryEntry) -> Result<()> {
    let json = serde_json::to_value(entry)?;
    // Save under the filestore's own KV for direct lookups
    {
        let kv = store.kv_store(database, fs);
        let key = Keys::info_registry(database, fs);
        kv.set(key, KvValue::Json(json.clone()), None, None);
    }
    // Mirror into the database's default KV to enable global listing
    {
        let default_kv = store.kv_store(database, crate::lua_bc::DEFAULT_KV_STORE);
        let key = Keys::info_registry(database, fs);
        default_kv.set(key, KvValue::Json(json), None, None);
    }
    Ok(())
}

/// Load a registry entry if present.
pub fn load_filestore_entry(store: &SharedStore, database: &str, fs: &str) -> Result<Option<FilestoreRegistryEntry>> {
    let key = Keys::info_registry(database, fs);
    // Try filestore KV first
    {
        let kv = store.kv_store(database, fs);
        if let Some(KvValue::Json(j)) = kv.get(&key) { return Ok(serde_json::from_value(j).ok()); }
    }
    // Fallback to default KV mirror
    {
        let default_kv = store.kv_store(database, crate::lua_bc::DEFAULT_KV_STORE);
        if let Some(KvValue::Json(j)) = default_kv.get(&key) { return Ok(serde_json::from_value(j).ok()); }
    }
    Ok(None)
}

/// List all filestore entries registered for a database by scanning the info registry prefix.
pub fn list_filestore_entries(store: &SharedStore, database: &str) -> Result<Vec<FilestoreRegistryEntry>> {
    // We don't know all filestore names a priori; use the default KV for scanning.
    // Convention: registry entries are kept in each filestore's KV. To enable listing, we iterate
    // through available KV store names via root store keys. As we don't have a catalog here,
    // fall back to probing known KV names from `store.keys()` API at per-store level.
    // Implementation note: our KvStore API doesn't expose list of stores; instead, emulate by
    // reading all keys across default KV (database + DEFAULT_KV store). For now, we probe a set
    // of likely filestore names by scanning root KV stores known to the application is out of scope.
    // Hence, return entries only for the current `database` by scanning all keys from this process' KV
    // stores that the caller passes in.
    // Practical approach: iterate a guessed shortlist from current process — we don't have that here,
    // so we degrade gracefully by looking for any keys matching the registry prefix in the current
    // filestore equal to the database name (self-scan). Clients can also call `load_filestore_entry`
    // directly if they know the name.

    // Since we cannot enumerate KV stores, attempt to read registry entries from a conventional
    // in-database registry bucket: use DEFAULT_KV store co-located with `database` if available under the same accessor.
    let default_kv = store.kv_store(database, crate::lua_bc::DEFAULT_KV_STORE);
    let prefix = Keys::info_registry_prefix(database);
    let mut out = Vec::new();
    for k in default_kv.keys() {
        if !k.starts_with(&prefix) { continue; }
        if let Some(KvValue::Json(j)) = default_kv.get(&k) {
            if let Ok(ent) = serde_json::from_value::<FilestoreRegistryEntry>(j) { out.push(ent); }
        }
    }
    // Stable order by name
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Drop (remove) a filestore registry entry.
pub fn drop_filestore_entry(store: &SharedStore, database: &str, fs: &str) -> Result<bool> {
    let key = Keys::info_registry(database, fs);
    let kv = store.kv_store(database, fs);
    let a = kv.delete(&key);
    let default_kv = store.kv_store(database, crate::lua_bc::DEFAULT_KV_STORE);
    let b = default_kv.delete(&key);
    Ok(a || b)
}

/// Alter a registry entry using an update payload; bumps `config_version` and `updated_at`.
pub fn alter_filestore_entry(store: &SharedStore, database: &str, fs: &str, update: FilestoreConfigUpdate) -> Result<Option<FilestoreRegistryEntry>> {
    if let Some(mut ent) = load_filestore_entry(store, database, fs)? {
        // Apply updates field-by-field
        if let Some(v) = update.security_check_enabled { ent.config.security_check_enabled = v; }
        if let Some(v) = update.acl_url { ent.config.acl_url = v; }
        if let Some(v) = update.acl_auth_header { ent.config.acl_auth_header = v; }
        if let Some(v) = update.acl_timeout_ms { ent.config.acl_timeout_ms = v; }
        if let Some(v) = update.acl_cache_ttl_allow_ms { ent.config.acl_cache_ttl_allow_ms = v; }
        if let Some(v) = update.acl_cache_ttl_deny_ms { ent.config.acl_cache_ttl_deny_ms = v; }
        if let Some(v) = update.acl_fail_open { ent.config.acl_fail_open = v; }
        if let Some(v) = update.git_remote { ent.config.git_remote = v; }
        if let Some(v) = update.git_branch { ent.config.git_branch = v; }
        if let Some(v) = update.git_mode { ent.config.git_mode = v; }
        if let Some(v) = update.git_backend { ent.config.git_backend = v; }
        if let Some(v) = update.git_push_backend { ent.config.git_push_backend = v; }
        if let Some(v) = update.lfs_patterns { ent.config.lfs_patterns = v; }
        if let Some(v) = update.html_description_max_bytes { ent.config.html_description_max_bytes = v; }

        ent.config_version = ent.config_version.saturating_add(1);
        ent.updated_at = Utc::now().timestamp();
        save_filestore_entry(store, database, fs, &ent)?;
        return Ok(Some(ent));
    }
    Ok(None)
}
