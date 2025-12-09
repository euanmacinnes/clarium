use std::time::{Duration, Instant};
use std::collections::HashMap as StdHashMap;
use std::sync::{Arc, OnceLock};
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use serde_json::Value as JsonValue;
use crate::storage::{SharedStore};
use polars::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Value variants supported by the in-memory KV store.
/// Note: Parquet values are kept in-memory as Polars DataFrame.
#[derive(Clone)]
pub enum KvValue {
    Str(String),
    Int(i64),
    Json(JsonValue),
    ParquetDf(DataFrame),
    /// Raw binary value intended for high-performance blobs (e.g., Lua bytecode)
    Bytes(Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StoreSettings {
    /// Arbitrary settings for the store. Extendable for future features (e.g. replication).
    pub name: String,
    /// If true, a GET will reset the TTL for keys that were inserted with a TTL.
    pub reset_on_access_default: bool,
    /// Placeholder for future replication options
    #[serde(default)]
    pub replication: Option<serde_json::Value>,
    /// Optional persistence settings loaded from `<store dir>/store.json`.
    #[serde(default)]
    pub persistence: Option<PersistenceSettings>,
}

impl Default for StoreSettings {
    fn default() -> Self {
        Self { name: String::new(), reset_on_access_default: true, replication: None, persistence: Some(PersistenceSettings::default()) }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PersistenceSettings {
    /// Enable periodic snapshotting of this KV store to disk
    #[serde(default)]
    pub enabled: bool,
    /// Interval in milliseconds between snapshots
    #[serde(default = "PersistenceSettings::default_interval_ms")]
    pub interval_ms: u64,
    /// Snapshot format: currently supports "bincode" (fast, compact)
    #[serde(default = "PersistenceSettings::default_format")]
    pub format: String,
}

impl PersistenceSettings {
    fn default_interval_ms() -> u64 { 5_000 }
    fn default_format() -> String { "bincode".to_string() }
}

impl Default for PersistenceSettings {
    fn default() -> Self {
        Self { enabled: false, interval_ms: Self::default_interval_ms(), format: Self::default_format() }
    }
}

#[derive(Clone)]
struct Entry {
    value: KvValue,
    /// Optional original TTL for resets
    ttl: Option<Duration>,
    /// Optional expiry time
    expires_at: Option<Instant>,
    /// If true for this key, accesses reset TTL
    reset_on_access: bool,
}

/// A single named in-memory KV store.
#[derive(Clone)]
pub struct KvStore {
    pub(crate) settings: StoreSettings,
    dir: PathBuf,
    map: Arc<parking_lot::RwLock<StdHashMap<String, Entry>>>,
    /// Guard to ensure we only spawn one persistence thread
    persist_started: Arc<parking_lot::Mutex<bool>>,
}

impl KvStore {
    pub(crate) fn new(dir: PathBuf, settings: StoreSettings) -> Self {
        std::fs::create_dir_all(&dir).ok();
        let s = Self { settings, dir, map: Arc::new(parking_lot::RwLock::new(StdHashMap::new())), persist_started: Arc::new(packing_lot_mutex()) };
        // Start persistence loop if enabled
        s.ensure_persistence_loop();
        s
    }

    fn config_path(&self) -> PathBuf { self.dir.join("store.json") }
    fn legacy_config_path(&self) -> PathBuf { self.dir.join("config.json") }
    fn snapshot_path(&self) -> PathBuf { self.dir.join("snapshot.bin") }
    fn parquet_dir(&self) -> PathBuf { self.dir.join("parquet") }

    pub fn load_or_default(dir: PathBuf, name: &str) -> Self {
        let cfg_new = dir.join("store.json");
        let cfg_legacy = dir.join("config.json");
        let mut settings = StoreSettings::default();
        settings.name = name.to_string();
        if let Ok(bytes) = std::fs::read(&cfg_new) {
            if let Ok(s) = serde_json::from_slice::<StoreSettings>(&bytes) { settings = s; }
        } else if let Ok(bytes) = std::fs::read(&cfg_legacy) {
            // migrate legacy
            if let Ok(mut s) = serde_json::from_slice::<StoreSettings>(&bytes) {
                s.name = name.to_string();
                settings = s;
                // persist to new path
                let _ = std::fs::write(&cfg_new, serde_json::to_vec_pretty(&settings).unwrap_or_default());
            }
        }
        Self::new(dir, settings)
    }

    fn ensure_persistence_loop(&self) {
        let mut started = self.persist_started.lock();
        if *started { return; }
        *started = true;
        drop(started);
        // Spawn background snapshot thread if enabled
        if self.settings.persistence.as_ref().map(|p| p.enabled).unwrap_or(false) {
            let interval = self.settings.persistence.as_ref().map(|p| p.interval_ms).unwrap_or(5_000);
            let this = self.clone();
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(interval));
                    let _ = this.save_snapshot();
                }
            });
        }
    }

    pub fn save_settings(&self) -> anyhow::Result<()> {
        let path = self.config_path();
        let bytes = serde_json::to_vec_pretty(&self.settings)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }

    fn save_snapshot(&self) -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize)]
        enum SnapVal { Str(String), Int(i64), Json(Vec<u8>), Bytes(Vec<u8>), Parquet { rel_path: String } }
        #[derive(Serialize, Deserialize)]
        struct SnapEntry { key: String, val: SnapVal, ttl_ms: Option<u64>, remaining_ms: Option<u64>, reset_on_access: bool }
        #[derive(Serialize, Deserialize)]
        struct Snapshot { version: u32, created_ms: i64, entries: Vec<SnapEntry> }

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
        let mut entries: Vec<SnapEntry> = Vec::new();
        let parquet_dir = self.parquet_dir();
        std::fs::create_dir_all(&parquet_dir).ok();
        for (k, v) in self.map.read().iter() {
            let (val, ttl_ms, remaining_ms) = match &v.value {
                KvValue::Str(s) => (SnapVal::Str(s.clone()), v.ttl.map(|d| d.as_millis() as u64), v.expires_at.map(|e| e.saturating_duration_since(Instant::now()).as_millis() as u64)),
                KvValue::Int(i) => (SnapVal::Int(*i), v.ttl.map(|d| d.as_millis() as u64), v.expires_at.map(|e| e.saturating_duration_since(Instant::now()).as_millis() as u64)),
                KvValue::Json(j) => (SnapVal::Json(serde_json::to_vec(j).unwrap_or_default()), v.ttl.map(|d| d.as_millis() as u64), v.expires_at.map(|e| e.saturating_duration_since(Instant::now()).as_millis() as u64)),
                KvValue::Bytes(b) => (SnapVal::Bytes(b.clone()), v.ttl.map(|d| d.as_millis() as u64), v.expires_at.map(|e| e.saturating_duration_since(Instant::now()).as_millis() as u64)),
                KvValue::ParquetDf(df) => {
                    let fname = format!("{}.parquet", sanitize_filename(k));
                    let path = parquet_dir.join(&fname);
                    if let Ok(mut f) = std::fs::File::create(&path) {
                        let _ = ParquetWriter::new(&mut f).finish(&mut df.clone());
                    }
                    (SnapVal::Parquet { rel_path: format!("parquet/{}", fname) }, v.ttl.map(|d| d.as_millis() as u64), v.expires_at.map(|e| e.saturating_duration_since(Instant::now()).as_millis() as u64))
                }
            };
            entries.push(SnapEntry { key: k.clone(), val, ttl_ms, remaining_ms, reset_on_access: v.reset_on_access });
        }
        let snap = Snapshot { version: 1, created_ms: now_ms, entries };
        let bytes = bincode::serialize(&snap)?;
        let tmp = self.snapshot_path().with_extension("bin.tmp");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(tmp, self.snapshot_path())?;
        Ok(())
    }

    /// Load snapshot from disk into memory; ignores errors to allow startup.
    pub fn load_snapshot(&self) -> anyhow::Result<()> {
        if !self.snapshot_path().exists() { return Ok(()); }
        #[derive(Serialize, Deserialize)]
        enum SnapVal { Str(String), Int(i64), Json(Vec<u8>), Bytes(Vec<u8>), Parquet { rel_path: String } }
        #[derive(Serialize, Deserialize)]
        struct SnapEntry { key: String, val: SnapVal, ttl_ms: Option<u64>, remaining_ms: Option<u64>, reset_on_access: bool }
        #[derive(Serialize, Deserialize)]
        struct Snapshot { version: u32, created_ms: i64, entries: Vec<SnapEntry> }
        let bytes = std::fs::read(self.snapshot_path())?;
        let snap: Snapshot = bincode::deserialize(&bytes)?;
        let now = Instant::now();
        let mut w = self.map.write();
        w.clear();
        for e in snap.entries.into_iter() {
            let kv = match e.val {
                SnapVal::Str(s) => KvValue::Str(s),
                SnapVal::Int(i) => KvValue::Int(i),
                SnapVal::Json(b) => {
                    let j: JsonValue = serde_json::from_slice(&b).unwrap_or(JsonValue::Null);
                    KvValue::Json(j)
                }
                SnapVal::Bytes(b) => KvValue::Bytes(b),
                SnapVal::Parquet { rel_path } => {
                    let p = self.dir.join(rel_path);
                    match polars::prelude::ParquetReader::new(std::fs::File::open(&p)?).finish() {
                        Ok(df) => KvValue::ParquetDf(df),
                        Err(_) => KvValue::Bytes(Vec::new()),
                    }
                }
            };
            let ttl = e.ttl_ms.map(|ms| Duration::from_millis(ms));
            let expires_at = match (ttl, e.remaining_ms) {
                (Some(_), Some(rem)) if rem > 0 => Some(now + Duration::from_millis(rem)),
                (Some(d), _) => Some(now + d), // fallback if missing remaining
                _ => None,
            };
            w.insert(e.key, Entry { value: kv, ttl, expires_at, reset_on_access: e.reset_on_access });
        }
        Ok(())
    }

    /// Set a key with optional TTL and per-key reset-on-access flag (defaults from store settings).
    pub fn set(&self, key: impl Into<String>, value: KvValue, ttl: Option<Duration>, reset_on_access: Option<bool>) {
        let key = key.into();
        let now = Instant::now();
        let reset = reset_on_access.unwrap_or(self.settings.reset_on_access_default);
        let expires_at = ttl.map(|d| now + d);
        let ent = Entry { value, ttl, expires_at, reset_on_access: reset };
        let mut w = self.map.write();
        w.insert(key, ent);
    }

    /// Convenience: store raw bytes without extra allocations by cloning once into the map.
    pub fn set_bytes(&self, key: impl Into<String>, bytes: &[u8], ttl: Option<Duration>, reset_on_access: Option<bool>) {
        self.set(key, KvValue::Bytes(bytes.to_vec()), ttl, reset_on_access);
    }

    /// Get a key. If expired, removes it and returns None. If reset_on_access, bumps expiry.
    pub fn get(&self, key: &str) -> Option<KvValue> {
        // First prune single key quickly
        let mut to_reset: Option<(String, Instant)> = None;
        {
            let r = self.map.read();
            if let Some(ent) = r.get(key) {
                if let Some(exp) = ent.expires_at {
                    if Instant::now() >= exp { /* expired */ }
                    else if ent.reset_on_access {
                        if let Some(ttl) = ent.ttl { to_reset = Some((key.to_string(), Instant::now() + ttl)); }
                    }
                }
            } else {
                return None;
            }
        }
        // Apply reset-on-access before checking expiry to avoid flakiness around the boundary
        let mut w = self.map.write();
        if let Some((k, new_exp)) = to_reset {
            if let Some(ent_mut) = w.get_mut(&k) { ent_mut.expires_at = Some(new_exp); }
        }
        // If expired (after potential reset), remove and return None
        if let Some(ent) = w.get(key) {
            if let Some(exp) = ent.expires_at { if Instant::now() >= exp { w.remove(key); return None; } }
        } else { return None; }
        w.get(key).map(|e| e.value.clone())
    }

    /// Convenience: get raw bytes if the stored value is binary.
    pub fn get_bytes(&self, key: &str) -> Option<Vec<u8>> {
        match self.get(key) {
            Some(KvValue::Bytes(b)) => Some(b),
            _ => None,
        }
    }

    pub fn delete(&self, key: &str) -> bool { self.map.write().remove(key).is_some() }
    pub fn clear(&self) { self.map.write().clear(); }
    pub fn len(&self) -> usize { self.map.read().len() }
    /// Return a snapshot of all keys in this store
    pub fn keys(&self) -> Vec<String> { self.map.read().keys().cloned().collect() }

    /// Delete keys that start with the provided prefix. Returns number of removed keys.
    pub fn delete_prefix(&self, prefix: &str) -> usize {
        let mut w = self.map.write();
        let to_remove: Vec<String> = w.keys().filter(|k| k.starts_with(prefix)).cloned().collect();
        let n = to_remove.len();
        for k in to_remove { w.remove(&k); }
        n
    }

    /// Remove expired keys. Returns number removed.
    pub fn sweep(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        let mut w = self.map.write();
        let keys: Vec<String> = w.iter()
            .filter_map(|(k, v)| v.expires_at.map(|exp| (k.clone(), exp)))
            .filter(|(_, exp)| now >= *exp)
            .map(|(k, _)| k)
            .collect();
        for k in keys { if w.remove(&k).is_some() { removed += 1; } }
        removed
    }

    /// Rename a key within this store. Returns true if the source existed and was moved.
    pub fn rename_key(&self, from: &str, to: &str) -> bool {
        if from == to { return true; }
        let mut w = self.map.write();
        if let Some(entry) = w.remove(from) {
            w.insert(to.to_string(), entry);
            true
        } else { false }
    }
}

/// Registry of KV stores per database under the root path.
#[derive(Clone)]
pub struct KvStoresRegistry {
    root: PathBuf,
    /// db_name -> (store_name -> KvStore)
    inner: Arc<parking_lot::RwLock<StdHashMap<String, StdHashMap<String, KvStore>>>>,
}

impl KvStoresRegistry {
    fn new(root: PathBuf) -> Self { Self { root, inner: Arc::new(parking_lot::RwLock::new(StdHashMap::new())) } }

    fn stores_dir_for_db(&self, db: &str) -> PathBuf { self.root.join(db).join("stores") }

    /// List existing KV stores for a database by scanning the filesystem under <db>/stores
    pub fn list_stores(&self, database: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let dir = self.stores_dir_for_db(database);
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for ent in rd.flatten() {
                if ent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    let name = ent.file_name().to_string_lossy().to_string();
                    if !name.starts_with('.') { out.push(name); }
                }
            }
        }
        out.sort();
        out
    }

    pub fn get_store(&self, database: &str, store_name: &str) -> KvStore {
        // Fast path read
        if let Some(st) = self.inner.read().get(database).and_then(|m| m.get(store_name)).cloned() { return st; }
        // Create path and load settings
        let dir = self.stores_dir_for_db(database).join(store_name);
        std::fs::create_dir_all(&dir).ok();
        let kv = KvStore::load_or_default(dir, store_name);
        let mut w = self.inner.write();
        let entry = w.entry(database.to_string()).or_default();
        entry.insert(store_name.to_string(), kv.clone());
        kv
    }

    /// Drop a store: remove from registry and delete its directory. Returns true if it existed.
    pub fn drop_store(&self, database: &str, store_name: &str) -> anyhow::Result<bool> {
        let dir = self.stores_dir_for_db(database).join(store_name);
        // Remove from cache first
        {
            let mut w = self.inner.write();
            if let Some(m) = w.get_mut(database) {
                m.remove(store_name);
            }
        }
        // Delete directory if exists
        if dir.exists() { std::fs::remove_dir_all(&dir).ok(); return Ok(true); }
        Ok(false)
    }

    /// Rename a store within a database: renames directory and updates registry cache.
    pub fn rename_store(&self, database: &str, from: &str, to: &str) -> anyhow::Result<()> {
        if from == to { return Ok(()); }
        let base = self.stores_dir_for_db(database);
        let src = base.join(from);
        let dst = base.join(to);
        std::fs::create_dir_all(&base).ok();
        if src.exists() {
            // If destination exists, error
            if dst.exists() { anyhow::bail!(format!("Target store already exists: {}", to)); }
            std::fs::rename(&src, &dst)?;
        } else {
            // Ensure destination exists for future use
            std::fs::create_dir_all(&dst).ok();
        }
        // Update cache: move KvStore if present, else lazy-load on next access
        let mut w = self.inner.write();
        if let Some(m) = w.get_mut(database) {
            if let Some(kv) = m.remove(from) {
                // Recreate with new dir, keeping settings but updating name
                let mut settings = kv.settings.clone();
                settings.name = to.to_string();
                let new_kv = KvStore::new(dst.clone(), settings);
                // Persist settings to config.json
                let _ = new_kv.save_settings();
                m.insert(to.to_string(), new_kv);
            }
        }
        Ok(())
    }

    /// Sweep all stores, return total removed count
    pub fn sweep_all(&self) -> usize {
        let mut total = 0;
        for (_db, m) in self.inner.read().iter() {
            for (_name, kv) in m.iter() { total += kv.sweep(); }
        }
        total
    }
}

static REGISTRIES: OnceLock<parking_lot::RwLock<StdHashMap<PathBuf, Arc<KvStoresRegistry>>>> = OnceLock::new();

fn kv_registry_for_root(root: &Path) -> Arc<KvStoresRegistry> {
    let root_key = root.to_path_buf();
    let map = REGISTRIES.get_or_init(|| parking_lot::RwLock::new(StdHashMap::new()));
    // fast path read
    if let Some(reg) = map.read().get(&root_key).cloned() { return reg; }
    // create
    let reg = Arc::new(KvStoresRegistry::new(root_key.clone()));
    map.write().insert(root_key, reg.clone());
    reg
}

impl SharedStore {
    pub fn new(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root_path = root.as_ref().to_path_buf();
        // Create the underlying store
        let s = Self(Arc::new(parking_lot::Mutex::new(crate::storage::Store::new(&root_path)?)));
        // One-time schema migration on startup for this root: upgrade legacy schema.json files
        // to nested { columns: {...}, locks: [...] } and ensure explicit tableType.
        let _ = crate::storage::schema::migrate_all_schemas_for_root(&root_path);
        // Seed and load system views (.view JSON format with column schemas)
        crate::system_views::load_system_views_for_root(&root_path);
        // Seed UDF scripts into <root>/.system/udf from repo scripts if missing
        crate::system_views::seed_udf_into_root(&root_path);
        // Initialize global ScriptRegistry once and load seeded UDFs so they are available for queries
        if let Ok(reg) = crate::scripts::ScriptRegistry::new() {
            crate::scripts::init_script_registry_once(reg.clone());
            let udf_root = crate::system_paths::udf_root(&root_path);
            let _ = crate::scripts::load_all_scripts_for_schema(&reg, &udf_root);
            // Also load global defaults from repo scripts to keep compatibility
            let _ = crate::scripts::load_global_default_scripts(&reg);
        }
        // Ensure a registry exists for this root (idempotent)
        let _ = kv_registry_for_root(&root_path);
        Ok(s)
    }

    /// Back-compat: return a clone of the root path of the underlying Store
    pub fn root_path(&self) -> std::path::PathBuf {
        let g = self.0.lock();
        g.root_path().clone()
    }

    pub fn kv_registry_for_root(&self) -> Arc<KvStoresRegistry> {
        let g = self.0.lock();
        let root = g.root_path().clone();
        drop(g);
        kv_registry_for_root(root.as_path())
    }

    /// Back-compat alias used across the codebase
    pub fn kv_registry(&self) -> Arc<KvStoresRegistry> { self.kv_registry_for_root() }

    pub fn kv_get_store(&self, database: &str, store_name: &str) -> KvStore {
        self.kv_registry_for_root().get_store(database, store_name)
    }

    /// Back-compat alias used across the codebase
    pub fn kv_store(&self, database: &str, store_name: &str) -> KvStore { self.kv_get_store(database, store_name) }
}

// small helper to construct default mutex for persist_started without importing Mutex type twice
fn packing_lot_mutex() -> parking_lot::Mutex<bool> { parking_lot::Mutex::new(false) }
