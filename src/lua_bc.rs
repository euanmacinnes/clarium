//! Lua bytecode cache with in-memory L1 and KV-backed L2 persistence.
//! Focus: high performance, minimal serialization (raw bytes in KV).

use std::collections::HashMap;
use std::sync::{Arc};
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock};
use xxhash_rust::xxh3::xxh3_64;
use anyhow::{Result, Context};

use crate::storage::{SharedStore, KvValue};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CacheKey {
    name: String,
    hash: String,
    abi: String,
}

#[derive(Clone)]
struct Entry { bytes: Arc<Vec<u8>>, size: usize }

struct Shard { map: RwLock<HashMap<CacheKey, Entry>> }

pub struct LuaBytecodeCache {
    shards: Vec<Shard>,
    compile_locks: Vec<Mutex<()>>, // striped per name
}

const N_SHARDS: usize = 64;
const N_LOCKS: usize = 256;

static GLOBAL: OnceCell<Arc<LuaBytecodeCache>> = OnceCell::new();

impl LuaBytecodeCache {
    pub fn global() -> Arc<Self> {
        GLOBAL.get_or_init(|| Arc::new(Self::new())).clone()
    }
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS { shards.push(Shard{ map: RwLock::new(HashMap::new()) }); }
        let mut compile_locks = Vec::with_capacity(N_LOCKS);
        for _ in 0..N_LOCKS { compile_locks.push(Mutex::new(())); }
        Self { shards, compile_locks }
    }

    #[inline]
    fn shard_idx(name: &str) -> usize { (xxh3_64(name.as_bytes()) as usize) & (N_SHARDS - 1) }
    #[inline]
    fn lock_idx(name: &str) -> usize { (xxh3_64(name.as_bytes()) as usize) & (N_LOCKS - 1) }

    /// Compute ABI salt based on mlua config and target.
    pub fn abi_salt() -> String {
        // Keep cheap and deterministic. Include lua54 feature and target arch.
        let lua = "lua54"; // compiled with mlua lua54 feature in Cargo.toml
        let arch = std::env::consts::ARCH;
        let endian = if cfg!(target_endian = "little") { "le" } else { "be" };
        format!("{}-{}-{}", lua, arch, endian)
    }

    /// Compute content hash over source and options; use fast XXH3 and hex for brevity.
    pub fn source_hash(abi: &str, strip_debug: bool, source: &str) -> String {
        let mut s = String::with_capacity(abi.len() + source.len() + 8);
        s.push_str(abi);
        s.push('|');
        s.push_str(if strip_debug { "1" } else { "0" });
        s.push('|');
        s.push_str(source);
        let h = xxh3_64(s.as_bytes());
        format!("{:016x}", h)
    }

    pub fn kv_key(abi: &str, name: &str, hash: &str) -> String {
        format!("lua.bc/{}/{}/{}", abi, name, hash)
    }

    /// Get bytecode or compile+dumps from source, persisting to KV under given database/store.
    /// strip_debug: if true, remove debug info in dump to reduce size.
    pub fn get_or_compile(
        &self,
        store: &SharedStore,
        database: &str,
        kv_store: &str,
        name: &str,
        source: &str,
        strip_debug: bool,
    ) -> Result<Arc<Vec<u8>>> {
        let norm = crate::scripts::ScriptRegistry::norm(name);
        let abi = Self::abi_salt();
        let hash = Self::source_hash(&abi, strip_debug, source);
        let key = CacheKey { name: norm.clone(), hash: hash.clone(), abi: abi.clone() };
        // L1 lookup
        let si = Self::shard_idx(&norm);
        if let Some(e) = self.shards[si].map.read().get(&key).cloned() { return Ok(e.bytes); }
        // L2 lookup (KV)
        let kv = store.kv_store(database, kv_store);
        let k = Self::kv_key(&abi, &norm, &hash);
        if let Some(bytes) = kv.get_bytes(&k) {
            let arc = Arc::new(bytes);
            self.shards[si].map.write().insert(key, Entry{ bytes: arc.clone(), size: arc.len() });
            return Ok(arc);
        }
        // Compile under per-key lock
        let li = Self::lock_idx(&norm);
        let _g = self.compile_locks[li].lock();
        // Recheck after acquiring lock
        if let Some(e) = self.shards[si].map.read().get(&key).cloned() { return Ok(e.bytes); }
        if let Some(bytes) = kv.get_bytes(&k) {
            let arc = Arc::new(bytes);
            self.shards[si].map.write().insert(key, Entry{ bytes: arc.clone(), size: arc.len() });
            return Ok(arc);
        }
        // Compile via mlua
        let bytes = Self::compile_dump(name, source, strip_debug)
            .with_context(|| format!("compile_dump failed for '{}':{}", name, source.lines().next().unwrap_or("")))?;
        kv.set_bytes(&k, &bytes, None, None);
        let arc = Arc::new(bytes);
        self.shards[si].map.write().insert(key, Entry{ bytes: arc.clone(), size: arc.len() });
        Ok(arc)
    }

    fn compile_dump(name: &str, source: &str, strip_debug: bool) -> Result<Vec<u8>> {
        use mlua::Lua;
        let lua = Lua::new();
        let func = lua.load(source).set_name(name).into_function()?;
        let dumped = func.dump(strip_debug);
        // Validate quickly by reloading
        let _f2 = lua.load(&dumped).into_function()?;
        Ok(dumped)
    }

    /// Invalidate all bytecode for a script name (all versions) in L1. Returns removed count.
    pub fn invalidate_name(&self, name: &str) -> usize {
        let norm = crate::scripts::ScriptRegistry::norm(name);
        let si = Self::shard_idx(&norm);
        let mut w = self.shards[si].map.write();
        let keys: Vec<CacheKey> = w.keys().filter(|k| k.name == norm).cloned().collect();
        let n = keys.len();
        for k in keys { w.remove(&k); }
        n
    }

    /// Invalidate entire L1 cache.
    pub fn invalidate_all(&self) -> usize {
        let mut total = 0;
        for sh in &self.shards {
            let mut w = sh.map.write();
            total += w.len();
            w.clear();
        }
        total
    }

    /// Delete persisted KV blobs for a given name under a database/store. Returns deleted count.
    pub fn purge_kv_for_name(&self, store: &SharedStore, database: &str, kv_store: &str, name: &str) -> usize {
        let norm = crate::scripts::ScriptRegistry::norm(name);
        let abi = Self::abi_salt();
        let prefix = format!("lua.bc/{}/{}/", abi, norm);
        let kv = store.kv_store(database, kv_store);
        kv.delete_prefix(&prefix)
    }
}

/// Default KV placement for script bytecode cache when no explicit store is provided.
pub const DEFAULT_DB: &str = "clarium";
pub const DEFAULT_KV_STORE: &str = "__scripts";
