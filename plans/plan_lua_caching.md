### Performance Study and Finalized Plan for Lua Bytecode Storage (In‑Memory + On‑Disk via KV)

---

### Executive Summary
- Primary objective: single-digit microseconds for hot-path function invocation after cache warmup; sub-millisecond warm-load from L1; low single-digit millisecond cold-load from KV; compile avoided unless source changed.
- Core tactics: deterministic keys; sharded lock-freeish read path; `Arc<Vec<u8>>` with preallocated capacity; binary KV values (avoid base64); per-key compile dedupe; TLS prepared functions; optional file-watch for proactive invalidation; robust `CLEAR SCRIPT CACHE` DDL.

---

### Data Model and Keys
- Cache Key: `(source_key, source_hash, abi_salt)`
  - `source_key`: `scripts.rs::norm(name)`; exported for reuse.
  - `source_hash`: `sha256(version|abi_salt|opts_flags|normalized_source_text)`
  - `abi_salt`: `lua_version|mlua_features|target_arch|endian|opt_level`
- KV keys: `lua.bc/<abi_salt>/<source_key>/<source_hash>` → bytes
- Metadata: `lua.meta/<source_key>` → `{ latest_hash, seen:[{hash, ts}], file:{path, mtime, size} }` (JSON, small)

Rationale: exact-match lookups, O(1) L1 map lookup, and O(1) KV fetch without scans. ABI salt prevents cross-version hazards.

---

### In‑Memory Cache (L1)
- Structure: N shard maps with `parking_lot::RwLock` per shard.
  - Shard index: `fxhash(source_key) & (N-1)`; N default 64; configurable.
- Entry: `{ bytes: Arc<Vec<u8>>, size: usize, last_used: AtomicU64, hits: AtomicU64 }`
- Policy: global soft limit by bytes and count; per-shard LRU ring with lock only on eviction.
- Read path (hot):
  - Compute shard → `read()` → get `Arc<Vec<u8>>` pointer → return; update `hits`/`last_used` via atomics (no write lock).
  - Expected latency: ~100–300 ns typical after CPU cache residency.
- Write path (miss):
  - Acquire per-key `Mutex` (striped) to dedupe compiles.
  - Insert under shard `write()`; trigger cooperative eviction if soft limit exceeded.
- Memory management:
  - Preallocate `Vec<u8>` to exact size at create; never reallocate (store dumped size first when available).
  - Optionally use a tiny byte-slab pool for frequent similar sizes (benefit marginal; keep as future toggle).

---

### KV Persistence (L2)
- Strong recommendation: add `KvValue::Bytes(Vec<u8>)` with direct `fs` write/read; disable JSON/base64 for bytecode.
- Storage layout:
  - File-per-key within KV store path; write via `write_all` to a temp then `rename` for atomicity.
  - Set `FILE_FLAG_SEQUENTIAL_SCAN`/`FILE_FLAG_WRITE_THROUGH` toggles based on size:
    - Small blobs (<64 KiB): default; rely on OS cache.
    - Large blobs: optional `mmap` read on Windows is limited; prefer buffered read with preallocated `Vec::with_capacity(len)` and `read_exact`.
- Read path:
  - Single `open` → `metadata` → preallocate buffer → `read_exact` → return `Vec<u8>`; wrap in `Arc`.
  - Expected latency (SSD): 0.2–1.0 ms for typical script sizes (1–50 KiB). Warm page cache: tens of microseconds.
- Write path:
  - `dump` result → write-through to KV; fsync only if durability required; otherwise rely on periodic fsync setting for throughput.

Fallback if `Bytes` not yet available:
- Transitional hex with `\x` prefix to minimize parsing ambiguity. But it adds ~2× size and ~35–80 µs encode/decode cost for 32 KiB; not acceptable for production. Use only for tests until `Bytes` lands.

---

### Compilation Service
- Use `mlua` APIs: `lua.load(source).set_name(name)?.into_function()?;` then `function.dump(strip_debug)`.
- Options in `CompileOpts`:
  - `strip_debug: bool = true` (smaller blobs)
  - `opt_level: u8` (if backend supports)
- Validate by reloading dumped bytecode into a fresh Lua state once to catch corruption.
- Deduplicate compiles via per-key mutex keyed by `(source_key, opts, abi_salt)`; contention-free for distinct keys.

Expected latencies:
- Dump: 0.2–3 ms typical for medium functions; scales with AST size. This is amortized away by caching.

---

### Execution Path and TLS Prepared Functions
- Keep L1 storing only the bytecode blob.
- For each thread/Lua state, maintain TLS map `(registry_stamp, cache_key)` → `mlua::Function`.
  - On invoke:
    - L1 bytes → `lua.load(&bytes).into_function()` (fast, no parse; bytecode load).
    - Cache `Function` in TLS for repeated calls until `registry_stamp` changes.
- Expected hot-call overhead after TLS warm: essentially just the Lua function call cost.

---

### Invalidation and Disk Change Detection
- On-access correctness: new source text → new `source_hash` → miss → recompile.
- Proactive invalidation:
  - Track `{path, mtime, size}` in `lua.meta/<name>`.
  - On lookup, `metadata()` fast check; if changed, invalidate L1 entries for `(name, *, abi)` and refresh.
  - Optional file watcher to bump a `scripts_registry_stamp` for instant TLS invalidation; best-effort only.
- DDL: `CLEAR SCRIPT CACHE` with scopes (`ALL`, `DATABASE`, `SCHEMA`, `NAME <ident>`) and optional `WITH PERSISTENT` to delete KV blobs too.

---

### Concurrency Model
- Sharded caches minimize lock contention.
- Per-key compile mutex prevents stampede on cold starts.
- Eviction under shard write lock; global stats via atomics.
- All operations are non-panicking; errors are propagated via `anyhow` with context; `tprintln!` logs for diagnostics.

---

### Observability (permanent, low overhead)
- Counters: `lua_bc_l1_hits`, `lua_bc_l2_hits`, `lua_bc_misses`, `lua_bc_compiles`, `lua_bc_evictions`, `lua_bc_bytes`, `lua_bc_entries`.
- Events via `tprintln!` and tracing:
  - `"[lua_bc] L1 hit {name}@{hash} size={n}"`
  - `"[lua_bc] L2 hit {kv_key} size={n}"`
  - `"[lua_bc] compile {name}@{hash} strip={b}"`
  - `"[lua_bc] invalidated {name} scope={...}"`
  - `"[lua_bc] clear DDL scope={...} persistent={bool}"`

---

### Defaults and Limits (tuned for performance)
- Shards: 64 (power of two).
- L1 memory cap: 128 MiB default; configurable 32 MiB–2 GiB.
- Max entries: 4096 default; per-name versions kept in L1: 2 (latest + previous).
- KV pruning: keep last 3 hashes per name per ABI; global cap e.g., 1–4 GiB per KV store.
- FSync policy: off by default for bytecode (treat as cache); enable via config for strict durability.

---

### Testing and Benchmarks (targets)
- Unit:
  - Hash stability and ABI salt unit tests.
  - L1 hit/miss logic; eviction and per-name versioning.
  - Error paths: corrupt KV value → recompile fallback.
- Integration:
  - Cold: First call compiles and persists; subsequent process loads from KV without compile.
  - Disk-change: modify source file; next call yields new hash and recompile.
  - DDL: scope clearing and optional persistent delete.
- Microbench targets (on SSD, release mode):
  - L1 hot bytes→Function load: 5–30 µs
  - L1 hit returning bytes only: < 1 µs
  - L2 KV hit (warm OS cache): 20–150 µs for 10–50 KiB blobs
  - Cold file read (SSD): 0.3–1.2 ms for 10–50 KiB blobs
  - Compile (one-time): 0.5–5 ms (depends on function size)

---

### Minimal API Surfaces to Add
- `scripts.rs`:
  - `pub fn norm(name: &str) -> String` (or re-export) for key normalization.
  - `fn script_file_meta(name) -> Option<{path, mtime, size}>` when file-backed.
  - `fn bump_registry_stamp(scope)` used by invalidation and DDL.
- Cache facade:
  - `get_or_compile(name, source, opts) -> Arc<Vec<u8>>`
  - `invalidate_name(name)` / `invalidate_scope(db?, schema?)`
  - `clear_persistent(name?/scope?)` (KV delete)
  - `stats()`
- KV:
  - `KvValue::Bytes(Vec<u8>)`, plus `get_bytes/set_bytes/delete_prefix(prefix)` for efficient range deletes on `lua.bc/<abi>/<name>/`.

---

### Rollout Steps
1. Implement `KvValue::Bytes` and fast-path binary read/write; add tests.
2. Build L1 cache (sharded, LRU, limits) and public facade.
3. Wire compilation service with per-key dedupe and validation reload.
4. Integrate with scripts registry; add file metadata checks and optional watcher.
5. Add DDL parser+executor for `CLEAR SCRIPT CACHE` with scopes.
6. Add metrics, `tprintln!` hooks.
7. Ship tests/benchmarks; tune shard count, caps, and fsync policy.

---

### Risks and Mitigations
- Bytecode incompatibility → ABI salt segregation.
- KV growth → pruning policy and `CLEAR ... WITH PERSISTENT` option.
- Lock contention → shard maps, atomics for hot-path counters, per-key mutex for compile only.
- Serialization overhead → enforce `Bytes` KV; avoid base64 entirely in production.

---

### Final Notes
This plan maximizes performance by avoiding unnecessary copies and serialization, ensuring O(1) lookups, and keeping locks off the hot path. It reuses the existing KV infrastructure with a minimal extension for binary values, provides robust invalidation including disk changes and a clear DDL, and aligns with your guidelines for thin interfaces, observability, and graceful error handling.