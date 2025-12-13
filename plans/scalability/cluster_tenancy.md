### Objective
Provide a single, integrated plan for a high‑availability (HA) scale‑out tenancy system that synchronizes schemas across servers via a WAL mechanism and supports `SYNC <tenancy>` for both DDL and read queries (`SELECT | MATCH | SHOW`). All read queries return a single merged result with an appended `tenancy` column.

---

### Concepts and Scope
- Tenancy: A globally unique, named logical group of servers and object scope used to coordinate cross‑server schema and read behaviors.
- Namespaces: `graph`, `table`, `time`, `filestore`. Tenancy scope can include any subset, with explicit object lists or patterns.
- Sync model: DDL executed under `SYNC <tenancy>` is transformed into a schema WAL (logical), globally ordered per tenancy. Members subscribe and apply idempotently in order.
- Read model: Queries under `SYNC <tenancy>` scatter to all active tenancy members, gather and merge results, and append a `tenancy` column.

---

### Command Surface
- Tenancy management:
  - `CREATE TENANCY <name> [WITH (namespaces=['table',...], objects=['ns.obj', ...])]`.
  - `ALTER TENANCY <name> SET SERVERS = {...}`; `ADD OBJECT <ns>.<obj>`; `DROP OBJECT ...`.
  - `SHOW TENANCY <name>;` – show members, scope, schema versions, lags, last errors.
- DDL envelope:
  - `SYNC <tenancy> CREATE/ALTER/DROP TABLE ...`
  - `SYNC <tenancy> CREATE/ALTER/DROP TIME TABLE ...`
  - `SYNC <tenancy> CREATE/ALTER/DROP GRAPH ...`
  - `SYNC <tenancy> filestore manifest/policy DDL ...`
- Read envelope:
  - `SYNC <tenancy> SELECT ...`
  - `SYNC <tenancy> MATCH ...`
  - `SYNC <tenancy> SHOW ...`
- Session scope:
  - `SET sync.tenancy = '<tenancy>'` (applies to DDL + SELECT/MATCH/SHOW) and `RESET sync.tenancy`.
  - `SET sync.read_policy = 'strong'|'timeline'|'eventual'` (default `timeline`).
  - `SET sync.append_tenancy_column = true|false` (default true for reads).

The `SYNC` wrapper is parsed as a thin envelope without altering inner syntax.

---

### Control Plane (Strongly Consistent)
- Storage backend: embedded meta‑raft cluster (default) or etcd/Consul.
- Data models:
  - `TenancyRegistry { name -> TenancyMeta { epoch, members, scope } }`
  - `TenancyMembership { tenancy -> { server_id -> member_info } }`
  - `TenancySchemaLog { tenancy -> ordered SchemaOp entries }`
  - `SchemaVersions { tenancy -> { qualified_object -> version } }`
- Epochs: Every membership/scope change increments `tenant_epoch`. `SchemaOp` carries the epoch to detect stale operations.
- Watchers: Routers and members subscribe to membership and schema WAL streams.

---

### Schema WAL and DDL Apply Flow
- `SchemaOp` (logical entry):
  - `tenant: String`, `op_id: Uuid`, `epoch: u64`, `ts`, `scope { ns, objects }`, `ddl_ir`, `preconditions { obj -> version }`, `apply_mode`, `retry_semantics`, optional `rollback_plan` for multi‑phase DDL.
- Coordinator path (router side):
  1) Parse inner DDL to canonical IR (per existing DDL modules).
  2) Validate tenancy membership and object scope; attach preconditions if requested.
  3) Append `SchemaOp` to `TenancySchemaLog` via meta‑store transaction; bump affected `SchemaVersions`.
  4) Wait per `apply_mode`: `AllMembers` (active), `Quorum(n)`, or `Timeboxed{quorum, timeout_ms}`.
- Member apply:
  - Subscribe to WAL; apply ops in order; persist `(tenant, op_id)` in local applied map and an `AppliedCursor` for replays; ack or record error with exponential backoff retry.
  - Never panic/bail for control flow; return structured errors and continue processing next ops.

---

### SYNC Reads: SELECT | MATCH | SHOW
- Router execution:
  1) Resolve active tenancy members per `sync.read_policy`:
     - Strong: prefer leaders with read‑index/linearizable reads.
     - Timeline: follower with lease‑validated read.
     - Eventual: any healthy replica.
  2) Build per‑member request with `{ tenancy, op_id, policy, budget }` where budget respects global `LIMIT/OFFSET`.
  3) Member executes the inner plan normally (per existing table/time/graph/filestore executors) against tenancy‑scoped objects.
  4) Router collects partials as frames/batches and performs final merge:
     - No ORDER BY: concat/interleave; apply global LIMIT/OFFSET.
     - With ORDER BY: k‑way merge or bounded heap top‑k; ensure `limit` typed as `IdxSize` for Polars.
  5) Append constant column `tenancy` (TEXT) to the final output, unless `sync.append_tenancy_column=false`. If a name collision exists, auto‑alias to `__tenancy`.
- Query kind specifics:
  - SELECT/time tables: tenancy scatter → server‑local partition scatter/gather → final merge. Projection pushdown avoids injecting the tenancy column into member plans unless referenced.
  - MATCH (graph): ensure global id space or use composite `(server_id, local_id)` for uniqueness when merging.
  - SHOW: list only tenancy‑scoped objects; deduplicate across members; append `tenancy` column.
- Pagination/cursors: maintain router‑side global cursors keyed by `{ tenant, op_id }`, with per‑member continuation tokens.

---

### Security & RBAC
- Roles/permissions:
  - `TENANCY_ADMIN`: create/alter/drop tenancy; manage members/scope.
  - `TENANCY_DDL`: execute `SYNC <tenancy>` DDL.
  - `TENANCY_READ`: execute `SYNC <tenancy> SELECT|MATCH|SHOW`.
- Audit: record actor, reason, and change ticket on every `SchemaOp` and SYNC read request (metadata only).
- Transport: mTLS per HA design; tenancy id included in authz context.

---

### Observability & Debugging
- Metrics:
  - Control plane: per‑tenancy WAL commit index, pending ops, apply latency distribution, member lag.
  - Read path: fan‑out width, per‑member latency, merge time, rows returned.
- Logs/Tracing:
  - Correlate by `tenant`, `op_id`, `server_id`, `epoch`, `query_kind`.
- Debug hooks:
  - Permanent `tprintln!` gates in debug builds; no runtime cost in release.

---

### Operational Runbooks
- Create and populate a tenancy:
  - `CREATE TENANCY fin_analytics WITH (namespaces=['table','time']);`
  - `ALTER TENANCY fin_analytics SET SERVERS = {'srv-a','srv-b','srv-c'};`
  - `ALTER TENANCY fin_analytics ADD OBJECT table.prices;`
  - Import existing object baseline: coordinator emits `SchemaSnapshot` op.
- Execute synchronized DDL:
  - `SYNC fin_analytics CREATE TABLE prices (...);`
- Execute cross‑tenancy reads:
  - `SYNC fin_analytics SELECT * FROM prices ORDER BY ts DESC LIMIT 100;`
  - `SYNC fin_analytics MATCH (n:User)-[r:BOUGHT]->(m:Item) RETURN n.id, m.id;`
  - `SYNC fin_analytics SHOW TABLES;`
- Inspect status:
  - `SHOW TENANCY fin_analytics;` to view members, versions, lags, last errors.

---

### Phased Delivery Plan
1) Foundations
   - Implement meta models: `TenancyMeta`, membership, scope, and `TenancySchemaLog` with watchers; epoch handling.
   - Router/member subscriptions; local `AppliedCursor` persistence; idempotent apply map.
   - CLI/SQL for `CREATE/ALTER/SHOW TENANCY`.
2) SYNC DDL
   - Parser envelope and `SET sync.tenancy`.
   - Coordinator to append ops and wait with default `AllMembers`.
   - Member `SchemaApplier` invokes existing DDL executors; structured error handling.
   - Observability for schema ops (metrics/logs/traces).
3) SYNC Reads
   - Envelope for `SELECT|MATCH|SHOW` with `sync.read_policy` and pagination budgets.
   - Tenancy scatter; per‑member execution; final merge; append `tenancy` column and aliasing.
   - ORDER BY/LIMIT k‑way merge/bounded heap; Polars 0.51+ compliant API usage.
   - Metrics for fan‑out/merge latencies; hedged reads integration.
4) Robustness
   - Apply modes `Quorum(n)` and `Timeboxed` for DDL; partial success reporting.
   - Drift detection via preconditions; repair via `SchemaSnapshot` ops.
   - Backups/PITR for tenancy metadata and WAL; recovery playbooks.
5) Advanced
   - Multi‑phase DDL (prepare/apply/finalize) and optional rollback plans.
   - Scope patterns (wildcards) with safeguards; quotas and concurrency caps per tenancy.
   - Cross‑partition 2PC alignment for future multi‑tenant write transactions.

Each phase includes documentation and integration tests; follow project guideline to prefer `cargo build/check` early and run unit tests once an entire phase is complete.

---

### High‑Level Rust APIs (aligning with existing code style)
- Meta store:
```
pub trait TenancyMetaStore {
    fn list_tenancies(&self) -> anyhow::Result<Vec<TenancyMeta>>;
    fn get_tenancy(&self, name: &str) -> anyhow::Result<TenancyMeta>;
    fn upsert_tenancy(&self, t: &TenancyMeta) -> anyhow::Result<()>;
    fn append_schema_op(&self, op: SchemaOp) -> anyhow::Result<SchemaCommit>;
    fn watch_schema_ops(&self, tenancy: &str) -> SchemaOpWatch;
}

pub struct TenancyMeta { pub name: String, pub epoch: u64, pub members: Vec<ServerMember>, pub scope: TenancyScope }
pub struct ServerMember { pub server_id: String, pub role: MemberRole, pub labels: Labels }
pub enum MemberRole { Full, ReadOnly }
pub struct TenancyScope { pub namespaces: Vec<Namespace>, pub objects: Vec<QualifiedName> }

pub struct SchemaOp { /* tenant, op_id, epoch, scope, ddl_ir, preconditions, apply_mode, retry_semantics, rollback */ }
```
- DDL coordinator:
```
pub trait TenancyCoordinator { fn submit(&self, op: SchemaOp, wait: ApplyWait) -> anyhow::Result<SubmitResult>; }
```
- Read coordinator:
```
pub enum SyncQueryKind { Select, Match, Show }
pub struct SyncQueryEnvelope { pub tenancy: String, pub kind: SyncQueryKind, pub inner: QueryPlan, pub policy: ReadPolicy, pub add_tenancy_col: bool }
pub trait TenancyQueryCoordinator { fn execute(&self, env: SyncQueryEnvelope) -> anyhow::Result<QueryResult>; }
```

Notes:
- Keep interfaces thin; factor heavy logic into submodules grouped by concern (rebalance, applier, merge, watchers).
- Prefer making existing private helpers public and reusing them rather than re‑implementing.
- For any DataFrame operations in merges or SHOW outputs, follow Polars 0.51+ practices: `Series::get(i)`; `try_extract`; `IdxSize` for sort limits; add/drop temporary columns with names like `"__mask"`.

---

### Risk & Mitigation Highlights
- Control plane split brain: use Raft/etcd transactions; no writes under minority.
- Member lag causing stale reads: default to `Timeline` but allow `Strong` policy; expose lag metrics and enable hedged reads.
- Name collisions on `tenancy` column: auto‑alias to `__tenancy` with warning metadata; allow explicit selection to override.
- Hot fan‑out: cap scatter width and implement per‑tenancy concurrency/row‑budget limits.

This combined plan delivers synchronized schema evolution and unified read results across tenancy members, building directly on the HA scale‑out foundation while keeping parsing thin, execution modular, and behavior observable and robust for enterprise deployments.