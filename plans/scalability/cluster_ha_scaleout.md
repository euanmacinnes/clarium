# Clarium Cluster Architecture: High Availability and Scale-Out (Design Doc)

Status: Draft for review

Owners: Core Engine Team

Last updated: 2025-12-11

---

## 1. Goals and Non-Goals

### Goals
- Horizontal scale-out across many nodes using partitioned graphs.
- High availability via replication and automatic failover.
- Strong durability guarantees for writes; predictable read latency classes.
- Stateless PGWire/SQL frontends with thin routing.
- Online elasticity: add/remove nodes, rebalance, leadership transfer.
- Operable: clear observability, backups, and runbooks.

### Non‑Goals (initial phases)
- Global serializable transactions across all partitions by default (provided later via 2PC when required).
- Fully synchronous multi‑region strong writes (later phase; expect higher latencies).

---

## 2. Architecture Overview

The cluster is split into a control plane (membership, leases, placement) and a data plane (partitioned graph shards replicated via Raft). PGWire servers remain stateless and route requests to shard leaders.

Key components:
- Control Plane: metadata store (embedded `openraft` meta-cluster or etcd/Consul) holding Node Registry, Shard Map, and Leases.
- Data Plane: per-partition Raft groups (multi-raft). Leaders handle writes; followers replicate WAL and serve timeline/eventual reads.
- Router Layer: PGWire frontends + query executor that scatter/gather subplans to partitions.
- Storage: per-shard WAL, snapshots, and compacted segments (already aligned with GraphStore layout).

```
[ Clients / Drivers ]
        |
   [ PGWire Routers ]  <-- stateless; cache shard map; retries
        |
  +-----+------------------- Control Plane (Raft/etcd) ----------------+
  | Node Registry | Shard Map | Leases | Placement | Rebalancer        |
  +--------------------------------------------------------------------+
        |
    [ Data Plane ]
  +-------------+    +-------------+    +-------------+
  | Shard P000  |    | Shard P001  |    | Shard P002  |   ...
  | Raft group  |    | Raft group  |    | Raft group  |
  | L + F + F   |    | L + F + F   |    | L + F + F   |
  +-------------+    +-------------+    +-------------+
```

---

## 3. Data Model and Sharding

### Sharding Unit
- Graph partition (existing `partitions` option in GraphStore manifests).

### Key to Partition Mapping
- Default: consistent hashing with virtual nodes on a stable key (e.g., node key or a hash of (src,dst) for edges). Hash seeded by `partitioning.hash_seed`.
- Partition count: choose 128–2048 based on dataset scale. Make it configurable per graph.

### Placement
- Replication factor (RF) default 3 (1 in dev). Persisted in manifest (`cluster.replication_factor`).
- Failure domains (labels): `az`, `rack`, `host`. Spread replicas across domains.
- Placement algorithm: score-based first-fit decreasing with anti-affinity and capacity weights. Rebalance periodically.

---

## 4. Control Plane Design

Two deployment options:
1) Embedded meta-cluster (preferred default): 3 or 5 nodes running `openraft` to store cluster metadata.
2) External etcd/Consul: store leases/membership externally; persist durable configuration in GraphStore manifests.

### Responsibilities
- Node Registry: node_id, addresses, labels, capacity hints, software version.
- Shard Map: partition → replica set (node_ids), leader, epoch/term, config version.
- Leases: short TTLs for node liveness; watch streams to push updates to routers.
- Placement & Rebalance: compute assignments and orchestrate safe transitions.

### Metadata Objects
- `Cluster` (singleton): version, created_at, config.
- `Node` (many): id, heartbeat_ts, labels, capacity.
- `Partition` (per-graph): id, replica_set, leader, term, epoch, policy.

### Consistency
- Strong consistency for Shard Map updates via Raft writes (meta-cluster) or etcd transactions.
- Epoch increments on every leader change; clients use epochs to detect and retry.

---

## 5. Data Plane (Multi-Raft) Design

Each partition forms a Raft group. We integrate `openraft` for consensus and log replication.

### Write Path
1. Router routes mutation to partition leader.
2. Leader appends log entry; quorum replicate; fsync policy: group commit on leader, durable at followers before commit index advances.
3. Apply to state machine: append to WAL (GraphStore), update delta logs.
4. Acknowledge to client after commit index reached (configurable for latency vs. durability trade-offs).

### Read Path
- Consistency levels:
  - Strong: route to leader.
  - Timeline: follower with `read_index` (linearizable within leader lease).
  - Eventual: follower without `read_index` (lowest latency, may be stale).

### Snapshots & Compaction
- Periodic snapshots per partition (size or time based). Store manifest + checksum.
- Compact Raft logs after stable snapshot.
- GraphStore compaction merges delta logs into segment files; preserve reverse adjacency if configured.

### Replica Changes
- Joint consensus reconfiguration: add non-voter, catch up, promote; demote and remove old replica. Orchestrated by control plane.

---

## 6. Routing Layer (PGWire) and Query Execution

Routers are stateless. They cache the Shard Map with TTL and subscribe to updates.

Responsibilities:
- Session management and authN/authZ token verification.
- Query planning boundary: split logical plan into per-partition subplans; keep router thin and defer heavy operators to executors.
- Scatter/Gather:
  - Build per-partition requests with consistency level.
  - Concurrently dispatch to leaders or followers per policy.
  - Merge partial results honoring order/limit semantics; use bounded heaps for top-k (see Polars guidelines for stable APIs).
- Retries & Hedged Reads:
  - Idempotency tokens for write retries.
  - Hedged reads for tail latency (send a backup after p95 delay).

---

## 7. Transactions

### Intra-Partition
- Single-partition transactions: provided by Raft linearizability; ensure WAL atomicity.

### Cross-Partition (Phase 3)
- Two-Phase Commit (2PC):
  - Coordinator: router-side transaction manager assigns a global txn_id.
  - Prepare: send prepare markers to involved leaders; on success, commit markers; on failure, abort.
  - Timeouts and recovery: coordinator writes transaction state to a small durable store (control plane or a dedicated `txn` meta-raft group).
- Sagas: for long-running tasks with compensations.

---

## 8. Configuration and Manifests

Extend the GraphStore named config (see `docs/graph-catalog.md`) with cluster keys:

```json
{
  "partitions": 256,
  "cluster.replication_factor": 3,
  "cluster.failure_domains": ["az"],
  "placement": { "strategy": "vnodes", "vnodes_per_shard": 64 },
  "leases": { "backend": "embedded_raft", "election_ms": 1200, "ttl_ms": 4000 },
  "rebalance": { "enable": true, "max_moves_per_min": 8 },
  "transport": { "protocol": "quic", "mtls": true },
  "consistency": { "default": "timeline", "strict_reads": ["system"], "allow_eventual": true }
}
```

---

## 9. Node Lifecycle and Rebalancing

### Add Node
1) Node registers to control plane with labels/capacity.
2) Planner assigns new replicas for under-replicated or hot partitions.
3) Add replicas as non-voters; catch up; promote; optionally transfer leaders.

### Remove/Drain Node
1) Mark unschedulable; transfer leaders away.
2) Add replacement replicas elsewhere; remove from quorums post-catch-up.

### Rebalance Triggers
- Capacity skew, hotspot metrics (QPS, CPU, IO), node addition/removal. Respect rate limits.

---

## 10. Failure Handling and HA

- Fast detection via leases (1–2s with jitter). Avoid thrash with backoff.
- Per-partition leader election handled by Raft. Routers validate leader epoch and retry on `NotLeader` errors.
- Write de-duplication via client-supplied idempotency keys and coordinator txn_ids.

---

## 11. Security and Multi-Tenancy

- mTLS between nodes; SPIFFE/SPIRE optional in larger deploys.
- RBAC integration (see `src/server/exec/filestore/sec/*`): control who can create graphs, modify placement, or administer cluster.
- Per-tenant quotas and placement constraints; optional data-at-rest encryption with per-shard keys.

---

## 12. Observability

### Metrics (per shard)
- Raft: term, leader, commit index, lag, append/commit latencies.
- Storage: WAL fsync latency, snapshot age/size, compaction durations.
- Query: per-operator timings, scatter width, tail latencies.

### Logs & Tracing
- Structured logs with correlation ids (session_id, txn_id, shard, epoch).
- Distributed tracing from router → shard; sampling controls.

### Debugging
- Permanent lightweight `tprintln!`-style debug hooks in non-release builds; no cost in release.

---

## 13. Backup and Restore

- Backups: stream snapshot plus incremental WAL segments to object storage.
- PITR: restore snapshot then apply WAL to target timestamp.
- Scope: per-graph, per-partition, or entire cluster.

---

## 14. API Surfaces (Rust)

Below are high-level interfaces. Keep primary interfaces thin; move heavy logic to dedicated modules. Prefer making existing private helpers public and relocating rather than re-implementing.

```rust
// Control plane (embedded)
pub trait ClusterMetaStore {
    fn get_nodes(&self) -> anyhow::Result<Vec<NodeInfo>>;
    fn get_shard_map(&self, graph: &str) -> anyhow::Result<ShardMap>;
    fn watch_shard_map(&self, graph: &str) -> ShardMapWatch;
    fn txn_update<F: FnOnce(&mut MetaTxn) -> anyhow::Result<()>>(&self, f: F) -> anyhow::Result<()>;
}

pub struct NodeInfo { pub id: String, pub addrs: NodeAddrs, pub labels: Labels, pub capacity: Capacity, pub version: String }
pub struct ShardMap { pub graph: String, pub parts: Vec<ShardEntry>, pub version: u64 }
pub struct ShardEntry { pub part: u32, pub replicas: Vec<String>, pub leader: Option<String>, pub epoch: u64 }

// Data plane client (router side)
pub trait ShardClient {
    fn write(&self, part: u32, req: WriteReq, epoch: u64, idempo: Option<String>) -> anyhow::Result<WriteResp>;
    fn read(&self, part: u32, req: ReadReq, policy: ReadPolicy, epoch_hint: Option<u64>) -> anyhow::Result<ReadResp>;
}

pub enum ReadPolicy { Strong, Timeline, Eventual }

// Placement engine
pub trait PlacementPlanner {
    fn plan_initial(&self, nodes: &[NodeInfo], parts: u32, rf: u32, domains: &[&str]) -> anyhow::Result<ShardMap>;
    fn plan_rebalance(&self, cur: &ShardMap, nodes: &[NodeInfo], policy: &RebalancePolicy) -> anyhow::Result<Vec<PlanOp>>;
}

pub enum PlanOp { AddReplica { part: u32, node: String }, Promote { part: u32, node: String }, TransferLeader { part: u32, from: String, to: String }, RemoveReplica { part: u32, node: String } }
```

Keep match statements thin: each `PlanOp` handled by a dedicated function grouped under a `rebalance` module.

---

## 15. Execution Flow Details

### Router Write (single partition)
1) Resolve partition → leader + epoch from Shard Map cache.
2) Send write with idempotency key and epoch.
3) On `NotLeader` or epoch mismatch, refresh map and retry once (exponential backoff thereafter).

### Router Read (scatter)
1) Build subplans per partition.
2) For each subplan, choose read policy (Strong/Timeline/Eventual).
3) Dispatch concurrently; merge results honoring sort/limit using bounded heaps or k-way merges.

---

## 16. Phased Delivery Plan

- Phase 0: Partitioning only; single node, RF=1; stabilize GraphStore manifest and APIs. (Baseline)
- Phase 1: RF=3 multi-raft; leader-only writes; static placement (manual config). Health checks and basic failover.
- Phase 2: Control plane with leases, dynamic placement, rebalancer; online leadership transfer. Router retries/hedging.
- Phase 3: Cross-shard transactions (2PC) and full scatter/gather; configurable consistency levels per query/session.
- Phase 4: Multi-AZ and optional multi-region; backups/PITR; hot shard auto-splitting.

Each phase ships with: docs, chaos tests, soak tests, and operational playbooks.

---

## 17. Testing Strategy

Keep tests in dedicated files (no inline tests). Use `cargo build`/`cargo check` to validate compiles early; only run unit tests once an entire phase is feature-complete.

### Unit Tests
- Placement planner: constraint satisfaction, anti-affinity, capacity weighting.
- Shard map updates: epoch increments, watchers, cache invalidation.
- Router scatter/gather: correct merges under ORDER BY/LIMIT.

### Integration Tests
- Embedded 3-node cluster with RF=3: leader elections, write/read consistency classes.
- Failure drills: kill leaders, network partitions, clock skews.
- Rebalance flows: add/remove nodes, leader transfers.

### Benchmarks
- Read tail latency with hedged reads.
- Write throughput under group commit settings.

---

## 18. Operational Runbooks

### Add Node
1) Start process with node labels/capacity.
2) Verify registration in Node Registry.
3) Observe planned replica adds; wait for catch-up and promotions.

### Drain Node
1) Mark unschedulable; transfer leaders away.
2) Watch replica removals; confirm zero shards assigned.

### Rolling Upgrade
1) For each node: transfer leaders; restart; verify healthy; proceed.

---

## 19. Risks and Mitigations

- Split brain in control plane: use Raft/etcd with strict quorum; no writes under minority.
- Hot partitions: support leadership transfer, rebalancing, and eventual auto-split.
- Long GC pauses/compaction: throttle compaction; snapshot scheduling; backpressure.
- Query fan-out storms: cap scatter width; push down filters; consider bloom/indices per partition.

---

## 20. Mapping to Current Codebase

- `docs/graph-catalog.md`: already includes `USING GRAPHSTORE` and mentions `cluster.replication_factor`. Extend JSON config schema as above.
- `src/server/graphstore.rs`: contains `ClusterGroup`, `ClusterMeta`, and validation. Evolve into runtime shard map and placement/controller modules. Maintain thin primary interfaces.
- `src/pgwire_server*.rs`: host stateless router logic; delegate heavy operations to `exec` submodules that implement scatter/gather.
- Security: integrate with `src/server/exec/filestore/sec/*` for RBAC.

---

## 21. Open Questions

- Embedded vs. external control plane default? Proposal: embedded for simplicity; support etcd via feature flag.
- QUIC vs. gRPC over TCP? Proposal: QUIC default for multiplexing and better head-of-line behavior.
- Read-your-writes semantics across partitions in a session: session stickiness vs. transaction scope.

---

## 22. Acceptance Criteria (per phase)

- Phase 1:
  - RF=3 multi-raft; leader failover < 3s p95 under nominal conditions.
  - Strong reads return latest committed data; timeline reads pass Jepsen-style read-index checks.
  - Basic metrics exposed for Raft and storage.
- Phase 2:
  - Dynamic rebalancing with rate limits; leadership transfer APIs.
  - Router retries and hedged reads reduce p99 tail by ≥30% in soak.
- Phase 3:
  - 2PC for multi-partition writes with recovery; no lost updates under failures.

---

## 23. Appendix: Example Config (default_cluster.json)

```json
{
  "partitions": 256,
  "gc_window": "20m",
  "cluster.replication_factor": 3,
  "cluster.failure_domains": ["az"],
  "placement": { "strategy": "vnodes", "vnodes_per_shard": 64 },
  "leases": { "backend": "embedded_raft", "election_ms": 1200, "ttl_ms": 4000 },
  "rebalance": { "enable": true, "max_moves_per_min": 8 },
  "transport": { "protocol": "quic", "mtls": true },
  "consistency": { "default": "timeline", "strict_reads": ["system"], "allow_eventual": true }
}
```
