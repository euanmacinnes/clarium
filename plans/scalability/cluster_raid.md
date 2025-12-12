### Short answer
- In a replicated, multi‑raft architecture with RF=3, prefer JBOD (no parity RAID) on each node with high‑quality NVMe SSDs. Let replication provide availability and durability. This is usually more cost‑effective than large RAID arrays.
- Add a small mirror (RAID1) only for the OS + WAL device if you want to reduce failovers due to a single‑device loss and to keep fsync/latency stable during a disk failure.
- Avoid RAID5/6 on large SSDs/HDDs (rebuild risk and write penalty). Consider RAID10 only for specific cases where you must keep a node up across a single‑device failure and you have high, sustained per‑node read concurrency.

### Why JBOD + replication usually wins
- System availability comes from cross‑node replication, not from intra‑node RAID. With RF=3 and healthy placement across failure domains, one node loss doesn’t affect write availability and barely affects read capacity.
- Cost efficiency: adding more smaller nodes with JBOD increases aggregate CPU/IO, reduces blast radius, and often lowers $/IOPS and $/TB compared to fewer large RAID boxes.
- Performance: modern NVMe SSDs deliver excellent random IO without needing RAID striping. Replication writes are dominated by network + WAL fsync; RAID5/6 adds extra write amplification.
- Operational simplicity: JBOD avoids long, risky RAID5/6 rebuilds (which can jeopardize data during UREs and throttle IO). Replication handles disk/node loss with predictable MTTR.

### When to add local RAID anyway
- OS + WAL reliability: mirroring a small boot+WAL device (RAID1) can prevent avoidable failovers and keep fsync latency stable if a single device dies.
- Compliance or maintenance constraints: if a node must remain up through single‑device failures (limited change windows), RAID10 on the data set can be justified.
- Very high per‑node concurrency for reads (filestore hot objects): RAID10 can raise per‑node read IOPS, but the same budget spent on more nodes often delivers better tail latencies and capacity.

### What to avoid
- RAID5/6 on large volumes: long rebuilds, high write penalty, elevated risk during rebuild. This is at odds with write‑heavy workloads (WAL + compaction) and with our goal to fail fast and recover via replication.

### Recommended baseline bill of materials
- Disks: 2× small SSDs (or partitions) for OS + WAL (RAID1 optional), plus N× NVMe SSDs as JBOD for data.
- Filesystems: XFS or ext4 on JBOD. If you need checksumming/snapshots on a node, ZFS is fine with mirrors (avoid wide RAIDZ for write‑heavy shards).
- NICs: at least 25G for balanced replication + client traffic when using NVMe; QoS for Raft replication and WAL shipping.
- Memory/ECC: ECC RAM strongly recommended; enough page cache to smooth compaction/reads.

### Namespace nuances (graphs, tables, time, filestore)
- Tables/time tables: mostly sequential/appends with background compaction. JBOD + NVMe is ideal; WAL on a mirrored device if you want extra safety.
- Filestore: chunk/packfile IO benefits from NVMe throughput. JBOD is fine; consider RAID10 only if per‑node hot read fan‑in is extreme and scaling out is impractical.

### Capacity and efficiency math (rule of thumb)
- Replication RF=3 → effective capacity ≈ 1/3. Don’t also pay a 2× RAID10 overhead unless you truly need intra‑node continuity.
- For the same budget, 6 JBOD nodes with RF=3 typically beat 3 RAID10 nodes on p95/p99 tail latency and rebuild risk.

### Ops guidance
- Monitor SMART and NVMe health; enable weekly scrubs if using ZFS. Keep a few cold spares for quick swaps; rely on the cluster to reshuffle replicas while a node is replaced.
- Isolate WAL from data when possible (dedicated device or partition) to protect fsync latency.
- Use placement rules to spread replicas across failure domains (az/rack/host) so node/disk failures don’t cluster.

### Bottom line
- Default: JBOD on each node with good NVMe, RF=3 replication across nodes. Optional RAID1 for OS+WAL. Avoid parity RAID. Scale out with more, simpler servers for the best cost/perf/availability balance.