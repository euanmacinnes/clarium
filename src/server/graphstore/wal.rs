//! GraphStore WAL (write-ahead log) — ACID skeleton
//! -------------------------------------------------
//! Minimal paged WAL with per-record CRC and fsync on commit. This is an
//! implementation skeleton to enable end-to-end tests and recovery logic in
//! subsequent iterations. It focuses on correctness and durability; further
//! optimizations (page CRCs, rolling indices, group commit tuning) can be
//! layered on top without changing the format exposed here.

use anyhow::{anyhow, Context, Result};
use crc32fast::Hasher as Crc32;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC_WAL: u32 = 0x47574C31; // 'GWL1'

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecKind { Begin = 1, Data = 2, Commit = 3, Abort = 4 }

#[derive(Debug, Clone, Copy)]
struct RecHeader { magic: u32, kind: u8, version: u8, _pad: u16, len: u32 }

impl RecHeader {
    fn write_to(&self, w: &mut File) -> Result<()> {
        let mut buf = [0u8; 12];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4] = self.kind; buf[5] = self.version; buf[6..8].copy_from_slice(&self._pad.to_le_bytes());
        buf[8..12].copy_from_slice(&self.len.to_le_bytes());
        w.write_all(&buf)?; Ok(())
    }
    fn read_from(r: &mut File) -> Result<Self> {
        let mut buf = [0u8; 12];
        r.read_exact(&mut buf)?;
        Ok(Self{
            magic: u32::from_le_bytes([buf[0],buf[1],buf[2],buf[3]]),
            kind: buf[4],
            version: buf[5],
            _pad: u16::from_le_bytes([buf[6],buf[7]]),
            len: u32::from_le_bytes([buf[8],buf[9],buf[10],buf[11]]),
        })
    }
}

fn write_u64(buf: &mut Vec<u8>, v: u64) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_u16(buf: &mut Vec<u8>, v: u16) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_u8(buf: &mut Vec<u8>, v: u8) { buf.push(v); }
fn write_bytes(buf: &mut Vec<u8>, b: &[u8]) { buf.extend_from_slice(b); }

#[derive(Debug, Clone)]
pub struct TxnBegin { pub txn_id: u64, pub snapshot_epoch: u64 }

#[derive(Debug, Clone)]
pub struct NodeOp { pub op: u8, pub label: String, pub key: String, pub node_id: Option<u64> }

#[derive(Debug, Clone)]
pub struct EdgeOp { pub op: u8, pub part: u32, pub src: u64, pub dst: u64, pub etype_id: u16 }

#[derive(Debug, Clone)]
pub struct TxnData { pub txn_id: u64, pub nodes: Vec<NodeOp>, pub edges: Vec<EdgeOp> }

#[derive(Debug, Clone)]
pub struct TxnCommit { pub txn_id: u64, pub commit_epoch: u64 }

#[derive(Debug, Clone)]
pub struct TxnAbort { pub txn_id: u64 }

#[derive(Debug, Clone)]
pub enum WalRecord { Begin(TxnBegin), Data(TxnData), Commit(TxnCommit), Abort(TxnAbort) }

/// WAL writer appends records and fsyncs on commit.
pub struct WalWriter { file: File, path: PathBuf, max_size_bytes: u64 }

impl WalWriter {
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
        let file = OpenOptions::new().create(true).append(true).read(true).open(path)
            .with_context(|| format!("open WAL for append: {}", path.display()))?;
        Ok(Self { file, path: path.to_path_buf(), max_size_bytes: 64 * 1024 * 1024 })
    }

    pub fn append_begin(&mut self, b: &TxnBegin) -> Result<()> { self.append(&WalRecord::Begin(b.clone()), false) }
    pub fn append_data(&mut self, d: &TxnData) -> Result<()> { self.append(&WalRecord::Data(d.clone()), false) }
    pub fn append_abort(&mut self, a: &TxnAbort) -> Result<()> { self.append(&WalRecord::Abort(a.clone()), false) }
    pub fn append_commit(&mut self, c: &TxnCommit) -> Result<()> { self.append(&WalRecord::Commit(c.clone()), true) }

    /// Append a commit record without fsync; used by group-commit batching where
    /// only the final commit in the batch should fsync.
    pub fn append_commit_nosync(&mut self, c: &TxnCommit) -> Result<()> { self.append(&WalRecord::Commit(c.clone()), false) }

    fn append(&mut self, rec: &WalRecord, sync: bool) -> Result<()> {
        let mut payload: Vec<u8> = Vec::new();
        match rec {
            WalRecord::Begin(b) => {
                write_u64(&mut payload, b.txn_id);
                write_u64(&mut payload, b.snapshot_epoch);
                self.write_record(RecKind::Begin, &payload, sync)
            }
            WalRecord::Data(d) => {
                write_u64(&mut payload, d.txn_id);
                write_u32(&mut payload, d.nodes.len() as u32);
                write_u32(&mut payload, d.edges.len() as u32);
                for n in &d.nodes {
                    write_u8(&mut payload, n.op);
                    write_u16(&mut payload, n.label.len() as u16); write_bytes(&mut payload, n.label.as_bytes());
                    write_u16(&mut payload, n.key.len() as u16); write_bytes(&mut payload, n.key.as_bytes());
                    write_u8(&mut payload, n.node_id.is_some() as u8);
                    if let Some(id) = n.node_id { write_u64(&mut payload, id); }
                }
                for e in &d.edges {
                    write_u8(&mut payload, e.op);
                    write_u32(&mut payload, e.part);
                    write_u64(&mut payload, e.src);
                    write_u64(&mut payload, e.dst);
                    write_u16(&mut payload, e.etype_id);
                }
                self.write_record(RecKind::Data, &payload, sync)
            }
            WalRecord::Commit(c) => {
                write_u64(&mut payload, c.txn_id);
                write_u64(&mut payload, c.commit_epoch);
                self.write_record(RecKind::Commit, &payload, sync)
            }
            WalRecord::Abort(a) => {
                write_u64(&mut payload, a.txn_id);
                self.write_record(RecKind::Abort, &payload, sync)
            }
        }
    }

    fn write_record(&mut self, kind: RecKind, payload: &[u8], sync: bool) -> Result<()> {
        // Simple rolling policy: if file exceeds threshold, roll to timestamped file and reopen current
        self.maybe_roll()?;
        let mut hasher = Crc32::new(); hasher.update(payload); let crc = hasher.finalize();
        let header = RecHeader { magic: MAGIC_WAL, kind: kind as u8, version: 1, _pad: 0, len: (payload.len() as u32) };
        header.write_to(&mut self.file)?;
        self.file.write_all(payload)?;
        self.file.write_all(&crc.to_le_bytes())?;
        if sync {
            self.file.flush()?;
            // On Windows this maps to FlushFileBuffers via std
            self.file.sync_all()?;
        }
        Ok(())
    }

    fn maybe_roll(&mut self) -> Result<()> {
        // Best effort: if current file larger than max_size_bytes, rename to wal.<epoch_millis>.lg and reopen
        let len = self.file.metadata().map(|m| m.len()).unwrap_or(0);
        if len < self.max_size_bytes { return Ok(()); }
        // Build new name
        let parent = self.path.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."));
        let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
        let rolled = parent.join(format!("wal.{}.lg", now_ms));
        // Close current by dropping File, then rename and reopen
        drop(&self.file);
        // If the current file may be named "current.lg", rename it
        let _ = std::fs::rename(&self.path, &rolled);
        // Reopen fresh current
        let newf = OpenOptions::new().create(true).append(true).read(true).open(&self.path)
            .with_context(|| format!("reopen WAL after roll: {}", self.path.display()))?;
        self.file = newf;
        Ok(())
    }
}

/// WAL reader that validates magic and per-record CRC and yields records.
pub struct WalReader { file: File, end: u64 }

impl WalReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut f = OpenOptions::new().read(true).open(path)
            .with_context(|| format!("open WAL for read: {}", path.display()))?;
        let end = f.seek(SeekFrom::End(0))?; f.seek(SeekFrom::Start(0))?;
        Ok(Self { file: f, end })
    }

    pub fn read_all(&mut self) -> Result<Vec<WalRecord>> {
        let mut out = Vec::new();
        loop {
            let pos = self.file.seek(SeekFrom::Current(0))?;
            if pos >= self.end { break; }
            // Attempt to read header; break on EOF
            let header = match RecHeader::read_from(&mut self.file) {
                Ok(h) => h,
                Err(_) => break,
            };
            if header.magic != MAGIC_WAL { return Err(anyhow!("bad WAL magic")); }
            let mut payload = vec![0u8; header.len as usize];
            self.file.read_exact(&mut payload)?;
            let mut crc_bytes = [0u8;4]; self.file.read_exact(&mut crc_bytes)?;
            let expected = u32::from_le_bytes(crc_bytes);
            let mut hasher = Crc32::new(); hasher.update(&payload); let got = hasher.finalize();
            if expected != got { return Err(anyhow!("WAL record CRC mismatch")); }
            // Decode record minimally
            let rec = match header.kind {
                x if x == RecKind::Begin as u8 => {
                    let txn = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                    let snap = u64::from_le_bytes(payload[8..16].try_into().unwrap());
                    WalRecord::Begin(TxnBegin{ txn_id: txn, snapshot_epoch: snap })
                }
                x if x == RecKind::Data as u8 => {
                    let mut o = 0usize;
                    let txn = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8;
                    let n_nodes = u32::from_le_bytes(payload[o..o+4].try_into().unwrap()) as usize; o+=4;
                    let n_edges = u32::from_le_bytes(payload[o..o+4].try_into().unwrap()) as usize; o+=4;
                    let mut nodes = Vec::with_capacity(n_nodes);
                    for _ in 0..n_nodes {
                        let op = payload[o]; o+=1;
                        let l_len = u16::from_le_bytes(payload[o..o+2].try_into().unwrap()) as usize; o+=2;
                        let label = String::from_utf8(payload[o..o+l_len].to_vec()).unwrap(); o+=l_len;
                        let k_len = u16::from_le_bytes(payload[o..o+2].try_into().unwrap()) as usize; o+=2;
                        let key = String::from_utf8(payload[o..o+k_len].to_vec()).unwrap(); o+=k_len;
                        let has_id = payload[o]; o+=1;
                        let node_id = if has_id!=0 { let id = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8; Some(id) } else { None };
                        nodes.push(NodeOp{ op, label, key, node_id });
                    }
                    let mut edges = Vec::with_capacity(n_edges);
                    for _ in 0..n_edges {
                        let op = payload[o]; o+=1;
                        let part = u32::from_le_bytes(payload[o..o+4].try_into().unwrap()); o+=4;
                        let src = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8;
                        let dst = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8;
                        let etype_id = u16::from_le_bytes(payload[o..o+2].try_into().unwrap()); o+=2;
                        edges.push(EdgeOp{ op, part, src, dst, etype_id });
                    }
                    WalRecord::Data(TxnData{ txn_id: txn, nodes, edges })
                }
                x if x == RecKind::Commit as u8 => {
                    let txn = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                    let epoch = u64::from_le_bytes(payload[8..16].try_into().unwrap());
                    WalRecord::Commit(TxnCommit{ txn_id: txn, commit_epoch: epoch })
                }
                x if x == RecKind::Abort as u8 => {
                    let txn = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                    WalRecord::Abort(TxnAbort{ txn_id: txn })
                }
                _ => return Err(anyhow!("unknown WAL kind")),
            };
            out.push(rec);
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "wal_tests.rs"]
mod wal_tests;
