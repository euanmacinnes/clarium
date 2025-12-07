//! GraphStore persisted delta logs (append-only, per-partition and nodes)
//! ---------------------------------------------------------------------
//! Simple append-only logs for recent edge/node mutations to bound WAL
//! replay time. Each record carries a `(txn_id, op_index)` pair for
//! idempotent application. We implement edge logs (per partition) and
//! node logs (single file under `nodes/delta.log`).

use anyhow::{anyhow, Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::delta::PartitionDeltaIndex;

const MAGIC_DLOG: u32 = 0x444C4F47; // 'DLOG'

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DKind { Edge = 1, Node = 2 }

#[derive(Debug, Clone, Copy)]
struct DHeader { magic: u32, kind: u8, version: u8, _pad: u16, len: u32 }

impl DHeader {
    fn write_to(&self, w: &mut File) -> Result<()> {
        let mut buf = [0u8; 12];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4] = self.kind; buf[5] = self.version; buf[6..8].copy_from_slice(&self._pad.to_le_bytes());
        buf[8..12].copy_from_slice(&self.len.to_le_bytes());
        w.write_all(&buf)?; Ok(())
    }
    fn read_from(r: &mut File) -> Result<Self> {
        let mut buf = [0u8; 12]; r.read_exact(&mut buf)?;
        Ok(Self{
            magic: u32::from_le_bytes([buf[0],buf[1],buf[2],buf[3]]),
            kind: buf[4], version: buf[5], _pad: u16::from_le_bytes([buf[6],buf[7]]),
            len: u32::from_le_bytes([buf[8],buf[9],buf[10],buf[11]]),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EdgeDeltaRec { pub txn_id: u64, pub op_index: u32, pub op: u8, pub src: u64, pub dst: u64 }

#[derive(Debug, Clone)]
pub struct NodeDeltaRec { pub txn_id: u64, pub op_index: u32, pub op: u8, pub label: String, pub key: String, pub node_id: Option<u64> }

/// Delta log writer (per-partition)
pub struct DeltaLogWriter { file: File }

impl DeltaLogWriter {
    pub fn open_append(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
        let file = OpenOptions::new().create(true).append(true).read(true).open(path)
            .with_context(|| format!("open delta log for append: {}", path.display()))?;
        Ok(Self { file })
    }

    pub fn append_edge(&mut self, r: &EdgeDeltaRec) -> Result<()> {
        let mut payload = Vec::<u8>::with_capacity(1+8+4+1+8+8);
        payload.extend_from_slice(&r.txn_id.to_le_bytes());
        payload.extend_from_slice(&r.op_index.to_le_bytes());
        payload.push(r.op);
        payload.extend_from_slice(&r.src.to_le_bytes());
        payload.extend_from_slice(&r.dst.to_le_bytes());
        let header = DHeader{ magic: MAGIC_DLOG, kind: DKind::Edge as u8, version: 1, _pad: 0, len: payload.len() as u32 };
        header.write_to(&mut self.file)?;
        self.file.write_all(&payload)?;
        self.file.flush()?;
        Ok(())
    }
}

/// Delta log reader (per-partition)
pub struct DeltaLogReader { file: File, end: u64 }

impl DeltaLogReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut f = OpenOptions::new().read(true).open(path)
            .with_context(|| format!("open delta log for read: {}", path.display()))?;
        let end = f.seek(SeekFrom::End(0))?; f.seek(SeekFrom::Start(0))?;
        Ok(Self { file: f, end })
    }

    pub fn read_all_edges(&mut self) -> Result<Vec<EdgeDeltaRec>> {
        let mut out = Vec::new();
        loop {
            let pos = self.file.seek(SeekFrom::Current(0))?;
            if pos >= self.end { break; }
            let header = match DHeader::read_from(&mut self.file) { Ok(h) => h, Err(_) => break };
            if header.magic != MAGIC_DLOG || header.kind != DKind::Edge as u8 { return Err(anyhow!("bad delta log record")); }
            let mut payload = vec![0u8; header.len as usize];
            self.file.read_exact(&mut payload)?;
            let mut o = 0usize;
            let txn_id = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8;
            let op_index = u32::from_le_bytes(payload[o..o+4].try_into().unwrap()); o+=4;
            let op = payload[o]; o+=1;
            let src = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8;
            let dst = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8;
            out.push(EdgeDeltaRec{ txn_id, op_index, op, src, dst });
        }
        Ok(out)
    }
}

/// Apply a list of delta records to a `PartitionDeltaIndex` idempotently using `(txn_id, op_index)`.
pub fn apply_edge_deltas(idx: &mut PartitionDeltaIndex, recs: &[EdgeDeltaRec], seen: &mut std::collections::HashSet<(u64,u32)>) {
    for r in recs {
        let key = (r.txn_id, r.op_index);
        if seen.contains(&key) { continue; }
        match r.op { 0 => idx.add_edge(r.src, r.dst), 1 => idx.del_edge(r.src, r.dst), _ => {} }
        seen.insert(key);
    }
}

/// Node delta log writer (single file under nodes/)
pub struct NodeDeltaLogWriter { file: File }

impl NodeDeltaLogWriter {
    pub fn open_append(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).ok(); }
        let file = OpenOptions::new().create(true).append(true).read(true).open(path)
            .with_context(|| format!("open node delta log for append: {}", path.display()))?;
        Ok(Self { file })
    }

    pub fn append_node(&mut self, r: &NodeDeltaRec) -> Result<()> {
        // payload: txn_id:u64, op_index:u32, op:u8, label_len:u16, label:bytes, key_len:u16, key:bytes, has_id:u8, [node_id:u64]
        let mut payload = Vec::<u8>::new();
        payload.extend_from_slice(&r.txn_id.to_le_bytes());
        payload.extend_from_slice(&r.op_index.to_le_bytes());
        payload.push(r.op);
        let lbytes = r.label.as_bytes();
        let kbytes = r.key.as_bytes();
        payload.extend_from_slice(&(lbytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(lbytes);
        payload.extend_from_slice(&(kbytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(kbytes);
        match r.node_id {
            Some(id) => { payload.push(1u8); payload.extend_from_slice(&id.to_le_bytes()); },
            None => { payload.push(0u8); }
        }
        let header = DHeader{ magic: MAGIC_DLOG, kind: DKind::Node as u8, version: 1, _pad: 0, len: payload.len() as u32 };
        header.write_to(&mut self.file)?;
        self.file.write_all(&payload)?;
        self.file.flush()?;
        Ok(())
    }
}

/// Node delta log reader
pub struct NodeDeltaLogReader { file: File, end: u64 }

impl NodeDeltaLogReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut f = OpenOptions::new().read(true).open(path)
            .with_context(|| format!("open node delta log for read: {}", path.display()))?;
        let end = f.seek(SeekFrom::End(0))?; f.seek(SeekFrom::Start(0))?;
        Ok(Self { file: f, end })
    }

    pub fn read_all_nodes(&mut self) -> Result<Vec<NodeDeltaRec>> {
        let mut out = Vec::new();
        loop {
            let pos = self.file.seek(SeekFrom::Current(0))?;
            if pos >= self.end { break; }
            let header = match DHeader::read_from(&mut self.file) { Ok(h) => h, Err(_) => break };
            if header.magic != MAGIC_DLOG || header.kind != DKind::Node as u8 { return Err(anyhow!("bad node delta log record")); }
            let mut payload = vec![0u8; header.len as usize];
            self.file.read_exact(&mut payload)?;
            let mut o = 0usize;
            let txn_id = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); o+=8;
            let op_index = u32::from_le_bytes(payload[o..o+4].try_into().unwrap()); o+=4;
            let op = payload[o]; o+=1;
            let l_len = u16::from_le_bytes(payload[o..o+2].try_into().unwrap()) as usize; o+=2;
            let label = String::from_utf8(payload[o..o+l_len].to_vec()).unwrap(); o+=l_len;
            let k_len = u16::from_le_bytes(payload[o..o+2].try_into().unwrap()) as usize; o+=2;
            let key = String::from_utf8(payload[o..o+k_len].to_vec()).unwrap(); o+=k_len;
            let has_id = payload[o]; o+=1;
            let node_id = if has_id != 0 { let id = u64::from_le_bytes(payload[o..o+8].try_into().unwrap()); Some(id) } else { None };
            out.push(NodeDeltaRec{ txn_id, op_index, op, label, key, node_id });
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "delta_log_tests.rs"]
mod delta_log_tests;
