//! Core FILESTORE data contracts (metadata objects persisted in KV)
//! Keep this module purely about types/serde and light helpers.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRef {
    pub oid: String,
    pub off: u64,
    pub len: u32,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Chunking {
    pub chunk_size: u32,
    pub chunks: Vec<ChunkRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileMeta {
    pub id: String,
    pub logical_path: String,
    pub size: u64,
    pub etag: String,
    pub version: u64,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub description_html: Option<String>,
    #[serde(default)]
    pub custom: Option<serde_json::Value>,
    #[serde(default)]
    pub chunking: Option<Chunking>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeEntry {
    pub path: String,
    pub file_id: String,
    pub etag: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tree {
    pub id: String,
    pub created_at: i64,
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitAuthor {
    pub name: String,
    pub email: String,
    pub time_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Commit {
    pub id: String,
    pub parents: Vec<String>,
    pub tree_id: String,
    pub author: CommitAuthor,
    pub message: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub git_sha: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefInfo {
    pub branch: String,
    pub head_commit_id: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Alias {
    pub alias: String,
    pub folder_prefix: String,
    #[serde(default)]
    pub target_store: Option<String>,
    #[serde(default)]
    pub target_prefix: Option<String>,
}
