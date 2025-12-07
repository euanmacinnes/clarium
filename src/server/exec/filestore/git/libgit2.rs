//! libgit2-backed implementation of `GitBackend` used only for push fallback.
//! Compiled when feature `libgit2-push` is enabled.

use anyhow::{anyhow, Result};

use super::backend::{GitBackend, GitCommitIds, GitRefUpdate};

#[derive(Debug, Default, Clone)]
pub struct Libgit2Backend;

impl Libgit2Backend { pub fn new() -> Self { Self } }

impl GitBackend for Libgit2Backend {
    fn ensure_repo(&self, _repo_path: &str, _remote: Option<&str>) -> Result<()> { Ok(()) }
    fn write_blob(&self, _repo_path: &str, _path: &str, _data: &[u8]) -> Result<String> { Err(anyhow!("libgit2_write_blob_unsupported")) }
    fn write_tree(&self, _repo_path: &str, _entries: &[(String, String)]) -> Result<String> { Err(anyhow!("libgit2_write_tree_unsupported")) }
    fn write_commit(&self, _repo_path: &str, _message: &str, _author: &str, _parents: &[String], _tree_id: &str) -> Result<GitCommitIds> { Err(anyhow!("libgit2_write_commit_unsupported")) }
    fn update_ref(&self, _repo_path: &str, _update: &GitRefUpdate) -> Result<()> { Err(anyhow!("libgit2_update_ref_unsupported")) }
    fn ls_remote(&self, _remote: &str) -> Result<Vec<(String, String)>> { Err(anyhow!("libgit2_ls_remote_unsupported")) }
    fn fetch(&self, _repo_path: &str, _remote: &str, _reference: &str) -> Result<()> { Err(anyhow!("libgit2_fetch_unsupported")) }
    fn push(&self, _repo_path: &str, _remote: &str, _reference: &str) -> Result<()> {
        // Minimal placeholder; real push implementation can be added later.
        Err(anyhow!("libgit2_push_unsupported"))
    }
}
