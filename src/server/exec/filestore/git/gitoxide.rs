//! Gitoxide-backed implementation of `GitBackend`.
//! Minimal plumbing now to keep builds green and provide clear Unsupported errors
//! where functionality isn't wired yet. Full implementation will follow the plan
//! in `plans/filestore_git_backend_plan.md`.

use anyhow::{anyhow, Result};

use super::backend::{GitBackend, GitCommitIds, GitRefUpdate};

#[derive(Debug, Default, Clone)]
pub struct GitoxideBackend;

impl GitoxideBackend {
    pub fn new() -> Self { Self }
}

impl GitBackend for GitoxideBackend {
    fn ensure_repo(&self, _repo_path: &str, _remote: Option<&str>) -> Result<()> {
        // TODO: integrate gix repository initialization/open when feature is enabled
        crate::tprintln!("gitoxide.ensure_repo placeholder ok");
        Ok(())
    }

    fn write_blob(&self, _repo_path: &str, _path: &str, _data: &[u8]) -> Result<String> {
        Err(anyhow!("gitoxide_write_blob_unsupported"))
    }

    fn write_tree(&self, _repo_path: &str, _entries: &[(String, String)]) -> Result<String> {
        Err(anyhow!("gitoxide_write_tree_unsupported"))
    }

    fn write_commit(&self, _repo_path: &str, _message: &str, _author: &str, _parents: &[String], _tree_id: &str) -> Result<GitCommitIds> {
        Err(anyhow!("gitoxide_write_commit_unsupported"))
    }

    fn update_ref(&self, _repo_path: &str, _update: &GitRefUpdate) -> Result<()> {
        Err(anyhow!("gitoxide_update_ref_unsupported"))
    }

    fn ls_remote(&self, _remote: &str) -> Result<Vec<(String, String)>> {
        Err(anyhow!("gitoxide_ls_remote_unsupported"))
    }

    fn fetch(&self, _repo_path: &str, _remote: &str, _reference: &str) -> Result<()> {
        Err(anyhow!("gitoxide_fetch_unsupported"))
    }

    fn push(&self, _repo_path: &str, _remote: &str, _reference: &str) -> Result<()> {
        // Explicit unsupported so `composite` can fallback to libgit2 when enabled.
        Err(anyhow!("gitoxide_push_unsupported"))
    }
}
