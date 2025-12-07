use anyhow::{Result, anyhow};

#[derive(Debug, Clone, Default)]
pub struct GitCommitIds {
    pub tree_id: String,
    pub commit_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct GitRefUpdate {
    pub reference: String,   // e.g., refs/heads/main
    pub new_target: String,  // commit id
}

/// Trait for FILESTORE Git operations. Concrete impls live in gitoxide/libgit2 backends.
#[allow(unused_variables)]
pub trait GitBackend: Send + Sync {
    fn ensure_repo(&self, repo_path: &str, remote: Option<&str>) -> Result<()> { Err(anyhow!("not_implemented")) }
    fn write_blob(&self, repo_path: &str, path: &str, data: &[u8]) -> Result<String> { Err(anyhow!("not_implemented")) }
    fn write_tree(&self, repo_path: &str, entries: &[(String, String)]) -> Result<String> { Err(anyhow!("not_implemented")) }
    fn write_commit(&self, repo_path: &str, message: &str, author: &str, parents: &[String], tree_id: &str) -> Result<GitCommitIds> { Err(anyhow!("not_implemented")) }
    fn update_ref(&self, repo_path: &str, update: &GitRefUpdate) -> Result<()> { Err(anyhow!("not_implemented")) }
    fn ls_remote(&self, remote: &str) -> Result<Vec<(String, String)>> { Err(anyhow!("not_implemented")) }
    fn fetch(&self, repo_path: &str, remote: &str, reference: &str) -> Result<()> { Err(anyhow!("not_implemented")) }
    fn push(&self, repo_path: &str, remote: &str, reference: &str) -> Result<()> { Err(anyhow!("not_implemented")) }
}
