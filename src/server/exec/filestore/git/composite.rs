//! Composite backend that prefers gitoxide and can fallback to libgit2 for push.

use super::backend::{GitBackend, GitCommitIds, GitRefUpdate};
use anyhow::Result;

pub struct CompositeGitBackend {
    gitoxide: Box<dyn GitBackend>,
    #[cfg(feature = "libgit2-push")]
    libgit2: Option<Box<dyn GitBackend>>, // optional fallback used for push
}

impl CompositeGitBackend {
    #[cfg(feature = "libgit2-push")]
    pub fn new(gitoxide: Box<dyn GitBackend>, libgit2: Option<Box<dyn GitBackend>>) -> Self {
        Self { gitoxide, libgit2 }
    }

    #[cfg(not(feature = "libgit2-push"))]
    pub fn new(gitoxide: Box<dyn GitBackend>) -> Self {
        Self { gitoxide }
    }
}

impl GitBackend for CompositeGitBackend {
    fn ensure_repo(&self, repo_path: &str, remote: Option<&str>) -> Result<()> {
        self.gitoxide.ensure_repo(repo_path, remote)
    }
    fn write_blob(&self, repo_path: &str, path: &str, data: &[u8]) -> Result<String> {
        self.gitoxide.write_blob(repo_path, path, data)
    }
    fn write_tree(&self, repo_path: &str, entries: &[(String, String)]) -> Result<String> {
        self.gitoxide.write_tree(repo_path, entries)
    }
    fn write_commit(&self, repo_path: &str, message: &str, author: &str, parents: &[String], tree_id: &str) -> Result<GitCommitIds> {
        self.gitoxide.write_commit(repo_path, message, author, parents, tree_id)
    }
    fn update_ref(&self, repo_path: &str, update: &GitRefUpdate) -> Result<()> {
        self.gitoxide.update_ref(repo_path, update)
    }
    fn ls_remote(&self, remote: &str) -> Result<Vec<(String, String)>> {
        self.gitoxide.ls_remote(remote)
    }
    fn fetch(&self, repo_path: &str, remote: &str, reference: &str) -> Result<()> {
        self.gitoxide.fetch(repo_path, remote, reference)
    }
    fn push(&self, repo_path: &str, remote: &str, reference: &str) -> Result<()> {
        // For now just call gitoxide; later we can detect Unsupported and fallback.
        #[cfg(feature = "libgit2-push")]
        {
            if let Err(e) = self.gitoxide.push(repo_path, remote, reference) {
                crate::tprintln!("gitoxide push error, attempting libgit2 fallback: {}", e);
                if let Some(ref l2) = self.libgit2 { return l2.push(repo_path, remote, reference); }
                return Err(e);
            }
            return Ok(());
        }
        #[cfg(not(feature = "libgit2-push"))]
        {
            self.gitoxide.push(repo_path, remote, reference)
        }
    }
}
