//! High-level Git operations orchestrating backend use.
//! Placeholder to keep module tree complete.

use super::backend::{GitBackend, GitRefUpdate};
use anyhow::Result;

#[allow(unused_variables)]
pub fn push_ref<B: GitBackend>(backend: &B, repo_path: &str, remote: &str, reference: &str) -> Result<()> {
    backend.push(repo_path, remote, reference)
}
