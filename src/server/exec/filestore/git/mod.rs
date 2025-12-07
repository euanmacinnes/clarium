//! Git backends for FILESTORE: gitoxide primary, optional libgit2 fallback for push.
//! This is a scaffold to keep the crate buildable while we implement details.

pub mod backend;
pub mod gitoxide;
#[cfg(feature = "libgit2-push")]
pub mod libgit2;
pub mod composite;
pub mod ops;

pub use backend::{GitBackend, GitCommitIds, GitRefUpdate};