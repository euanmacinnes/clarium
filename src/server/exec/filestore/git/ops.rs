//! High-level Git operations orchestrating backend selection and calls.

use anyhow::Result;

use super::backend::GitBackend;
use super::composite::CompositeGitBackend;
use super::gitoxide::GitoxideBackend;
#[cfg(feature = "libgit2-push")]
use super::libgit2::Libgit2Backend;

use crate::server::exec::filestore::config::EffectiveConfig;

/// Select an appropriate Git backend based on `EffectiveConfig.git_push_backend`.
/// Modes:
/// - "auto" (default): prefer gitoxide; on push unsupported and when feature enabled, fallback to libgit2.
/// - "gitoxide": force gitoxide only.
/// - "libgit2": force libgit2 push-only backend (requires `libgit2-push` feature; else falls back to gitoxide).
pub fn select_backend(eff: &EffectiveConfig) -> Box<dyn GitBackend> {
    let mode = eff.git_push_backend.to_ascii_lowercase();
    match mode.as_str() {
        "gitoxide" => {
            crate::tprintln!("GitBackend select=gitoxide");
            Box::new(GitoxideBackend::new())
        }
        "libgit2" => {
            #[cfg(feature = "libgit2-push")]
            {
                crate::tprintln!("GitBackend select=libgit2");
                return Box::new(Libgit2Backend::new());
            }
            #[cfg(not(feature = "libgit2-push"))]
            {
                crate::tprintln!("GitBackend select=libgit2 requested but feature disabled; falling back to gitoxide");
                return Box::new(GitoxideBackend::new());
            }
        }
        _ /* auto or unknown */ => {
            #[cfg(feature = "libgit2-push")]
            {
                crate::tprintln!("GitBackend select=auto (gitoxide with libgit2 fallback)");
                return Box::new(CompositeGitBackend::new(Box::new(GitoxideBackend::new()), Some(Box::new(Libgit2Backend::new()))));
            }
            #[cfg(not(feature = "libgit2-push"))]
            {
                crate::tprintln!("GitBackend select=auto (gitoxide only; libgit2 feature disabled)");
                return Box::new(CompositeGitBackend::new(Box::new(GitoxideBackend::new())));
            }
        }
    }
}

/// Push a reference using the selected backend.
#[allow(unused_variables)]
pub fn push_ref<B: GitBackend>(backend: &B, repo_path: &str, remote: &str, reference: &str) -> Result<()> {
    backend.push(repo_path, remote, reference)
}
