use serde::{Deserialize, Serialize};

/// Global FILESTORE settings applied to all filestores unless overridden.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GlobalFilestoreConfig {
    pub acl_cache_ttl_allow_ms: u64,
    pub acl_cache_ttl_deny_ms: u64,

    pub git_remote: Option<String>,
    pub git_branch: Option<String>,
    /// 'plumbing_only' or 'worktree'
    pub git_mode: String,
    /// Backend selection; currently informational. Default 'gitoxide'.
    pub git_backend: String,
    /// Push backend selection: 'auto' | 'gitoxide' | 'libgit2'
    pub git_push_backend: String,
    /// Optional patterns e.g., "*.pdf;*.pptx"
    pub lfs_patterns: Option<String>,

    /// Maximum bytes for description_html stored verbatim
    pub html_description_max_bytes: usize,

    /// Grace period in seconds before GC can permanently delete tombstoned entries
    pub gc_grace_seconds: u64,
}

impl Default for GlobalFilestoreConfig {
    fn default() -> Self {
        Self {
            acl_cache_ttl_allow_ms: 60_000,
            acl_cache_ttl_deny_ms: 10_000,

            git_remote: None,
            git_branch: Some("main".to_string()),
            git_mode: "plumbing_only".to_string(),
            git_backend: "gitoxide".to_string(),
            git_push_backend: "auto".to_string(),
            lfs_patterns: None,

            html_description_max_bytes: 32 * 1024,
            gc_grace_seconds: 86_400, // 1 day by default
        }
    }
}

/// Per-filestore configuration. Unspecified values inherit from Global.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FilestoreConfig {
    /// Enable/disable calling external ACL service entirely (dev bypass)
    pub security_check_enabled: bool,
    // ACL cache ttl overrides (local evaluator cache)
    pub acl_cache_ttl_allow_ms: Option<u64>,
    pub acl_cache_ttl_deny_ms: Option<u64>,

    // Git defaults at filestore root
    pub git_remote: Option<String>,
    pub git_branch: Option<String>,
    pub git_mode: Option<String>,
    pub git_backend: Option<String>,
    pub git_push_backend: Option<String>,
    pub lfs_patterns: Option<String>,

    // Metadata limits
    pub html_description_max_bytes: Option<usize>,
}

impl Default for FilestoreConfig {
    fn default() -> Self {
        Self {
            security_check_enabled: true,
            acl_cache_ttl_allow_ms: None,
            acl_cache_ttl_deny_ms: None,
            git_remote: None,
            git_branch: None,
            git_mode: None,
            git_backend: None,
            git_push_backend: None,
            lfs_patterns: None,
            html_description_max_bytes: None,
        }
    }
}

/// Per-folder Git overrides; only Git options can be overridden at folder level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct FolderGitOverride {
    pub git_remote: Option<String>,
    pub git_branch: Option<String>,
    pub git_mode: Option<String>,
}

/// Fully resolved effective config used during execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EffectiveConfig {
    pub security_check_enabled: bool,
    // ACL cache ttl (local evaluator cache)
    pub acl_cache_ttl_allow_ms: u64,
    pub acl_cache_ttl_deny_ms: u64,

    // Git (possibly overridden by folder)
    pub git_remote: Option<String>,
    pub git_branch: Option<String>,
    pub git_mode: String,
    pub git_backend: String,
    pub git_push_backend: String,
    pub lfs_patterns: Option<String>,

    pub html_description_max_bytes: usize,
}

impl EffectiveConfig {
    /// Build an effective config from Global + Filestore + optional Folder override.
    pub fn from_layers(global: &GlobalFilestoreConfig, fs: &FilestoreConfig, folder: Option<&FolderGitOverride>) -> Self {
        let security_check_enabled = fs.security_check_enabled;
        // ACL cache precedence: fs override if Some, else global
        let acl_cache_ttl_allow_ms = fs.acl_cache_ttl_allow_ms.unwrap_or(global.acl_cache_ttl_allow_ms);
        let acl_cache_ttl_deny_ms = fs.acl_cache_ttl_deny_ms.unwrap_or(global.acl_cache_ttl_deny_ms);

        // Git precedence: start with global, overlay filestore, then folder
        let mut git_remote = fs.git_remote.clone().or_else(|| global.git_remote.clone());
        let mut git_branch = fs.git_branch.clone().or_else(|| global.git_branch.clone());
        let mut git_mode = fs.git_mode.clone().unwrap_or_else(|| global.git_mode.clone());

        if let Some(ov) = folder {
            if ov.git_remote.is_some() { git_remote = ov.git_remote.clone(); }
            if ov.git_branch.is_some() { git_branch = ov.git_branch.clone(); }
            if ov.git_mode.is_some() { git_mode = ov.git_mode.clone().unwrap(); }
        }

        let git_backend = fs.git_backend.clone().unwrap_or_else(|| global.git_backend.clone());
        let git_push_backend = fs.git_push_backend.clone().unwrap_or_else(|| global.git_push_backend.clone());
        let lfs_patterns = fs.lfs_patterns.clone().or_else(|| global.lfs_patterns.clone());

        let html_description_max_bytes = fs.html_description_max_bytes.unwrap_or(global.html_description_max_bytes);

        Self {
            security_check_enabled,
            acl_cache_ttl_allow_ms,
            acl_cache_ttl_deny_ms,
            git_remote,
            git_branch,
            git_mode,
            git_backend,
            git_push_backend,
            lfs_patterns,
            html_description_max_bytes,
        }
    }
}

// Tests for filestore config live under tests/ and are wired via tests.rs
