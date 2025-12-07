//! FILESTORE scaffolding: config and path normalization utilities.
//! This module lays groundwork for the full FILESTORE implementation per plans.

pub mod paths;
pub mod config;
pub mod security;
pub mod kv;
pub mod host_path;
pub mod git;
pub mod correlation;
pub mod types;
pub mod ops;
pub mod registry;
pub mod show;
pub mod ddl;
pub mod gc;

// Re-export common types for early adopters
pub use config::{GlobalFilestoreConfig, FilestoreConfig, FolderGitOverride, EffectiveConfig};
pub use paths::{normalize_nfc, validate_logical_path, split_normalized_segments};
pub use security::{ACLAction, AclUser, AclContext, AclDecision, check_acl};
pub use host_path::{is_host_path_allowed, normalize_abs_path};
pub use correlation::{CorrelationId, correlation_id_opt_str};
pub use types::{FileMeta, Chunking, ChunkRef, Tree, Commit, RefInfo, Alias};
pub use ops::{ingest_from_bytes, get_file_meta, get_file_bytes, update_from_bytes, rename_file, delete_file, ingest_from_host_path, head_file_meta, list_files_by_prefix};
pub use ops::current_branch_head;
pub use registry::{FilestoreRegistryEntry, save_filestore_entry, load_filestore_entry, list_filestore_entries, drop_filestore_entry, alter_filestore_entry};
pub use show::{show_filestores_df, show_filestore_config_df, show_files_df, show_trees_df, show_commits_df, show_diff_df, show_chunks_df, show_aliases_df, show_admin_counts_df, show_files_df_paged, show_health_df};
pub use ops::{create_tree_from_prefix, commit_tree, load_tree, list_trees, list_commits};
pub use ddl::{create_filestore, alter_filestore_ddl, drop_filestore};
pub use gc::{gc_dry_run, gc_apply};
