use std::path::{Path, PathBuf};

/// Centralized helpers for system-wide folders rooted at a database root.
/// This keeps locations consistent across modules (views, UDFs, etc.).
#[inline]
pub fn system_root(db_root: &Path) -> PathBuf { db_root.join(".system") }

// ---- UDF roots (under .system/udf) ----
#[inline]
pub fn udf_root(db_root: &Path) -> PathBuf { system_root(db_root).join("udf") }

#[inline]
pub fn udf_scalars_dir(db_root: &Path) -> PathBuf { udf_root(db_root).join("scalars") }

#[inline]
pub fn udf_aggregates_dir(db_root: &Path) -> PathBuf { udf_root(db_root).join("aggregates") }

#[inline]
pub fn udf_constraints_dir(db_root: &Path) -> PathBuf { udf_root(db_root).join("constraints") }

#[inline]
pub fn udf_tvfs_dir(db_root: &Path) -> PathBuf { udf_root(db_root).join("tvfs") }

// ---- System views (under .system/<schema>) ----
#[inline]
pub fn pg_catalog_views_dir(db_root: &Path) -> PathBuf { system_root(db_root).join("pg_catalog") }

#[inline]
pub fn information_schema_views_dir(db_root: &Path) -> PathBuf { system_root(db_root).join("information_schema") }

// ---- Repository defaults (relative to the crate root) ----
#[inline]
pub fn repo_scripts_root() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts") }

#[inline]
pub fn repo_udf_subdir(sub: &str) -> PathBuf { repo_scripts_root().join(sub) }

#[inline]
pub fn repo_system_views_root() -> PathBuf { repo_scripts_root().join("system_views") }
