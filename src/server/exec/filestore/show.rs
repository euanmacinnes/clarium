//! SHOW/TVF builders for FILESTORE information schema.
//! Read-only helpers that return Polars DataFrames following Junie Polars guidelines.

use anyhow::Result;
use polars::prelude::*;

use crate::storage::SharedStore;

use super::config::{EffectiveConfig, GlobalFilestoreConfig};
use super::registry::{list_filestore_entries, load_filestore_entry};
use super::types::{FileMeta, Tree, Commit, Alias};
use super::ops::{list_files_by_prefix, list_trees, list_commits, load_tree, diff_trees};
use super::kv::Keys;
use crate::storage::KvValue;

#[inline]
fn empty_files_df() -> Result<DataFrame> {
    Ok(DataFrame::new(vec![
        Series::new("logical_path".into(), Vec::<String>::new()).into(),
        Series::new("size".into(), Vec::<i64>::new()).into(),
        Series::new("etag".into(), Vec::<String>::new()).into(),
        Series::new("version".into(), Vec::<i64>::new()).into(),
        Series::new("updated_at".into(), Vec::<i64>::new()).into(),
        Series::new("deleted".into(), Vec::<bool>::new()).into(),
        Series::new("content_type".into(), Vec::<String>::new()).into(),
    ])?)
}

/// Build a DataFrame listing registered filestores for a database.
pub fn show_filestores_df(store: &SharedStore, database: &str) -> Result<DataFrame> {
    let entries = list_filestore_entries(store, database)?;
    let n = entries.len();
    let mut name: Vec<String> = Vec::with_capacity(n);
    let mut git_remote: Vec<String> = Vec::with_capacity(n);
    let mut git_branch: Vec<String> = Vec::with_capacity(n);
    let mut git_mode: Vec<String> = Vec::with_capacity(n);
    let mut git_push_backend: Vec<String> = Vec::with_capacity(n);
    let mut acl_url: Vec<String> = Vec::with_capacity(n);
    let mut acl_fail_open: Vec<bool> = Vec::with_capacity(n);
    let mut lfs_patterns: Vec<String> = Vec::with_capacity(n);
    let mut config_version: Vec<i64> = Vec::with_capacity(n);
    let mut created_at: Vec<i64> = Vec::with_capacity(n);
    let mut updated_at: Vec<i64> = Vec::with_capacity(n);

    for e in entries {
        name.push(e.name.clone());
        git_remote.push(e.config.git_remote.clone().unwrap_or_default());
        git_branch.push(e.config.git_branch.clone().unwrap_or_default());
        git_mode.push(e.config.git_mode.clone().unwrap_or_else(|| "plumbing_only".to_string()));
        git_push_backend.push(e.config.git_push_backend.clone().unwrap_or_else(|| "auto".to_string()));
        acl_url.push(e.config.acl_url.clone().unwrap_or_default());
        acl_fail_open.push(e.config.acl_fail_open.unwrap_or(false));
        lfs_patterns.push(e.config.lfs_patterns.clone().unwrap_or_default());
        config_version.push(e.config_version as i64);
        created_at.push(e.created_at);
        updated_at.push(e.updated_at);
    }

    let df = DataFrame::new(vec![
        Series::new("name".into(), name).into(),
        Series::new("git_remote".into(), git_remote).into(),
        Series::new("git_branch".into(), git_branch).into(),
        Series::new("git_mode".into(), git_mode).into(),
        Series::new("git_push_backend".into(), git_push_backend).into(),
        Series::new("acl_url".into(), acl_url).into(),
        Series::new("acl_fail_open".into(), acl_fail_open).into(),
        Series::new("lfs_patterns".into(), lfs_patterns).into(),
        Series::new("config_version".into(), config_version).into(),
        Series::new("created_at".into(), created_at).into(),
        Series::new("updated_at".into(), updated_at).into(),
    ])?;
    Ok(df)
}

/// Build a single-row DataFrame showing Global, Filestore, and Effective config for a filestore.
pub fn show_filestore_config_df(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    folder_prefix: Option<&str>,
) -> Result<DataFrame> {
    let global = GlobalFilestoreConfig::default();
    let fs_cfg = if let Some(ent) = load_filestore_entry(store, database, filestore)? { ent.config } else { super::config::FilestoreConfig::default() };
    let folder = folder_prefix.map(|_| super::config::FolderGitOverride::default());
    let eff = EffectiveConfig::from_layers(&global, &fs_cfg, folder.as_ref());

    // Single row of strings/bools/ints summarizing key fields
    let cols = vec![
        ("global_acl_url", global.acl_url.unwrap_or_default()),
        ("global_git_branch", global.git_branch.unwrap_or_else(|| "".to_string())),
        ("fs_acl_url", fs_cfg.acl_url.clone().unwrap_or_default()),
        ("fs_git_remote", fs_cfg.git_remote.clone().unwrap_or_default()),
        ("effective_git_remote", eff.git_remote.clone().unwrap_or_default()),
        ("effective_git_branch", eff.git_branch.clone().unwrap_or_default()),
        ("effective_git_mode", eff.git_mode.clone()),
        ("effective_git_push_backend", eff.git_push_backend.clone()),
        ("effective_lfs_patterns", eff.lfs_patterns.clone().unwrap_or_default()),
    ];

    // Build series ensuring correct types
    let mut names: Vec<String> = Vec::with_capacity(cols.len());
    let mut values: Vec<String> = Vec::with_capacity(cols.len());
    for (k, v) in cols {
        names.push(k.to_string());
        values.push(v);
    }

    // Additional scalar fields
    let bools = vec![
        ("effective_acl_fail_open", eff.acl_fail_open),
        ("security_check_enabled", eff.security_check_enabled),
    ];
    let mut bool_names: Vec<String> = Vec::with_capacity(bools.len());
    let mut bool_vals: Vec<bool> = Vec::with_capacity(bools.len());
    for (k, v) in bools { bool_names.push(k.to_string()); bool_vals.push(v); }

    // Construct DF with two blocks then horizontally concatenate
    let df1 = DataFrame::new(vec![Series::new("__name".into(), names.clone()).into(), Series::new("__value".into(), values).into()])?;
    // Pivot-like shape to single row
    let mut row_map = std::collections::BTreeMap::new();
    for i in 0..df1.height() {
        let k = df1.column("__name")?.get(i).ok().and_then(|v| v.get_str().map(|s| s.to_string())).unwrap_or_default();
        let v = df1.column("__value")?.get(i).ok().map(|av| av.to_string()).unwrap_or_default();
        row_map.insert(k, v);
    }
    // Build final columns
    let mut final_cols: Vec<Column> = Vec::new();
    for (k, v) in row_map.into_iter() {
        final_cols.push(Series::new(k.as_str().into(), vec![v]).into());
    }
    // Append boolean fields
    for (i, k) in bool_names.iter().enumerate() {
        let v = *bool_vals.get(i).unwrap_or(&false);
        final_cols.push(Series::new(k.as_str().into(), vec![v]).into());
    }
    let df = DataFrame::new(final_cols)?;
    Ok(df)
}

/// Show files within a filestore, optionally filtered by logical path prefix.
pub fn show_files_df(store: &SharedStore, database: &str, filestore: &str, prefix: Option<&str>) -> Result<DataFrame> {
    let list: Vec<FileMeta> = list_files_by_prefix(store, database, filestore, prefix)?;
    let n = list.len();
    let mut logical_path: Vec<String> = Vec::with_capacity(n);
    let mut size: Vec<i64> = Vec::with_capacity(n);
    let mut etag: Vec<String> = Vec::with_capacity(n);
    let mut version: Vec<i64> = Vec::with_capacity(n);
    let mut updated_at: Vec<i64> = Vec::with_capacity(n);
    let mut deleted: Vec<bool> = Vec::with_capacity(n);
    let mut content_type: Vec<String> = Vec::with_capacity(n);
    for m in list.into_iter() {
        logical_path.push(m.logical_path);
        size.push(m.size as i64);
        etag.push(m.etag);
        version.push(m.version as i64);
        updated_at.push(m.updated_at);
        deleted.push(m.deleted);
        content_type.push(m.content_type.unwrap_or_default());
    }
    let df = DataFrame::new(vec![
        Series::new("logical_path".into(), logical_path).into(),
        Series::new("size".into(), size).into(),
        Series::new("etag".into(), etag).into(),
        Series::new("version".into(), version).into(),
        Series::new("updated_at".into(), updated_at).into(),
        Series::new("deleted".into(), deleted).into(),
        Series::new("content_type".into(), content_type).into(),
    ])?;
    Ok(df)
}

/// Show trees persisted for a filestore.
pub fn show_trees_df(store: &SharedStore, database: &str, filestore: &str) -> Result<DataFrame> {
    let list: Vec<Tree> = list_trees(store, database, filestore)?;
    let n = list.len();
    let mut id: Vec<String> = Vec::with_capacity(n);
    let mut created_at: Vec<i64> = Vec::with_capacity(n);
    let mut entry_count: Vec<i64> = Vec::with_capacity(n);
    for t in list.into_iter() {
        id.push(t.id);
        created_at.push(t.created_at);
        entry_count.push(t.entries.len() as i64);
    }
    let df = DataFrame::new(vec![
        Series::new("id".into(), id).into(),
        Series::new("created_at".into(), created_at).into(),
        Series::new("entry_count".into(), entry_count).into(),
    ])?;
    Ok(df)
}

/// Show commits persisted for a filestore.
pub fn show_commits_df(store: &SharedStore, database: &str, filestore: &str) -> Result<DataFrame> {
    let list: Vec<Commit> = list_commits(store, database, filestore)?;
    let n = list.len();
    let mut id: Vec<String> = Vec::with_capacity(n);
    let mut tree_id: Vec<String> = Vec::with_capacity(n);
    let mut parents: Vec<String> = Vec::with_capacity(n);
    let mut author: Vec<String> = Vec::with_capacity(n);
    let mut message: Vec<String> = Vec::with_capacity(n);
    let mut tags: Vec<String> = Vec::with_capacity(n);
    let mut git_sha: Vec<String> = Vec::with_capacity(n);
    let mut created_at: Vec<i64> = Vec::with_capacity(n);
    for c in list.into_iter() {
        id.push(c.id);
        tree_id.push(c.tree_id);
        parents.push(if c.parents.is_empty() { String::new() } else { c.parents.join(",") });
        author.push(format!("{} <{}>", c.author.name, c.author.email));
        message.push(c.message);
        tags.push(if c.tags.is_empty() { String::new() } else { c.tags.join(",") });
        git_sha.push(c.git_sha.unwrap_or_default());
        created_at.push(c.created_at);
    }
    let df = DataFrame::new(vec![
        Series::new("id".into(), id).into(),
        Series::new("tree_id".into(), tree_id).into(),
        Series::new("parents".into(), parents).into(),
        Series::new("author".into(), author).into(),
        Series::new("message".into(), message).into(),
        Series::new("tags".into(), tags).into(),
        Series::new("git_sha".into(), git_sha).into(),
        Series::new("created_at".into(), created_at).into(),
    ])?;
    Ok(df)
}

/// Show diff between two tree IDs, or between a tree and the current live prefix if `right_tree_id` is None and `live_prefix` provided.
pub fn show_diff_df(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    left_tree_id: &str,
    right_tree_id: Option<&str>,
    live_prefix: Option<&str>,
) -> Result<DataFrame> {
    // Load left tree (required)
    let left = load_tree(store, database, filestore, left_tree_id)?.ok_or_else(|| anyhow::anyhow!("left_tree_not_found"))?;
    // Determine right tree
    let right: super::types::Tree = if let Some(rid) = right_tree_id {
        load_tree(store, database, filestore, rid)?.ok_or_else(|| anyhow::anyhow!("right_tree_not_found"))?
    } else {
        // Build a temporary live snapshot for the given prefix
        let files = list_files_by_prefix(store, database, filestore, live_prefix)?;
        let entries: Vec<super::types::TreeEntry> = files.into_iter().filter(|m| !m.deleted)
            .map(|m| super::types::TreeEntry { path: m.logical_path, file_id: m.id, etag: m.etag, size: m.size }).collect();
        super::types::Tree { id: "__live__".to_string(), created_at: 0, entries }
    };
    let diff = diff_trees(&left, &right);
    let n = diff.len();
    let mut path: Vec<String> = Vec::with_capacity(n);
    let mut change: Vec<String> = Vec::with_capacity(n);
    let mut size_old: Vec<i64> = Vec::with_capacity(n);
    let mut size_new: Vec<i64> = Vec::with_capacity(n);
    let mut etag_old: Vec<String> = Vec::with_capacity(n);
    let mut etag_new: Vec<String> = Vec::with_capacity(n);
    for (p, ch, so, sn, eo, en) in diff.into_iter() {
        path.push(p);
        change.push(ch);
        size_old.push(so.unwrap_or(0) as i64);
        size_new.push(sn.unwrap_or(0) as i64);
        etag_old.push(eo.unwrap_or_default());
        etag_new.push(en.unwrap_or_default());
    }
    let df = DataFrame::new(vec![
        Series::new("path".into(), path).into(),
        Series::new("change".into(), change).into(),
        Series::new("size_old".into(), size_old).into(),
        Series::new("size_new".into(), size_new).into(),
        Series::new("etag_old".into(), etag_old).into(),
        Series::new("etag_new".into(), etag_new).into(),
    ])?;
    Ok(df)
}

/// Show chunks present in a filestore by scanning the chunk namespace.
pub fn show_chunks_df(store: &SharedStore, database: &str, filestore: &str) -> Result<DataFrame> {
    let kv = store.kv_store(database, filestore);
    // Build prefix for chunks: derive from a nil UUID pattern similar to trees/commits
    let prefix_sample = Keys::chunk(database, filestore, &uuid::Uuid::nil());
    let prefix = &prefix_sample[..prefix_sample.len() - uuid::Uuid::nil().to_string().len()];
    let mut oid: Vec<String> = Vec::new();
    let mut size: Vec<i64> = Vec::new();
    let mut ref_count: Vec<i64> = Vec::new();
    for k in kv.keys() {
        if !k.starts_with(prefix) { continue; }
        // Key ends with UUID; use it as oid
        if let Some(id_part) = k.split("::").last() {
            oid.push(id_part.to_string());
            // Size unknown without loading bytes; attempt to read Bytes value if present
            let sz = match kv.get(&k) { Some(KvValue::Bytes(b)) => b.len() as i64, _ => 0 };
            size.push(sz);
            // ref_count not tracked yet; default 0
            ref_count.push(0);
        }
    }
    let df = DataFrame::new(vec![
        Series::new("oid".into(), oid).into(),
        Series::new("size".into(), size).into(),
        Series::new("ref_count".into(), ref_count).into(),
    ])?;
    Ok(df)
}

/// Show aliases defined for a filestore by scanning alias keys.
pub fn show_aliases_df(store: &SharedStore, database: &str, filestore: &str) -> Result<DataFrame> {
    let kv = store.kv_store(database, filestore);
    let prefix_sample = Keys::alias(database, filestore, "");
    // Drop empty tail — Keys::alias concatenates after '::'
    let prefix = &prefix_sample[..];
    let mut alias: Vec<String> = Vec::new();
    let mut folder_prefix: Vec<String> = Vec::new();
    let mut target_store: Vec<String> = Vec::new();
    let mut target_prefix: Vec<String> = Vec::new();
    for k in kv.keys() {
        if !k.starts_with(prefix) { continue; }
        if let Some(KvValue::Json(j)) = kv.get(&k) {
            if let Ok(a) = serde_json::from_value::<Alias>(j) {
                alias.push(a.alias);
                folder_prefix.push(a.folder_prefix);
                target_store.push(a.target_store.unwrap_or_default());
                target_prefix.push(a.target_prefix.unwrap_or_default());
            }
        }
    }
    let df = DataFrame::new(vec![
        Series::new("alias".into(), alias).into(),
        Series::new("folder_prefix".into(), folder_prefix).into(),
        Series::new("target_store".into(), target_store).into(),
        Series::new("target_prefix".into(), target_prefix).into(),
    ])?;
    Ok(df)
}

/// Show admin counts for a filestore: files (live/tombstoned), chunks, trees, commits.
pub fn show_admin_counts_df(store: &SharedStore, database: &str, filestore: &str) -> Result<DataFrame> {
    let kv = store.kv_store(database, filestore);
    // Files
    let path_prefix = Keys::path(database, filestore, "");
    let mut files_live = 0i64;
    let mut files_tomb = 0i64;
    for k in kv.keys() {
        if !k.starts_with(&path_prefix) { continue; }
        if let Some(KvValue::Json(j)) = kv.get(&k) {
            if let Ok(m) = serde_json::from_value::<FileMeta>(j) {
                if m.deleted { files_tomb += 1; } else { files_live += 1; }
            }
        }
    }
    // Chunks
    let chunk_prefix_sample = Keys::chunk(database, filestore, &uuid::Uuid::nil());
    let chunk_prefix = &chunk_prefix_sample[..chunk_prefix_sample.len() - uuid::Uuid::nil().to_string().len()];
    let mut chunks = 0i64;
    for k in kv.keys() { if k.starts_with(chunk_prefix) { chunks += 1; } }
    // Trees
    let tree_prefix_sample = Keys::tree(database, filestore, &uuid::Uuid::nil());
    let tree_prefix = &tree_prefix_sample[..tree_prefix_sample.len() - uuid::Uuid::nil().to_string().len()];
    let mut trees = 0i64; for k in kv.keys() { if k.starts_with(tree_prefix) { trees += 1; } }
    // Commits
    let com_prefix_sample = Keys::commit(database, filestore, &uuid::Uuid::nil());
    let com_prefix = &com_prefix_sample[..com_prefix_sample.len() - uuid::Uuid::nil().to_string().len()];
    let mut commits = 0i64; for k in kv.keys() { if k.starts_with(com_prefix) { commits += 1; } }

    let df = DataFrame::new(vec![
        Series::new("files_live".into(), vec![files_live]).into(),
        Series::new("files_tombstoned".into(), vec![files_tomb]).into(),
        Series::new("chunks".into(), vec![chunks]).into(),
        Series::new("trees".into(), vec![trees]).into(),
        Series::new("commits".into(), vec![commits]).into(),
    ])?;
    Ok(df)
}

/// Paged file listing with optional logical prefix filter.
/// If `offset` is greater than available rows, returns empty.
pub fn show_files_df_paged(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    prefix: Option<&str>,
    offset: usize,
    limit: Option<usize>,
) -> Result<DataFrame> {
    let df_all = show_files_df(store, database, filestore, prefix)?;
    let h = df_all.height();
    if offset >= h {
        return empty_files_df();
    }
    let start = offset as i64;
    let len = match limit { Some(n) => n.min(h - offset), None => h - offset } as usize;
    Ok(df_all.slice(start, len))
}

#[cfg(test)]
mod show_tests;

/// Health summary: orphaned chunks, stale refs, and config mismatches.
/// Current implementation provides conservative counts; deeper checks will be added later.
pub fn show_health_df(store: &SharedStore, database: &str, filestore: &str) -> Result<DataFrame> {
    // Orphaned chunks: unknown without reverse index — return 0 for now
    let orphaned_chunks: i64 = 0;
    // Stale refs: if a local ref points to non-existent commit
    let kv = store.kv_store(database, filestore);
    let mut stale_refs: i64 = 0;
    let ref_prefix = super::kv::Keys::git_ref(database, filestore, "local", "");
    for k in kv.keys() {
        if !k.starts_with(&ref_prefix) { continue; }
        if let Some(crate::storage::KvValue::Json(j)) = kv.get(&k) {
            if let Ok(r) = serde_json::from_value::<super::types::RefInfo>(j) {
                let commit_key = super::kv::Keys::commit(database, filestore, &uuid::Uuid::parse_str(&r.head_commit_id).unwrap_or(uuid::Uuid::nil()));
                if kv.get(&commit_key).is_none() { stale_refs += 1; }
            }
        }
    }
    // Config mismatches: not tracked yet — 0
    let config_mismatches: i64 = 0;
    let df = DataFrame::new(vec![
        Series::new("orphaned_chunks".into(), vec![orphaned_chunks]).into(),
        Series::new("stale_refs".into(), vec![stale_refs]).into(),
        Series::new("config_mismatches".into(), vec![config_mismatches]).into(),
    ])?;
    Ok(df)
}
