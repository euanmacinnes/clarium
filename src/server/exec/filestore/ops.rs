//! Core FILESTORE operations: ingest/update/get using in-memory KV backend.
//! These are early functional implementations aligned with the full plan.

use anyhow::{bail, Result};
use uuid::Uuid;
use chrono::Utc;

use crate::storage::{SharedStore, KvValue};

use super::paths::{validate_logical_path, normalize_nfc};
use super::security::{AclUser, AclContext, ACLAction, check_acl};
use super::config::EffectiveConfig;
use super::kv::{Keys, etag_for_bytes};
use super::types::{FileMeta, Tree, TreeEntry, Commit, CommitAuthor, RefInfo};
use super::host_path::{is_host_path_allowed, normalize_abs_path};

/// Ingest file content from raw bytes. Stores bytes and writes metadata.
/// Returns the written FileMeta on success.
pub async fn ingest_from_bytes(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    logical_path: &str,
    bytes: &[u8],
    content_type: Option<&str>,
    description_html: Option<&str>,
    user: &AclUser,
    eff: &EffectiveConfig,
    ctx: &AclContext,
) -> Result<FileMeta> {
    validate_logical_path(logical_path)?;
    let path_nfc = normalize_nfc(logical_path);

    // Enforce metadata limits
    if let Some(ct) = content_type {
        if ct.len() > 255 { bail!("content_type_too_long"); }
    }
    if let Some(desc) = description_html {
        if desc.as_bytes().len() > eff.html_description_max_bytes { bail!("description_html_too_large"); }
    }

    // ACL gate
    let cm = super::security::ContentMeta { size_bytes: Some(bytes.len() as u64), media_type: content_type.map(|s| s.to_string()) };
    let mut ctx2 = ctx.clone();
    ctx2.content_meta = Some(cm);
    let decision = check_acl(eff, user, ACLAction::Write, &path_nfc, None, &ctx2, filestore).await;
    if !decision.allow {
        bail!(decision.reason.unwrap_or_else(|| "acl_denied".to_string()));
    }

    let size = bytes.len() as u64;
    let etag = etag_for_bytes(bytes);
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();

    // Persist bytes then metadata
    let blob_key = Keys::blob(database, filestore, &Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil()));
    {
        let kv = store.kv_store(database, filestore);
        kv.set_bytes(blob_key, bytes, None, None);
    }

    let meta = FileMeta {
        id: id.clone(),
        logical_path: path_nfc.clone(),
        size,
        etag: etag.clone(),
        version: 1,
        created_at: now,
        updated_at: now,
        content_type: content_type.map(|s| s.to_string()),
        deleted: false,
        description_html: description_html.map(|s| s.to_string()),
        custom: None,
        chunking: None,
    };

    let path_key = Keys::path(database, filestore, &path_nfc);
    let kv = store.kv_store(database, filestore);
    let meta_json = serde_json::to_value(&meta)?;
    kv.set(path_key, KvValue::Json(meta_json), None, None);

    let corr = ctx.request_id.as_deref().unwrap_or("-");
    let desc_len = meta.description_html.as_ref().map(|s| s.len()).unwrap_or(0);
    crate::tprintln!("FILESTORE ingest_from_bytes ok fs={} path={} size={} etag={} ct_len={} desc_len={} [corr={}]",
        filestore, path_nfc, size, etag, content_type.map(|s| s.len()).unwrap_or(0), desc_len, corr);

    Ok(meta)
}

/// Fetch FileMeta for a logical path, if present.
pub fn get_file_meta(store: &SharedStore, database: &str, filestore: &str, logical_path: &str) -> Result<Option<FileMeta>> {
    validate_logical_path(logical_path)?;
    let path_nfc = normalize_nfc(logical_path);
    let kv = store.kv_store(database, filestore);
    let key = Keys::path(database, filestore, &path_nfc);
    if let Some(KvValue::Json(j)) = kv.get(&key) {
        let meta: FileMeta = serde_json::from_value(j)?;
        Ok(Some(meta))
    } else {
        Ok(None)
    }
}

/// Fetch raw bytes for a file by its metadata (id).
pub fn get_file_bytes(store: &SharedStore, database: &str, filestore: &str, meta: &FileMeta) -> Result<Option<Vec<u8>>> {
    let uuid = Uuid::parse_str(&meta.id).unwrap_or_else(|_| Uuid::nil());
    let key = Keys::blob(database, filestore, &uuid);
    let kv = store.kv_store(database, filestore);
    Ok(kv.get_bytes(&key))
}

/// Lightweight metadata fetch for preflight checks.
#[derive(Debug, Clone)]
pub struct HeadMeta {
    pub exists: bool,
    pub etag: Option<String>,
    pub version: Option<u64>,
    pub size: Option<u64>,
    pub deleted: bool,
}

/// Return minimal metadata for a logical path (exists/etag/version/size/deleted).
pub fn head_file_meta(store: &SharedStore, database: &str, filestore: &str, logical_path: &str) -> Result<HeadMeta> {
    match get_file_meta(store, database, filestore, logical_path)? {
        Some(m) => Ok(HeadMeta { exists: !m.deleted, etag: Some(m.etag), version: Some(m.version), size: Some(m.size), deleted: m.deleted }),
        None => Ok(HeadMeta { exists: false, etag: None, version: None, size: None, deleted: false }),
    }
}

/// Update existing file content by logical path with optimistic concurrency using `if_match_etag`.
pub async fn update_from_bytes(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    logical_path: &str,
    if_match_etag: &str,
    bytes: &[u8],
    content_type: Option<&str>,
    description_html: Option<&str>,
    user: &AclUser,
    eff: &EffectiveConfig,
    ctx: &AclContext,
) -> Result<FileMeta> {
    // Fetch current meta
    let cur = get_file_meta(store, database, filestore, logical_path)?
        .ok_or_else(|| anyhow::anyhow!("not_found"))?;
    if cur.deleted { bail!("gone"); }
    if cur.etag != if_match_etag { bail!("precondition_failed"); }

    // Enforce metadata limits
    if let Some(ct) = content_type { if ct.len() > 255 { bail!("content_type_too_long"); } }
    if let Some(desc) = description_html { if desc.as_bytes().len() > eff.html_description_max_bytes { bail!("description_html_too_large"); } }

    // ACL gate
    let cm = super::security::ContentMeta { size_bytes: Some(bytes.len() as u64), media_type: content_type.map(|s| s.to_string()) };
    let mut ctx2 = ctx.clone();
    ctx2.content_meta = Some(cm);
    let decision = check_acl(eff, user, ACLAction::Write, &cur.logical_path, None, &ctx2, filestore).await;
    if !decision.allow { bail!(decision.reason.unwrap_or_else(|| "acl_denied".to_string())); }

    // Overwrite blob and update meta
    let size = bytes.len() as u64;
    let etag = etag_for_bytes(bytes);
    let now = Utc::now().timestamp();
    let uuid = Uuid::parse_str(&cur.id).unwrap_or_else(|_| Uuid::nil());
    let blob_key = Keys::blob(database, filestore, &uuid);
    let kv = store.kv_store(database, filestore);
    kv.set_bytes(blob_key, bytes, None, None);

    let mut meta = cur;
    meta.size = size;
    meta.etag = etag.clone();
    meta.updated_at = now;
    meta.version = meta.version.saturating_add(1);
    meta.content_type = content_type.map(|s| s.to_string()).or(meta.content_type);
    if description_html.is_some() { meta.description_html = description_html.map(|s| s.to_string()); }

    let path_key = Keys::path(database, filestore, &meta.logical_path);
    kv.set(path_key, KvValue::Json(serde_json::to_value(&meta)?), None, None);
    let corr = ctx.request_id.as_deref().unwrap_or("-");
    let desc_len = meta.description_html.as_ref().map(|s| s.len()).unwrap_or(0);
    crate::tprintln!("FILESTORE update_from_bytes ok fs={} path={} size={} etag={} ct_len={} desc_len={} [corr={}]",
        filestore, meta.logical_path, size, etag, meta.content_type.as_ref().map(|s| s.len()).unwrap_or(0), desc_len, corr);
    Ok(meta)
}

/// Rename a file from old logical path to new logical path.
pub async fn rename_file(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    old_path: &str,
    new_path: &str,
    user: &AclUser,
    eff: &EffectiveConfig,
    ctx: &AclContext,
) -> Result<FileMeta> {
    validate_logical_path(old_path)?; validate_logical_path(new_path)?;
    let old_nfc = normalize_nfc(old_path);
    let new_nfc = normalize_nfc(new_path);

    // ACL Move for old and new
    let d1 = check_acl(eff, user, ACLAction::Move, &old_nfc, None, ctx, filestore).await;
    if !d1.allow { bail!(d1.reason.unwrap_or_else(|| "acl_denied_old".to_string())); }
    let d2 = check_acl(eff, user, ACLAction::Move, &new_nfc, Some(&old_nfc), ctx, filestore).await;
    if !d2.allow { bail!(d2.reason.unwrap_or_else(|| "acl_denied_new".to_string())); }

    let kv = store.kv_store(database, filestore);
    let old_key = Keys::path(database, filestore, &old_nfc);
    let meta_val = kv.get(&old_key).ok_or_else(|| anyhow::anyhow!("not_found"))?;
    let mut meta: FileMeta = match meta_val { KvValue::Json(j) => serde_json::from_value(j)?, _ => bail!("corrupt_meta") };
    if meta.deleted { bail!("gone"); }

    // Write new meta and tombstone old
    let now = Utc::now().timestamp();
    let mut new_meta = meta.clone();
    new_meta.logical_path = new_nfc.clone();
    new_meta.version = new_meta.version.saturating_add(1);
    new_meta.updated_at = now;

    let new_key = Keys::path(database, filestore, &new_nfc);
    kv.set(new_key, KvValue::Json(serde_json::to_value(&new_meta)?), None, None);

    meta.deleted = true;
    meta.updated_at = now;
    meta.version = meta.version.saturating_add(1);
    kv.set(old_key, KvValue::Json(serde_json::to_value(&meta)?), None, None);
    let corr = ctx.request_id.as_deref().unwrap_or("-");
    crate::tprintln!("FILESTORE rename_file ok fs={} {} -> {} [corr={}]", filestore, old_nfc, new_nfc, corr);
    Ok(new_meta)
}

/// Soft-delete a file by logical path.
pub async fn delete_file(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    logical_path: &str,
    user: &AclUser,
    eff: &EffectiveConfig,
    ctx: &AclContext,
) -> Result<()> {
    let path_nfc = normalize_nfc(logical_path);
    let decision = check_acl(eff, user, ACLAction::Delete, &path_nfc, None, ctx, filestore).await;
    if !decision.allow { bail!(decision.reason.unwrap_or_else(|| "acl_denied".to_string())); }
    let kv = store.kv_store(database, filestore);
    let key = Keys::path(database, filestore, &path_nfc);
    let val = kv.get(&key).ok_or_else(|| anyhow::anyhow!("not_found"))?;
    let mut meta: FileMeta = match val { KvValue::Json(j) => serde_json::from_value(j)?, _ => bail!("corrupt_meta") };
    if meta.deleted { return Ok(()); }
    meta.deleted = true;
    meta.updated_at = Utc::now().timestamp();
    meta.version = meta.version.saturating_add(1);
    kv.set(key, KvValue::Json(serde_json::to_value(&meta)?), None, None);
    let corr = ctx.request_id.as_deref().unwrap_or("-");
    crate::tprintln!("FILESTORE delete_file ok fs={} path={} [corr={}]", filestore, path_nfc, corr);
    Ok(())
}

/// Ingest content from a host path after allowlist validation.
pub async fn ingest_from_host_path(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    logical_path: &str,
    host_path: &str,
    allowlist: &str,
    content_type: Option<&str>,
    user: &AclUser,
    eff: &EffectiveConfig,
    ctx: &AclContext,
) -> Result<FileMeta> {
    // Host path validation
    let _abs = normalize_abs_path(host_path)?;
    let allowed = is_host_path_allowed(host_path, allowlist)?;
    if !allowed { bail!("host_path_not_allowed"); }
    let bytes = std::fs::read(host_path)?;
    ingest_from_bytes(store, database, filestore, logical_path, &bytes, content_type, None, user, eff, ctx).await
}

/// List files by logical path prefix. When `prefix` is None or empty, lists all.
pub fn list_files_by_prefix(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    prefix: Option<&str>,
) -> Result<Vec<FileMeta>> {
    let logical_prefix_nfc = prefix.map(|p| normalize_nfc(p)).unwrap_or_else(String::new);
    let key_space_prefix = if logical_prefix_nfc.is_empty() {
        // path namespace prefix only
        format!("{}{}", super::kv::Keys::path(database, filestore, ""), "")
    } else {
        super::kv::Keys::path(database, filestore, &logical_prefix_nfc)
    };
    let kv = store.kv_store(database, filestore);
    let mut out = Vec::new();
    for k in kv.keys() {
        if !k.starts_with(&key_space_prefix) { continue; }
        if let Some(KvValue::Json(j)) = kv.get(&k) {
            if let Ok(meta) = serde_json::from_value::<FileMeta>(j) {
                out.push(meta);
            }
        }
    }
    // Keep a stable order by logical_path
    out.sort_by(|a, b| a.logical_path.cmp(&b.logical_path));
    Ok(out)
}

/// Read the current HEAD commit id for a given branch if present.
/// Returns Some(<commit_id>) or None when the ref is missing or malformed.
pub fn current_branch_head(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    branch: &str,
) -> Option<String> {
    let kv = store.kv_store(database, filestore);
    let key = Keys::git_ref(database, filestore, "local", branch);
    match kv.get(&key) {
        Some(KvValue::Json(j)) => serde_json::from_value::<RefInfo>(j).ok().map(|r| r.head_commit_id),
        _ => None,
    }
}

/// Create a tree snapshot from a logical folder prefix. Includes non-deleted files only.
pub fn create_tree_from_prefix(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    prefix: Option<&str>,
) -> Result<Tree> {
    let files = list_files_by_prefix(store, database, filestore, prefix)?;
    let entries: Vec<TreeEntry> = files
        .into_iter()
        .filter(|m| !m.deleted)
        .map(|m| TreeEntry { path: m.logical_path, file_id: m.id, etag: m.etag, size: m.size })
        .collect();
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    let tree = Tree { id: id.clone(), created_at: now, entries };
    let key = Keys::tree(database, filestore, &Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil()));
    let kv = store.kv_store(database, filestore);
    kv.set(key, KvValue::Json(serde_json::to_value(&tree)?), None, None);
    crate::tprintln!("FILESTORE create_tree_from_prefix ok fs={} prefix={} entries={} tree_id={}",
        filestore, prefix.unwrap_or(""), tree.entries.len(), id);
    Ok(tree)
}

/// Persist a commit object that points to a tree and updates the branch ref.
pub fn commit_tree(
    store: &SharedStore,
    database: &str,
    filestore: &str,
    tree_id: &str,
    parents: &[String],
    author: &CommitAuthor,
    message: &str,
    tags: &[String],
    branch: &str,
) -> Result<Commit> {
    // Basic existence check for the tree
    let tree_key = Keys::tree(database, filestore, &Uuid::parse_str(tree_id).unwrap_or_else(|_| Uuid::nil()));
    let kv = store.kv_store(database, filestore);
    if kv.get(&tree_key).is_none() { bail!("tree_not_found"); }

    // If no parents provided, infer from current branch head
    let parents_eff: Vec<String> = if parents.is_empty() {
        if let Some(head) = current_branch_head(store, database, filestore, branch) { vec![head] } else { vec![] }
    } else { parents.to_vec() };

    // Normalize tags: trim, drop empty, dedupe, stable order
    let mut tag_set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for t in tags.iter() { let tt = t.trim(); if !tt.is_empty() { tag_set.insert(tt.to_string()); } }
    let tags_norm: Vec<String> = tag_set.into_iter().collect();

    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    let commit = Commit {
        id: id.clone(),
        parents: parents_eff,
        tree_id: tree_id.to_string(),
        author: CommitAuthor { name: author.name.clone(), email: author.email.clone(), time_unix: author.time_unix },
        message: message.to_string(),
        tags: tags_norm,
        git_sha: None,
        created_at: now,
    };
    let commit_key = Keys::commit(database, filestore, &Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::nil()));
    kv.set(commit_key, KvValue::Json(serde_json::to_value(&commit)?), None, None);

    // Update branch ref
    let ref_key = Keys::git_ref(database, filestore, "local", branch);
    let ref_info = RefInfo { branch: branch.to_string(), head_commit_id: id.clone(), updated_at: now };
    kv.set(ref_key, KvValue::Json(serde_json::to_value(&ref_info)?), None, None);
    crate::tprintln!("FILESTORE commit_tree ok fs={} branch={} commit_id={} tree_id={}", filestore, branch, id, tree_id);
    Ok(commit)
}

/// Helper: load a Tree by id.
pub fn load_tree(store: &SharedStore, database: &str, filestore: &str, tree_id: &str) -> Result<Option<Tree>> {
    let key = Keys::tree(database, filestore, &Uuid::parse_str(tree_id).unwrap_or_else(|_| Uuid::nil()));
    let kv = store.kv_store(database, filestore);
    Ok(match kv.get(&key) {
        Some(KvValue::Json(j)) => serde_json::from_value::<Tree>(j).ok(),
        _ => None,
    })
}

/// Helper: load a Commit by id.
pub fn load_commit(store: &SharedStore, database: &str, filestore: &str, commit_id: &str) -> Result<Option<Commit>> {
    let key = Keys::commit(database, filestore, &Uuid::parse_str(commit_id).unwrap_or_else(|_| Uuid::nil()));
    let kv = store.kv_store(database, filestore);
    Ok(match kv.get(&key) {
        Some(KvValue::Json(j)) => serde_json::from_value::<Commit>(j).ok(),
        _ => None,
    })
}

/// List all Trees for a filestore by scanning keys.
pub fn list_trees(store: &SharedStore, database: &str, filestore: &str) -> Result<Vec<Tree>> {
    let kv = store.kv_store(database, filestore);
    let prefix = Keys::tree(database, filestore, &Uuid::nil());
    let mut out = Vec::new();
    for k in kv.keys() {
        if k.starts_with(&prefix[..prefix.len()-Uuid::nil().to_string().len()]) {
            if let Some(KvValue::Json(j)) = kv.get(&k) {
                if let Ok(t) = serde_json::from_value::<Tree>(j) { out.push(t); }
            }
        }
    }
    // Stable order by created_at then id
    out.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
    Ok(out)
}

/// List all Commits for a filestore by scanning keys.
pub fn list_commits(store: &SharedStore, database: &str, filestore: &str) -> Result<Vec<Commit>> {
    let kv = store.kv_store(database, filestore);
    let prefix = Keys::commit(database, filestore, &Uuid::nil());
    let mut out = Vec::new();
    for k in kv.keys() {
        if k.starts_with(&prefix[..prefix.len()-Uuid::nil().to_string().len()]) {
            if let Some(KvValue::Json(j)) = kv.get(&k) {
                if let Ok(c) = serde_json::from_value::<Commit>(j) { out.push(c); }
            }
        }
    }
    // Newest first
    out.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id)));
    Ok(out)
}

/// Compute a simple diff between two trees: added/removed/modified by etag or size.
pub fn diff_trees(
    left: &Tree,
    right: &Tree,
) -> Vec<(String, String, Option<u64>, Option<u64>, Option<String>, Option<String>)> {
    use std::collections::HashMap;
    let mut lm: HashMap<&str, &TreeEntry> = HashMap::new();
    let mut rm: HashMap<&str, &TreeEntry> = HashMap::new();
    for e in &left.entries { lm.insert(e.path.as_str(), e); }
    for e in &right.entries { rm.insert(e.path.as_str(), e); }

    let mut paths: Vec<&str> = lm.keys().copied().chain(rm.keys().copied()).collect();
    paths.sort(); paths.dedup();
    let mut out = Vec::new();
    for p in paths {
        match (lm.get(p), rm.get(p)) {
            (None, Some(rb)) => out.push((p.to_string(), "added".to_string(), None, Some(rb.size), None, Some(rb.etag.clone()))),
            (Some(lb), None) => out.push((p.to_string(), "removed".to_string(), Some(lb.size), None, Some(lb.etag.clone()), None)),
            (Some(lb), Some(rb)) => {
                if lb.etag != rb.etag || lb.size != rb.size {
                    out.push((p.to_string(), "modified".to_string(), Some(lb.size), Some(rb.size), Some(lb.etag.clone()), Some(rb.etag.clone())));
                }
            }
            _ => {}
        }
    }
    out
}

