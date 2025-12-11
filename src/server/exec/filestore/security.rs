use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

use super::config::EffectiveConfig;
use super::sec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ACLAction {
    Read,
    Write,
    Delete,
    Move,
    Copy,
    Rename,
    List,
    Commit,
    Push,
    Pull,
    Clone,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AclUser {
    pub id: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub ip: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct AclContext {
    #[serde(default)]
    pub filestore_config_version: Option<u32>,
    #[serde(default)]
    pub content_meta: Option<ContentMeta>,
    #[serde(default)]
    pub git: Option<GitCtx>,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ContentMeta {
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct GitCtx {
    #[serde(default)]
    pub remote: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AclDecision {
    pub allow: bool,
    pub reason: Option<String>,
    #[serde(default)]
    pub effective_perms: Vec<String>,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
}

impl AclDecision {
    pub fn allow(reason: impl Into<String>) -> Self {
        Self { allow: true, reason: Some(reason.into()), effective_perms: vec![], ttl_ms: None }
    }
    pub fn deny(reason: impl Into<String>) -> Self {
        Self { allow: false, reason: Some(reason.into()), effective_perms: vec![], ttl_ms: None }
    }
}

/// In-memory TTL cache for ACL decisions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    filestore: String,
    user: String,
    action: String,
    path: String,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    decision: AclDecision,
    expires_at: Instant,
}

static ACL_CACHE: Lazy<RwLock<HashMap<CacheKey, CacheEntry>>> = Lazy::new(|| RwLock::new(HashMap::new()));

// Basic guardrails & counters for observability
const ACL_CACHE_MAX: usize = 10_000;
static ACL_HITS: AtomicU64 = AtomicU64::new(0);
static ACL_MISSES: AtomicU64 = AtomicU64::new(0);
static ACL_EVICTIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize)]
struct AclRequest<'a> {
    filestore: &'a str,
    user: &'a AclUser,
    action: ACLAction,
    paths: AclPaths<'a>,
    context: AclContext,
}

#[derive(Debug, Clone, Serialize)]
struct AclPaths<'a> {
    #[serde(rename = "logical")]
    logical: &'a str,
    #[serde(rename = "old", skip_serializing_if = "Option::is_none")]
    old: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AclResponse {
    data: String,
    #[serde(default)]
    results: Vec<AclDecision>,
    #[serde(default)]
    error: Option<AclError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AclError {
    code: Option<String>,
    message: Option<String>,
}

/// Check ACL decisions with cache and HTTP POST per configured contract.
/// Behavior:
/// - If `security_check_enabled == false`, allow.
/// - Else try cache, else POST to ACL service. On transport/timeout, allow only if `acl_fail_open==true`.
pub async fn check_acl(
    eff: &EffectiveConfig,
    user: &AclUser,
    action: ACLAction,
    logical_path: &str,
    old_path: Option<&str>,
    ctx: &AclContext,
    filestore_name: &str,
) -> AclDecision {
    let corr = ctx
        .request_id
        .as_ref()
        .map(|s| format!(" [corr={}]", s))
        .unwrap_or_else(String::new);
    // Dev bypass
    if !eff.security_check_enabled {
        crate::tprintln!("ACL bypass enabled; allowing action {:?} on {}{}", action, logical_path, corr);
        return AclDecision::allow("security_check_disabled");
    }

    // Build cache key
    let key = CacheKey {
        filestore: filestore_name.to_string(),
        user: user.id.clone(),
        action: format!("{:?}", action).to_lowercase(),
        path: logical_path.to_string(),
    };

    // Cache lookup
    if let Some(hit) = ACL_CACHE.read().get(&key).cloned() {
        if hit.expires_at > Instant::now() {
            crate::tprintln!("ACL cache hit: {:?} -> {}{}", key.action, hit.decision.allow, corr);
            ACL_HITS.fetch_add(1, Ordering::Relaxed);
            return hit.decision;
        }
    }
    ACL_MISSES.fetch_add(1, Ordering::Relaxed);
    crate::tprintln!(
        "ACL cache miss: action={:?} user={} path={} (hits={}, misses={}, evictions={}){}",
        key.action,
        key.user,
        key.path,
        ACL_HITS.load(Ordering::Relaxed),
        ACL_MISSES.load(Ordering::Relaxed),
        ACL_EVICTIONS.load(Ordering::Relaxed),
        corr
    );

    // Evaluate authorization locally via Security v2 (RBAC/ABAC) — replaces remote HTTP path
    let v2_user = sec::model::User { id: user.id.clone(), roles: user.roles.clone(), ip: user.ip.clone() };
    let v2_action = match action {
        ACLAction::Read => sec::model::Action::Read,
        ACLAction::Write => sec::model::Action::Write,
        ACLAction::Delete => sec::model::Action::Delete,
        ACLAction::Move => sec::model::Action::Move,
        ACLAction::Copy => sec::model::Action::Copy,
        ACLAction::Rename => sec::model::Action::Rename,
        ACLAction::List => sec::model::Action::List,
        ACLAction::Commit => sec::model::Action::Commit,
        ACLAction::Push => sec::model::Action::Push,
        ACLAction::Pull => sec::model::Action::Pull,
        ACLAction::Clone => sec::model::Action::Clone,
    };
    let v2_ctx = sec::model::Context {
        filestore_config_version: ctx.filestore_config_version,
        media_type: ctx.content_meta.as_ref().and_then(|m| m.media_type.clone()),
        size_bytes: ctx.content_meta.as_ref().and_then(|m| m.size_bytes),
        git_remote: ctx.git.as_ref().and_then(|g| g.remote.clone()),
        git_branch: ctx.git.as_ref().and_then(|g| g.branch.clone()),
        request_id: ctx.request_id.clone(),
    };
    let v2_res = sec::resources::res_path("unknown_db", filestore_name, logical_path);
    let v2_dec = sec::authorize(&v2_user, v2_action, &v2_res, &v2_ctx);
    crate::tprintln!(
        "ACL local eval: user={} action={:?} path={} allow={}{}",
        user.id,
        action,
        logical_path,
        v2_dec.allow,
        corr
    );
    let mut decision = if v2_dec.allow { AclDecision::allow(v2_dec.reason.unwrap_or_else(|| "allow".into())) } else { AclDecision::deny(v2_dec.reason.unwrap_or_else(|| "deny".into())) };

    // No shadow evaluation needed; v2 is the source of truth now.

    // Determine TTL
    let ttl_ms = decision.ttl_ms.unwrap_or_else(|| if decision.allow { eff.acl_cache_ttl_allow_ms } else { eff.acl_cache_ttl_deny_ms });
    let expires_at = Instant::now() + Duration::from_millis(ttl_ms);
    // Insert with guardrails: sweep expired, enforce max size, then insert.
    {
        let mut w = ACL_CACHE.write();
        // Sweep expired
        let now = Instant::now();
        let mut expired_keys: Vec<CacheKey> = Vec::new();
        for (k, v) in w.iter() {
            if v.expires_at <= now { expired_keys.push(k.clone()); }
        }
        if !expired_keys.is_empty() {
            for k in expired_keys { w.remove(&k); }
        }
        // If over capacity, evict arbitrary entries (HashMap iteration order is fine here)
        if w.len() >= ACL_CACHE_MAX {
            // Evict up to 5% of capacity to reduce churn
            let evict_n = ((ACL_CACHE_MAX as f64) * 0.05) as usize + 1;
            let mut removed = 0usize;
            let keys_to_remove: Vec<CacheKey> = w.keys().take(evict_n).cloned().collect();
            for k in keys_to_remove { if w.remove(&k).is_some() { removed += 1; } }
            if removed > 0 {
                ACL_EVICTIONS.fetch_add(removed as u64, Ordering::Relaxed);
                crate::tprintln!("ACL cache evicted {} entries (size before >= {}, hits={}, misses={}, evictions={}){}",
                    removed, ACL_CACHE_MAX,
                    ACL_HITS.load(Ordering::Relaxed),
                    ACL_MISSES.load(Ordering::Relaxed),
                    ACL_EVICTIONS.load(Ordering::Relaxed),
                    corr);
            }
        }
        w.insert(key, CacheEntry { decision: decision.clone(), expires_at });
    }
    decision
}
