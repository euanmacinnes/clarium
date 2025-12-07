use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

use super::config::EffectiveConfig;

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

    // No URL configured → deny unless fail-open
    let Some(url) = eff.acl_url.as_deref() else {
        if eff.acl_fail_open { return AclDecision::allow("acl_fail_open_no_url"); }
        return AclDecision::deny("acl_url_not_configured");
    };

    // Prepare request
    let req_body = AclRequest {
        filestore: filestore_name,
        user,
        action: action.clone(),
        paths: AclPaths { logical: logical_path, old: old_path },
        context: ctx.clone(),
    };

    // Execute HTTP POST
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(eff.acl_timeout_ms))
        .build();

    let client = match client {
        Ok(c) => c,
        Err(e) => {
            crate::tprintln!("ACL client build error: {}{}", e, corr);
            return if eff.acl_fail_open { AclDecision::allow("acl_fail_open_client_build") } else { AclDecision::deny("acl_client_build_error") };
        }
    };

    let mut req = client.post(url).json(&req_body);
    if let Some(h) = eff.acl_auth_header.as_deref() {
        // The configured string is the full header value, typically an Authorization header.
        req = req.header("Authorization", h);
    }

    crate::tprintln!("ACL POST start: {} action={:?} path={}{}", url, action, logical_path, corr);
    let resp = req.send().await;
    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            crate::tprintln!("ACL POST transport error: {}{}", e, corr);
            return if eff.acl_fail_open { AclDecision::allow("acl_fail_open_transport") } else { AclDecision::deny("acl_transport_error") };
        }
    };

    let status = resp.status();
    let text = resp.text().await.unwrap_or_else(|_| "".into());
    let parsed: Result<AclResponse, _> = serde_json::from_str(&text);
    if !status.is_success() || parsed.is_err() {
        crate::tprintln!("ACL POST bad status/body: status={} body={} parse_err={}{}", status, text, parsed.as_ref().err().map(|e| e.to_string()).unwrap_or_default(), corr);
        return if eff.acl_fail_open { AclDecision::allow("acl_fail_open_bad_status") } else { AclDecision::deny("acl_bad_status_or_body") };
    }
    let resp = parsed.unwrap();
    crate::tprintln!("ACL POST ok: data={}{}", resp.data, corr);

    // Map response to a decision
    let decision = if resp.data == "ok" {
        // allow if any result allows
        if resp.results.iter().any(|r| r.allow) {
            let mut d = AclDecision::allow("acl_ok");
            // Prefer the first allowing result's perms/ttl
            if let Some(first) = resp.results.into_iter().find(|r| r.allow) {
                d.effective_perms = first.effective_perms;
                d.ttl_ms = first.ttl_ms;
            }
            d
        } else {
            AclDecision::deny("acl_denied")
        }
    } else {
        AclDecision::deny("acl_error")
    };

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
