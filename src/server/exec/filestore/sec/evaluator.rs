//! In-process authorization evaluator (storage-backed RBAC with deny precedence).
//! Keep logic small and fast; avoid large match statements by delegating to helpers.

use std::collections::HashMap;
use parking_lot::RwLock;
use once_cell::sync::Lazy;
use regex::Regex;

use super::model::{Action, Context, ResourceId, User};
use super::api::Decision;

// Global store for loading roles/policies on demand (set once by host)
static STORE: Lazy<RwLock<Option<crate::storage::SharedStore>>> = Lazy::new(|| RwLock::new(None));

// Cache compiled policies per role with a simple global epoch tag
#[derive(Clone)]
struct CompiledPolicy {
    actions: Vec<String>, // lowercase; "*" matches all
    res_regex: Regex,
    allow: bool,          // true=allow, false=deny
    priority: i32,
}

static ROLE_CACHE: Lazy<RwLock<HashMap<String, (u64, Vec<CompiledPolicy>)>>> = Lazy::new(|| RwLock::new(HashMap::new()));

fn allow_all() -> Decision { Decision { allow: true, reason: Some("role_admin".into()) } }
fn deny(reason: &str) -> Decision { Decision { allow: false, reason: Some(reason.into()) } }

fn has_role(user: &User, role: &str) -> bool { user.roles.iter().any(|r| r.eq_ignore_ascii_case(role)) }

fn action_str(a: Action) -> &'static str {
    match a {
        Action::Read => "read",
        Action::Write => "write",
        Action::Delete => "delete",
        Action::Move => "move",
        Action::Copy => "copy",
        Action::Rename => "rename",
        Action::List => "list",
        Action::Commit => "commit",
        Action::Push => "push",
        Action::Pull => "pull",
        Action::Clone => "clone",
    }
}

fn glob_to_regex(pattern: &str) -> Regex {
    // Convert simple glob (with * and **) to a Rust regex anchored at both ends
    // Escape regex meta, then restore wildcards: ** -> .*, * -> [^/]*
    let mut s = regex::escape(pattern);
    // Replace escaped globs
    s = s.replace("\\*\\*", ".*");
    s = s.replace("\\*", "[^/]*");
    let full = format!("^{}$", s);
    Regex::new(&full).unwrap_or_else(|_| Regex::new("^$" ).unwrap())
}

fn compile_policy(p: &crate::server::exec::filestore::sec::storage::policies::PolicyRecord) -> CompiledPolicy {
    let rx = glob_to_regex(&p.resource_selector);
    let allow = p.effect == "allow";
    CompiledPolicy { actions: p.actions.clone(), res_regex: rx, allow, priority: p.priority }
}

fn fetch_policies_for_roles_sync(store: &crate::storage::SharedStore, role_ids: &[String]) -> Vec<CompiledPolicy> {
    // Attempt to call async storage using the current Tokio runtime; if unavailable, return empty
    match tokio::runtime::Handle::try_current() {
        Ok(h) => {
            if let Ok(policies) = h.block_on(crate::server::exec::filestore::sec::storage::policies::list_policies_for_roles(store, role_ids)) {
                return policies.into_iter().map(|p| compile_policy(&p)).collect();
            }
        }
        Err(_) => { /* no runtime; fall through */ }
    }
    Vec::new()
}

fn load_policies_for_roles(role_ids: &[String]) -> Vec<CompiledPolicy> {
    if role_ids.is_empty() { return Vec::new(); }
    // Check cache per role; refetch when epoch changes
    let epoch = crate::server::exec::filestore::sec::epochs::epoch_global();
    let store_opt = STORE.read().clone();
    let Some(store) = store_opt else { return Vec::new(); };

    let mut out: Vec<CompiledPolicy> = Vec::new();
    let mut miss_roles: Vec<String> = Vec::new();
    {
        let cache = ROLE_CACHE.read();
        for r in role_ids {
            if let Some((ep, pols)) = cache.get(r) {
                if *ep == epoch { out.extend_from_slice(pols); continue; }
            }
            miss_roles.push(r.clone());
        }
    }
    if !miss_roles.is_empty() {
        let policies = fetch_policies_for_roles_sync(&store, &miss_roles);
        let mut grouped: HashMap<String, Vec<CompiledPolicy>> = HashMap::new();
        for p in policies.into_iter() {
            // We lost role_id in compile step; fetch raw again to preserve
        }
        // Fetch raw to preserve role mapping
        match tokio::runtime::Handle::try_current() {
            Ok(h) => {
                if let Ok(raw) = h.block_on(crate::server::exec::filestore::sec::storage::policies::list_policies_for_roles(&store, &miss_roles)) {
                    let mut grouped: HashMap<String, Vec<CompiledPolicy>> = HashMap::new();
                    for rp in raw.iter() {
                        grouped.entry(rp.role_id.clone()).or_default().push(compile_policy(rp));
                    }
                    let mut w = ROLE_CACHE.write();
                    for (role, pols) in grouped.into_iter() {
                        w.insert(role.clone(), (epoch, pols.clone()));
                    }
                }
            }
            Err(_) => { /* no runtime; skip cache fill */ }
        }
    }
    // Collect final list
    let cache = ROLE_CACHE.read();
    for r in role_ids {
        if let Some((_ep, pols)) = cache.get(r) { out.extend_from_slice(pols); }
    }
    // Sort by priority (desc) to make evaluation consistent
    out.sort_by(|a, b| b.priority.cmp(&a.priority));
    out
}

/// Host can set the global store to enable storage-backed RBAC
pub fn set_store(store: &crate::storage::SharedStore) {
    *STORE.write() = Some(store.clone());
}

// ----- L1 per-thread decision cache (tiny) -----
thread_local! {
    static L1_CACHE: std::cell::RefCell<(u64, HashMap<String, Decision>)> = std::cell::RefCell::new((0, HashMap::new()));
}

#[inline]
fn l1_key(user: &User, action: Action, res: &ResourceId) -> String {
    format!("{}|{}|{}", user.id, action_str(action), res.0)
}

fn l1_get(epoch: u64, key: &str) -> Option<Decision> {
    L1_CACHE.with(|cell| {
        let (ep, map) = &*cell.borrow();
        if *ep == epoch { map.get(key).cloned() } else { None }
    })
}

fn l1_put(epoch: u64, key: String, value: Decision) {
    L1_CACHE.with(|cell| {
        let mut pair = cell.borrow_mut();
        let (ref mut ep, ref mut map) = *pair;
        if *ep != epoch { map.clear(); *ep = epoch; }
        // Cap size to 512 to keep it tiny
        if map.len() >= 512 { map.clear(); }
        map.insert(key, value);
    });
}

pub fn evaluate(user: &User, action: Action, res: &ResourceId, _ctx: &Context) -> Decision {
    let epoch = crate::server::exec::filestore::sec::epochs::epoch_global();
    let key = l1_key(user, action, res);
    if let Some(hit) = l1_get(epoch, &key) { return hit; }
    // Admin fast-path
    if has_role(user, "admin") { return allow_all(); }

    // Resolve roles: explicit roles from principal/user
    let mut roles: Vec<String> = user.roles.clone();

    // Optionally augment with dynamic memberships from storage
    if let Some(store) = STORE.read().clone() {
        // Try to fetch dynamic roles using current Tokio runtime; ignore on failure
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if let Ok(mut dyn_roles) = handle.block_on(crate::server::exec::filestore::sec::storage::role_memberships::list_roles_for_user(&store, &user.id)) {
                roles.append(&mut dyn_roles);
            }
        }
    }

    // Load compiled policies for these roles
    let compiled = load_policies_for_roles(&roles);
    let a = action_str(action);
    let r = &res.0;

    // Deny precedence: any deny match blocks immediately
    for p in compiled.iter() {
        let act_match = p.actions.iter().any(|x| x == "*" || x == a);
        if act_match && p.res_regex.is_match(r) {
            if !p.allow { return deny("policy_deny"); }
        }
    }
    // Allow if any allow policy matches
    for p in compiled.iter() {
        let act_match = p.actions.iter().any(|x| x == "*" || x == a);
        if act_match && p.res_regex.is_match(r) {
            if p.allow { return Decision { allow: true, reason: Some("policy_allow".into()) }; }
        }
    }

    // Fallback: minimal role-based gates to keep behavior sensible if no policies loaded
    let out = match action {
        Action::Read | Action::List | Action::Pull | Action::Clone => {
            if has_role(user, "db_reader") || has_role(user, "fs_reader") { Decision { allow: true, reason: Some("role_reader".into()) } } else { deny("no_read_policy") }
        }
        _ => {
            if has_role(user, "db_writer") || has_role(user, "fs_writer") { Decision { allow: true, reason: Some("role_writer".into()) } } else { deny("no_write_policy") }
        }
    };
    l1_put(epoch, key, out.clone());
    out
}
