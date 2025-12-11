//! Hook traits and registry. Keep non-blocking surfaces thin and small.

use super::api::Decision;
use super::model::{Action, Context, ResourceId, User};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::io::Write;

#[derive(Debug, Clone)]
pub struct HookEvent {
    pub user: User,
    pub action: Action,
    pub resource: ResourceId,
    pub ctx: Context,
    pub decision: Option<Decision>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookOutcome { Continue, Veto }

pub trait PreAuthHook: Send + Sync {
    fn on_pre_auth(&self, _ev: &HookEvent) -> HookOutcome { HookOutcome::Continue }
}
pub trait PostAuthHook: Send + Sync {
    fn on_post_auth(&self, _ev: &HookEvent) {}
}
pub trait PreMutationHook: Send + Sync {
    fn on_pre_mutation(&self, _ev: &HookEvent) -> HookOutcome { HookOutcome::Continue }
}
pub trait PostMutationHook: Send + Sync {
    fn on_post_mutation(&self, _ev: &HookEvent) {}
}
pub trait PostReadHook: Send + Sync {
    fn on_post_read(&self, _ev: &HookEvent) {}
}
pub trait PostListHook: Send + Sync {
    fn on_post_list(&self, _ev: &HookEvent) {}
}

pub struct HookRegistry {
    pub pre_auth: Vec<Box<dyn PreAuthHook>>,    
    pub post_auth: Vec<Box<dyn PostAuthHook>>,  
    pub pre_mut: Vec<Box<dyn PreMutationHook>>, 
    pub post_mut: Vec<Box<dyn PostMutationHook>>,
    pub post_read: Vec<Box<dyn PostReadHook>>,  
    pub post_list: Vec<Box<dyn PostListHook>>,  
}

impl Default for HookRegistry {
    fn default() -> Self {
        Self { pre_auth: vec![], post_auth: vec![], pre_mut: vec![], post_mut: vec![], post_read: vec![], post_list: vec![] }
    }
}

// Global registry (process-local)
static REG: Lazy<RwLock<HookRegistry>> = Lazy::new(|| RwLock::new(HookRegistry::default()));

pub fn register_post_auth(h: Box<dyn PostAuthHook>) {
    REG.write().post_auth.push(h);
}

pub fn emit_post_auth(ev: &HookEvent) {
    for h in REG.read().post_auth.iter() {
        // Best-effort; hooks must not panic (hook implementors should handle errors internally)
        h.on_post_auth(ev);
    }
}

// --- Simple file logger sink for audit events ---

struct FileLogger { path: String }

impl FileLogger {
    fn new(path: &str) -> Self { Self { path: path.to_string() } }
}

impl PostAuthHook for FileLogger {
    fn on_post_auth(&self, ev: &HookEvent) {
        // Write a compact JSON line; ignore errors
        let ts = chrono::Utc::now().timestamp_millis();
        let obj = serde_json::json!({
            "ts": ts,
            "user": ev.user.id,
            "roles": ev.user.roles,
            "action": format!("{:?}", ev.action).to_lowercase(),
            "resource": ev.resource.0,
            "allow": ev.decision.as_ref().map(|d| d.allow).unwrap_or(false),
            "reason": ev.decision.as_ref().and_then(|d| d.reason.clone()),
            "request_id": ev.ctx.request_id,
        });
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&self.path) {
            let _ = writeln!(&mut f, "{}", obj.to_string());
        }
    }
}

/// Convenience: register a file logger sink to capture post-auth audit events.
pub fn register_file_logger(path: &str) {
    register_post_auth(Box::new(FileLogger::new(path)));
}
