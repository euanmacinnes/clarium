//! Hook traits and registry (scaffold). Keep non-blocking surfaces thin.

use super::api::Decision;
use super::model::{Action, Context, ResourceId, User};

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
