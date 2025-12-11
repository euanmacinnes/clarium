use anyhow::Result;

use super::evaluator;
use super::model::{Action, Context, ResourceId, User};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityMode {
    Local,
    ShadowRemote,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub allow: bool,
    pub reason: Option<String>,
}

pub fn authorize(user: &User, action: Action, resource: &ResourceId, ctx: &Context) -> Decision {
    evaluator::evaluate(user, action, resource, ctx)
}

pub fn explain(user: &User, action: Action, resource: &ResourceId) -> Result<String> {
    // Placeholder for detailed explain; wire to evaluator traces later.
    Ok(format!("explain: user={} action={:?} resource={}", user.id, action, resource.0))
}
