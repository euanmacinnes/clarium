//! RBAC structures and helpers (stubbed for initial scaffolding).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RoleId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Role {
    pub id: RoleId,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Effect { Allow, Deny }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Policy {
    pub id: PolicyId,
    pub role: RoleId,
    pub actions: Vec<String>,
    pub resource_selector: String,
    #[serde(default)]
    pub predicate_json: Option<String>,
    pub effect: Effect,
    #[serde(default)]
    pub priority: i32,
}
