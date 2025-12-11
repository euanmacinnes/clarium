//! Filestore Security v2 — local RBAC/ABAC evaluator, hooks, and storage scaffolding.
//!
//! This module is a new, modular security stack for FILESTORE. It does not
//! replace the legacy `security.rs` yet; integration is incremental. Keep
//! each concern in a small sub‑module to avoid large files and match arms.

pub mod api;
pub mod model;
pub mod rbac;
pub mod abac;
pub mod evaluator;
pub mod hooks;
pub mod resources;
pub mod published;
pub mod epochs;
pub mod storage;

// Re‑exports for thin public surface
pub use api::{authorize, explain, SecurityMode, Decision};
pub use model::{Action, User, ResourceId, Context};
pub use hooks::{HookRegistry, HookEvent, HookOutcome};
