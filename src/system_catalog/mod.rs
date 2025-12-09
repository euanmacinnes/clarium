// Thin router and modules for system catalog implementations.
// New code should live here; `src/system.rs` remains a thin delegator.

pub mod registry;
pub mod pg_catalog;
pub mod information_schema;
pub mod shared;
