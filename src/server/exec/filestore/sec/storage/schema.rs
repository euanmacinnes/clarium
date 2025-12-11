//! Schema definitions for security storage (scaffold).

#[derive(Debug, Clone, Copy)]
pub enum TableKind {
    Users,
    Roles,
    RoleMemberships,
    Policies,
    Resources,
    FsOverrides,
    Publications,
    PubGraph,
    Epochs,
}

pub fn all_tables() -> &'static [TableKind] {
    use TableKind::*;
    &[Users, Roles, RoleMemberships, Policies, Resources, FsOverrides, Publications, PubGraph, Epochs]
}
