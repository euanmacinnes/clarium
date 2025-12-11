//! Installation routines for starting data (initial seed implementation).

use anyhow::Result;
use crate::tprintln;

use crate::server::exec::filestore::sec::rbac::{Policy, PolicyId, Role, RoleId, Effect};

/// Install minimal starting roles and policies in memory.
/// NOTE: This is a placeholder; wire to real storage DDLs per table in follow-ups.
pub fn install_starting_data() -> Result<()> {
    // Seed roles
    let roles = vec![
        Role { id: RoleId("admin".into()), description: Some("Administrator".into()) },
        Role { id: RoleId("db_reader".into()), description: Some("Database reader".into()) },
        Role { id: RoleId("db_writer".into()), description: Some("Database writer".into()) },
        Role { id: RoleId("fs_reader".into()), description: Some("Filestore reader".into()) },
        Role { id: RoleId("fs_writer".into()), description: Some("Filestore writer".into()) },
    ];
    // Seed coarse policies (expressed as resource selector strings for now)
    let policies = vec![
        Policy { id: PolicyId("pol_admin_all".into()), role: RoleId("admin".into()), actions: vec!["*".into()], resource_selector: "res://**".into(), predicate_json: None, effect: Effect::Allow, priority: 1000 },
        Policy { id: PolicyId("pol_db_read".into()), role: RoleId("db_reader".into()), actions: vec!["read".into(), "list".into()], resource_selector: "res://*/table/**".into(), predicate_json: None, effect: Effect::Allow, priority: 100 },
        Policy { id: PolicyId("pol_db_write".into()), role: RoleId("db_writer".into()), actions: vec!["write".into(), "rename".into(), "delete".into()], resource_selector: "res://*/table/**".into(), predicate_json: None, effect: Effect::Allow, priority: 100 },
        Policy { id: PolicyId("pol_fs_read".into()), role: RoleId("fs_reader".into()), actions: vec!["read".into(), "list".into(), "pull".into(), "clone".into()], resource_selector: "res://*/**/path/**".into(), predicate_json: None, effect: Effect::Allow, priority: 100 },
        Policy { id: PolicyId("pol_fs_write".into()), role: RoleId("fs_writer".into()), actions: vec!["write".into(), "rename".into(), "delete".into(), "commit".into(), "push".into()], resource_selector: "res://*/**/path/**".into(), predicate_json: None, effect: Effect::Allow, priority: 100 },
    ];
    tprintln!("security.install_starting_data roles={} policies={}", roles.len(), policies.len());
    Ok(())
}
