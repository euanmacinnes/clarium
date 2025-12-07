use super::*;
use crate::server::exec::filestore::*;

#[tokio::test]
async fn test_acl_bypass_allows() {
    let global = GlobalFilestoreConfig::default();
    let mut fs = FilestoreConfig::default();
    fs.security_check_enabled = false;
    let eff = EffectiveConfig::from_layers(&global, &fs, None);
    let user = AclUser { id: "u1".into(), roles: vec![], ip: None };
    let ctx = AclContext::default();
    let dec = check_acl(&eff, &user, ACLAction::Read, "a/b", None, &ctx, "fs1").await;
    assert!(dec.allow);
}
