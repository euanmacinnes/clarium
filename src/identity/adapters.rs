use super::principal::Principal;

// Legacy filestore ACL user
pub fn to_filestore_legacy_user(p: &Principal) -> crate::server::exec::filestore::security::AclUser {
    crate::server::exec::filestore::security::AclUser {
        id: p.user_id.clone(),
        roles: p.roles.clone(),
        ip: p.attrs.ip.clone(),
    }
}

// New v2 filestore user
pub fn to_filestore_v2_user(p: &Principal) -> crate::server::exec::filestore::sec::model::User {
    crate::server::exec::filestore::sec::model::User {
        id: p.user_id.clone(),
        roles: p.roles.clone(),
        ip: p.attrs.ip.clone(),
    }
}
