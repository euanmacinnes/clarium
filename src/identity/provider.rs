use anyhow::{Result, anyhow};
// Keep provider request/response plain Rust structs to avoid serde requirements on Session
use crate::tprintln;

use super::principal::Principal;
use super::session::{Session, SessionManager};

#[derive(Debug, Clone)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
    pub db: Option<String>,
    pub ip: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LoginResponse {
    pub session: Session,
}

pub trait AuthProvider: Send + Sync {
    fn login(&self, req: &LoginRequest) -> Result<LoginResponse>;
}

pub struct LocalAuthProvider {
    pub db_root: String,
    pub sm: SessionManager,
}

impl LocalAuthProvider {
    pub fn new(db_root: String, sm: SessionManager) -> Self { Self { db_root, sm } }
}

impl AuthProvider for LocalAuthProvider {
    fn login(&self, req: &LoginRequest) -> Result<LoginResponse> {
        // Verify password using existing user store (global scope for now)
        if !crate::security::authenticate(&self.db_root, &req.username, &req.password)? {
            return Err(anyhow!("invalid_credentials"));
        }
        // Map permissions to roles using existing authorizer heuristics
        let mut roles: Vec<String> = vec!["user".into()];
        let is_admin = crate::security::authorize(&self.db_root, &req.username, crate::security::CommandKind::Schema, None).unwrap_or(false);
        if is_admin { roles.push("admin".into()); }
        // Database-scoped roles inferred from command authorizations
        if crate::security::authorize(&self.db_root, &req.username, crate::security::CommandKind::Select, req.db.as_deref()).unwrap_or(false) {
            roles.push("db_reader".into());
        }
        if crate::security::authorize(&self.db_root, &req.username, crate::security::CommandKind::Insert, req.db.as_deref()).unwrap_or(false) {
            roles.push("db_writer".into());
        }
        if crate::security::authorize(&self.db_root, &req.username, crate::security::CommandKind::Calculate, req.db.as_deref()).unwrap_or(false) {
            roles.push("compute".into());
        }
        if crate::security::authorize(&self.db_root, &req.username, crate::security::CommandKind::DeleteRows, req.db.as_deref()).unwrap_or(false) {
            roles.push("db_deleter".into());
        }

        // Principal with basic attributes
        let principal = super::principal::Principal {
            user_id: req.username.clone(),
            roles,
            attrs: super::principal::Attrs { ip: req.ip.clone(), ..Default::default() },
        };
        let session = self.sm.issue(principal);
        tprintln!("auth.login user={} sid={}", req.username, session.session_id);
        Ok(LoginResponse { session })
    }
}
