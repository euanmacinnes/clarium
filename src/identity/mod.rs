//! Central identity and session management for unified login across Clarium.
//! Keep the public surface thin and split implementation across sub-modules.

mod principal;
mod session;
mod provider;
mod adapters;
mod request_context;
mod authorizer;

pub use principal::{Principal, Attrs};
pub use session::{Session, SessionToken, SessionManager};
pub use provider::{AuthProvider, LocalAuthProvider, LoginRequest, LoginResponse};
pub use adapters::{to_filestore_legacy_user, to_filestore_v2_user};
pub use request_context::RequestContext;
pub use authorizer::{Role, check_command_allowed};
