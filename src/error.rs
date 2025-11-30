//! Unified application error model and mapping helpers.
//! This module provides a common error enum used across frontends (HTTP, WebSocket, pgwire)
//! and exec modules, along with helper mappers to various protocols.

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppError {
    UserInput { code: String, message: String },
    NotFound { code: String, message: String },
    Conflict { code: String, message: String },
    Auth { code: String, message: String },
    Csrf { code: String, message: String },
    Ddl { code: String, message: String },
    Exec { code: String, message: String },
    Io { code: String, message: String },
    Internal { code: String, message: String },
}

impl AppError {
    pub fn code_str(&self) -> &str {
        match self {
            AppError::UserInput { code, .. }
            | AppError::NotFound { code, .. }
            | AppError::Conflict { code, .. }
            | AppError::Auth { code, .. }
            | AppError::Csrf { code, .. }
            | AppError::Ddl { code, .. }
            | AppError::Exec { code, .. }
            | AppError::Io { code, .. }
            | AppError::Internal { code, .. } => code.as_str(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            AppError::UserInput { message, .. }
            | AppError::NotFound { message, .. }
            | AppError::Conflict { message, .. }
            | AppError::Auth { message, .. }
            | AppError::Csrf { message, .. }
            | AppError::Ddl { message, .. }
            | AppError::Exec { message, .. }
            | AppError::Io { message, .. }
            | AppError::Internal { message, .. } => message.as_str(),
        }
    }

    pub fn user<S: Into<String>>(code: S, msg: S) -> Self { AppError::UserInput { code: code.into(), message: msg.into() } }
    pub fn not_found<S: Into<String>>(code: S, msg: S) -> Self { AppError::NotFound { code: code.into(), message: msg.into() } }
    pub fn conflict<S: Into<String>>(code: S, msg: S) -> Self { AppError::Conflict { code: code.into(), message: msg.into() } }
    pub fn auth<S: Into<String>>(code: S, msg: S) -> Self { AppError::Auth { code: code.into(), message: msg.into() } }
    pub fn csrf<S: Into<String>>(code: S, msg: S) -> Self { AppError::Csrf { code: code.into(), message: msg.into() } }
    pub fn ddl<S: Into<String>>(code: S, msg: S) -> Self { AppError::Ddl { code: code.into(), message: msg.into() } }
    pub fn exec<S: Into<String>>(code: S, msg: S) -> Self { AppError::Exec { code: code.into(), message: msg.into() } }
    pub fn io<S: Into<String>>(code: S, msg: S) -> Self { AppError::Io { code: code.into(), message: msg.into() } }
    pub fn internal<S: Into<String>>(code: S, msg: S) -> Self { AppError::Internal { code: code.into(), message: msg.into() } }

    /// Map to HTTP status code.
    pub fn http_status(&self) -> u16 {
        match self {
            AppError::UserInput { .. } => 400,
            AppError::NotFound { .. } => 404,
            AppError::Conflict { .. } => 409,
            AppError::Auth { .. } => 401,
            AppError::Csrf { .. } => 403,
            AppError::Ddl { .. } => 400,
            AppError::Exec { .. } => 422,
            AppError::Io { .. } => 503,
            AppError::Internal { .. } => 500,
        }
    }

    /// Pgwire mapping: return (sqlstate, severity, message)
    pub fn pgwire_fields(&self) -> (&'static str, &'static str, String) {
        let msg = self.message().to_string();
        match self {
            AppError::UserInput { .. } | AppError::Ddl { .. } => ("22000", "ERROR", msg), // data exception
            AppError::NotFound { .. } => ("42P01", "ERROR", msg),  // undefined_table
            AppError::Conflict { .. } => ("23505", "ERROR", msg),  // unique_violation (best-effort)
            AppError::Auth { .. } | AppError::Csrf { .. } => ("28000", "FATAL", msg), // invalid_authorization_specification
            AppError::Exec { .. } => ("XX000", "ERROR", msg),      // internal_error
            AppError::Io { .. } => ("08006", "FATAL", msg),        // connection_failure
            AppError::Internal { .. } => ("XX000", "ERROR", msg),  // internal_error
        }
    }
}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code_str(), self.message())
    }
}

impl std::error::Error for AppError {}

pub type AppResult<T> = Result<T, AppError>;

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        // Default mapping: treat as Exec unless downcasted elsewhere
        AppError::Exec { code: "exec_error".into(), message: err.to_string() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_status_mapping() {
        assert_eq!(AppError::user("bad_input", "oops").http_status(), 400);
        assert_eq!(AppError::not_found("not_found", "missing").http_status(), 404);
        assert_eq!(AppError::conflict("conflict", "dup").http_status(), 409);
        assert_eq!(AppError::auth("auth", "no").http_status(), 401);
        assert_eq!(AppError::csrf("csrf", "blocked").http_status(), 403);
        assert_eq!(AppError::ddl("ddl_error", "bad ddl").http_status(), 400);
        assert_eq!(AppError::exec("exec_error", "fail").http_status(), 422);
        assert_eq!(AppError::io("io", "io").http_status(), 503);
        assert_eq!(AppError::internal("internal", "panic").http_status(), 500);
    }

    #[test]
    fn pgwire_fields_mapping() {
        let (code, sev, msg) = AppError::not_found("nf", "no table").pgwire_fields();
        assert_eq!(code, "42P01");
        assert_eq!(sev, "ERROR");
        assert_eq!(msg, "no table");

        let (code, sev, _) = AppError::auth("auth", "bad").pgwire_fields();
        assert_eq!(code, "28000");
        assert_eq!(sev, "FATAL");

        let (code, sev, _) = AppError::exec("exec_error", "x").pgwire_fields();
        assert_eq!(code, "XX000");
        assert_eq!(sev, "ERROR");
    }
}
