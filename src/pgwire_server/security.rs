use anyhow::{anyhow, Result, bail};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, debug, warn};
use crate::pgwire_server::{misc::*, write_parameter, send::send_ready_with_status};
use crate::tprintln;

use crate::{storage::SharedStore, server::exec};
use crate::server::query::{self, Command};
use crate::server::exec::exec_select::handle_select;
use polars::prelude::{AnyValue, DataFrame, DataType, TimeUnit};
use crate::ident::{DEFAULT_DB, DEFAULT_SCHEMA};
use regex::Regex;
use std::collections::HashMap;

pub async fn send_auth_ok_and_params(socket: &mut tokio::net::TcpStream, startup_params: &std::collections::HashMap<String, String>) -> Result<()> {
    // AuthenticationOk
    write_msg_header(socket, b'R', 8).await?; // len = 8
    write_i32(socket, 0).await?; // AuthenticationOk
    // Commonly expected ParameterStatus fields for libpq/psycopg compatibility
    write_parameter(socket, "server_version", "14.0").await?;
    write_parameter(socket, "server_version_num", "140000").await?;
    write_parameter(socket, "server_encoding", "UTF8").await?;
    write_parameter(socket, "client_encoding", "UTF8").await?;
    write_parameter(socket, "DateStyle", "ISO, MDY").await?;
    write_parameter(socket, "integer_datetimes", "on").await?;
    write_parameter(socket, "standard_conforming_strings", "on").await?;
    write_parameter(socket, "TimeZone", "UTC").await?;
    write_parameter(socket, "default_transaction_read_only", "off").await?;
    write_parameter(socket, "is_superuser", "off").await?;
    let sp = format!("\"$user\", {}", DEFAULT_SCHEMA);
    write_parameter(socket, "search_path", &sp).await?;
    // session_authorization and application_name from startup
    if let Some(user) = startup_params.get("user") {
        write_parameter(socket, "session_authorization", user).await?;
        debug!(target: "pgwire", "sent ParameterStatus session_authorization='{}'", user);
    }
    // Echo back application_name if provided by client
    if let Some(app_name) = startup_params.get("application_name") {
        write_parameter(socket, "application_name", app_name).await?;
        debug!(target: "pgwire", "sent ParameterStatus application_name='{}'", app_name);
    }
    // BackendKeyData (K) - process ID and secret key for cancellation requests
    // According to common server behavior, send this after ParameterStatus
    socket.write_all(b"K").await?;
    write_i32(socket, 12).await?; // length (4 + 4 + 4)
    write_i32(socket, std::process::id() as i32).await?; // process ID
    write_i32(socket, 12345).await?; // secret key (dummy value)
    debug!(target: "pgwire", "sent BackendKeyData (pid={}, secret=12345)", std::process::id());
    // ReadyForQuery (always idle right after startup)
    send_ready_with_status(socket, b'I').await
}

pub async fn request_password(socket: &mut tokio::net::TcpStream) -> Result<()> {
    // AuthenticationCleartextPassword (code 3)
    write_msg_header(socket, b'R', 8).await?;
    write_i32(socket, 3).await?;
    Ok(())
}

pub async fn read_password_message(socket: &mut tokio::net::TcpStream) -> Result<String> {
    let mut tag = [0u8;1];
    socket.read_exact(&mut tag).await?;
    if tag[0] != b'p' { return Err(anyhow!("Expected PasswordMessage")); }
    let len = read_u32(socket).await? as usize;
    let mut buf = vec![0u8; len - 4];
    socket.read_exact(&mut buf).await?;
    // Trim trailing null if present
    if let Some(&0) = buf.last() { buf.pop(); }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}