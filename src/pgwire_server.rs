//! Experimental pgwire server integration (feature-gated).
//! Minimal PostgreSQL wire-protocol handler supporting:
//! - Startup (no auth), simple query flow
//! - SELECT: delegates to existing query engine and streams rows
//! - INSERT: basic INSERT INTO <db>(col, ...) VALUES (...)

use anyhow::{anyhow, Result, bail};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, debug, warn};
use crate::tprintln;

use crate::{storage::SharedStore, server::exec};
use crate::server::query::{self, Command};
use crate::server::exec::exec_select::handle_select;
use polars::prelude::{AnyValue, DataFrame, DataType, TimeUnit, StringEncoding};
use crate::ident::{DEFAULT_DB, DEFAULT_SCHEMA};
use regex::Regex;

const PG_TYPE_TEXT: i32 = 25; // use text for all columns for simplicity

static CONN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn pgwire_trace_enabled() -> bool {
    std::env::var("CLARIUM_PGWIRE_TRACE").map(|v| {
        let s = v.to_lowercase();
        s == "1" || s == "true" || s == "yes" || s == "on"
    }).unwrap_or(false)
}

#[inline]
fn anyvalue_to_opt_string(av: &AnyValue) -> Option<String> {
    match av {
        AnyValue::Null => None,
        AnyValue::String(s) => Some(s.to_string()),
        AnyValue::StringOwned(s) => Some(s.to_string()),
        AnyValue::Int8(v) => Some(v.to_string()),
        AnyValue::Int16(v) => Some(v.to_string()),
        AnyValue::Int32(v) => Some(v.to_string()),
        AnyValue::Int64(v) => Some(v.to_string()),
        AnyValue::UInt8(v) => Some(v.to_string()),
        AnyValue::UInt16(v) => Some(v.to_string()),
        AnyValue::UInt32(v) => Some(v.to_string()),
        AnyValue::UInt64(v) => Some(v.to_string()),
        AnyValue::Float32(v) => Some(v.to_string()),
        AnyValue::Float64(v) => Some(v.to_string()),
        AnyValue::Boolean(v) => Some(v.to_string()),
        other => Some(format!("{}", other)),
    }
}

fn hex_dump_prefix(data: &[u8], max: usize) -> String {
    let take = data.len().min(max);
    data.iter().take(take).map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ")
}

pub async fn start_pgwire(store: SharedStore, bind: &str) -> Result<()> {
    let addr: SocketAddr = bind.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("pgwire listening on {}", addr);
    loop {
        let (mut socket, peer) = listener.accept().await?;
        let store = store.clone();
        let conn_id = CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            if let Err(e) = handle_conn(&mut socket, store, conn_id, &peer.to_string()).await {
                error!(target: "pgwire", "conn_id={} peer={} error: {}", conn_id, peer, e);
                let _ = socket.shutdown();
            }
        });
    }
}

use std::collections::HashMap;

#[derive(Clone)]
struct PreparedStatement {
    name: String,
    sql: String,
    param_types: Vec<i32>,
}

#[derive(Clone)]
struct Portal {
    name: String,
    stmt_name: String,
    // Store raw text parameters (None for NULL)
    params: Vec<Option<String>>,
    param_formats: Vec<i16>,
    result_formats: Vec<i16>,
}

struct ConnState {
    current_database: String,
    current_schema: String,
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    // if an error occurred in extended flow, we keep going until Sync
    in_error: bool,
    // inside explicit transaction block (BEGIN..)
    in_tx: bool,
}

async fn handle_conn(socket: &mut tokio::net::TcpStream, store: SharedStore, conn_id: u64, peer: &str) -> Result<()> {
    tprintln!("[pgwire] conn_id={} new connection established from {}", conn_id, peer);
    #[inline]
    fn env_default_db() -> String {
        std::env::var("CLARIUM_DEFAULT_DB").unwrap_or_else(|_| DEFAULT_DB.to_string())
    }
    #[inline]
    fn env_default_schema() -> String {
        std::env::var("CLARIUM_DEFAULT_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string())
    }
    // Trust mode for dev/test: when enabled via env, skip password auth entirely
    fn pgwire_trust_enabled() -> bool {
        std::env::var("CLARIUM_PGWIRE_TRUST").map(|v| {
            let s = v.to_lowercase();
            s == "1" || s == "true" || s == "yes" || s == "on"
        }).unwrap_or(false)
    }
    // Startup packet
    let len = read_u32(socket).await?;
    let mut buf = vec![0u8; (len - 4) as usize];
    socket.read_exact(&mut buf).await?;
    if pgwire_trace_enabled() {
        tprintln!("[pgwire] conn_id={} startup packet len={}, first={} bytes: {}", conn_id, len, buf.len().min(32), hex_dump_prefix(&buf, 32));
    } else {
        tprintln!("[pgwire] conn_id={} received startup packet, len={}", conn_id, len);
    }
    // Check for SSLRequest (0x04D2162F) or GSSENC (0x04D2162A)
    if buf.len() == 4 {
        let code = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        // Respond 'N' to refuse SSL/GSS, then expect new StartupMessage
        if code == 80877103 || code == 80877104 {
            debug!(target: "pgwire", "conn_id={} SSL/GSSENC request detected (code={}), refusing with 'N'", conn_id, code);
            socket.write_all(b"N").await?;
            // Read actual startup
            let len2 = read_u32(socket).await?;
            let mut buf2 = vec![0u8; (len2 - 4) as usize];
            socket.read_exact(&mut buf2).await?;
            let params = parse_startup_params(&buf2);
            let user = params.get("user").cloned().unwrap_or_else(|| "".to_string());
            debug!(target: "pgwire", "conn_id={} startup params parsed, user='{}' (keys={:?})", conn_id, user, params.keys().collect::<Vec<_>>() );
            // Request cleartext password
            if !pgwire_trust_enabled() {
                request_password(socket).await?;
                let password = read_password_message(socket).await?;
                debug!(target: "pgwire", "conn_id={} password received, authenticating user '{}'", conn_id, user);
                let db_root = store.root_path();
                let ok = crate::security::authenticate(db_root.to_string_lossy().as_ref(), &user, &password)?;
                if !ok { 
                    debug!(target: "pgwire", "conn_id={} authentication failed for user '{}'", conn_id, user);
                    send_error(socket, "authentication failed").await?; 
                    return Ok(()); 
                }
                debug!(target: "pgwire", "conn_id={} authentication successful for user '{}'", conn_id, user);
            } else {
                debug!(target: "pgwire", "conn_id={} TRUST mode enabled via CLARIUM_PGWIRE_TRUST; skipping password auth for user '{}'", conn_id, user);
            }
            send_auth_ok_and_params(socket, &params).await?;
            // Initialize session state honoring dbname/database if provided
            let db = params.get("database").cloned()
                .or_else(|| params.get("dbname").cloned())
                .unwrap_or_else(|| env_default_db());
            let mut state = ConnState { current_database: db, current_schema: env_default_schema(), statements: HashMap::new(), portals: HashMap::new(), in_error: false, in_tx: false };
            run_query_loop(socket, &store, &user, &mut state, conn_id).await?;
            Ok(())
        } else {
            // Unknown 4-byte request; continue without auth (shouldn't happen)
            send_error(socket, "unsupported startup request").await?;
            Ok(())
        }
    } else {
        // Normal parameter list present
        let params = parse_startup_params(&buf);
        let user = params.get("user").cloned().unwrap_or_else(|| "".to_string());
        debug!(target: "pgwire", "conn_id={} normal startup (no SSL), user='{}' (keys={:?})", conn_id, user, params.keys().collect::<Vec<_>>() );
        if !pgwire_trust_enabled() {
            request_password(socket).await?;
            let password = read_password_message(socket).await?;
            debug!(target: "pgwire", "conn_id={} password received, authenticating user '{}'", conn_id, user);
            let db_root = store.root_path();
            let ok = crate::security::authenticate(db_root.to_string_lossy().as_ref(), &user, &password)?;
            if !ok { 
                debug!(target: "pgwire", "conn_id={} authentication failed for user '{}'", conn_id, user);
                send_error(socket, "authentication failed").await?; 
                return Ok(()); 
            }
            debug!(target: "pgwire", "conn_id={} authentication successful for user '{}'", conn_id, user);
        } else {
            debug!(target: "pgwire", "conn_id={} TRUST mode enabled via CLARIUM_PGWIRE_TRUST; skipping password auth for user '{}'", conn_id, user);
        }
        send_auth_ok_and_params(socket, &params).await?;
        // Initialize session state honoring dbname/database if provided
        let db = params.get("database").cloned()
            .or_else(|| params.get("dbname").cloned())
            .unwrap_or_else(|| env_default_db());
        let mut state = ConnState { current_database: db, current_schema: env_default_schema(), statements: HashMap::new(), portals: HashMap::new(), in_error: false, in_tx: false };
        run_query_loop(socket, &store, &user, &mut state, conn_id).await?;
        Ok(())
    }
}

async fn send_auth_ok_and_params(socket: &mut tokio::net::TcpStream, startup_params: &std::collections::HashMap<String, String>) -> Result<()> {
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

fn parse_startup_params(payload: &[u8]) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;
    let mut m = HashMap::new();
    // The StartupMessage payload begins with a 4-byte protocol version, followed by
    // a sequence of null-terminated key/value C-strings and a final terminating 0.
    if payload.len() < 4 { return m; }
    let kv = &payload[4..];
    let mut parts: Vec<String> = Vec::new();
    let mut cur = Vec::new();
    for &b in kv.iter() {
        if b == 0 { parts.push(String::from_utf8_lossy(&cur).into_owned()); cur.clear(); }
        else { cur.push(b); }
    }
    let mut i = 0;
    while i + 1 < parts.len() {
        let k = parts[i].clone();
        let v = parts[i+1].clone();
        if !k.is_empty() { m.insert(k, v); }
        i += 2;
    }
    m
}

async fn request_password(socket: &mut tokio::net::TcpStream) -> Result<()> {
    // AuthenticationCleartextPassword (code 3)
    write_msg_header(socket, b'R', 8).await?;
    write_i32(socket, 3).await?;
    Ok(())
}

async fn read_password_message(socket: &mut tokio::net::TcpStream) -> Result<String> {
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

async fn read_i16(socket: &mut tokio::net::TcpStream) -> Result<i16> { let mut b = [0u8;2]; socket.read_exact(&mut b).await?; Ok(i16::from_be_bytes(b)) }
async fn read_i32(socket: &mut tokio::net::TcpStream) -> Result<i32> { let mut b = [0u8;4]; socket.read_exact(&mut b).await?; Ok(i32::from_be_bytes(b)) }
async fn read_cstring(socket: &mut tokio::net::TcpStream) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut byte = [0u8;1];
    loop {
        socket.read_exact(&mut byte).await?;
        if byte[0] == 0 { break; }
        buf.push(byte[0]);
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

async fn send_parse_complete(socket: &mut tokio::net::TcpStream) -> Result<()> { 
    debug!("pgwire: sending ParseComplete");
    socket.write_all(b"1").await?; 
    write_i32(socket, 4).await 
}
async fn send_bind_complete(socket: &mut tokio::net::TcpStream) -> Result<()> { 
    debug!("pgwire: sending BindComplete");
    socket.write_all(b"2").await?; 
    write_i32(socket, 4).await 
}
async fn send_close_complete(socket: &mut tokio::net::TcpStream) -> Result<()> { socket.write_all(b"3").await?; write_i32(socket, 4).await }
async fn send_no_data(socket: &mut tokio::net::TcpStream) -> Result<()> {
    debug!(target: "pgwire", "sending NoData (len=4)");
    socket.write_all(b"n").await?; write_i32(socket, 4).await
}
async fn send_parameter_description(socket: &mut tokio::net::TcpStream, param_types: &[i32]) -> Result<()> {
    debug!(target: "pgwire", "sending ParameterDescription ({} params)", param_types.len());
    socket.write_all(b"t").await?;
    let mut payload = Vec::new();
    let n = param_types.len() as i16;
    payload.extend_from_slice(&n.to_be_bytes());
    for oid in param_types { payload.extend_from_slice(&oid.to_be_bytes()); }
    let total_len = (payload.len() as i32) + 4;
    debug!(target: "pgwire", "ParameterDescription payload_len={} total_frame_len={}", payload.len(), total_len);
    write_i32(socket, total_len).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

async fn run_query_loop(socket: &mut tokio::net::TcpStream, store: &SharedStore, user: &str, state: &mut ConnState, conn_id: u64) -> Result<()> {
    tprintln!("[pgwire] conn_id={} entering query loop for user '{}' (db='{}', schema='{}')", conn_id, user, state.current_database, state.current_schema);
    // Accumulate a simple cycle summary between Sync boundaries to quickly verify message order.
    // Emitted when Sync -> ReadyForQuery completes.
    let mut cycle_summary = String::new();
    // Track last handled message and last error text for exit snapshot
    let mut last_msg: Option<u8> = None;
    let mut last_err: Option<String> = None;
    loop {
        let mut tag = [0u8; 1];
        match socket.read_exact(&mut tag).await {
            Ok(_) => {}
            Err(e) => {
                if !cycle_summary.is_empty() {
                    tprintln!("[pgwire] conn_id={} cycle(partial): {}", conn_id, cycle_summary.trim());
                    cycle_summary.clear();
                }
                warn!(target: "pgwire", "conn_id={} read_exact(tag) failed: {} (os_error={:?}); exiting query loop", conn_id, e, e.raw_os_error());
                tprintln!("[pgwire] conn_id={} snapshot on exit: db='{}' schema='{}' stmts={} portals={} in_error={} last_msg={} last_err='{}'",
                    conn_id,
                    state.current_database,
                    state.current_schema,
                    state.statements.len(),
                    state.portals.len(),
                    state.in_error,
                    last_msg.map(|b| (b as char).to_string()).unwrap_or_else(|| "-".into()),
                    last_err.clone().unwrap_or_else(|| "".into())
                );
                break;
            }
        }
        tprintln!("[pgwire] conn_id={} received message type byte={} (as char='{}')", conn_id, tag[0], tag[0] as char);
        last_msg = Some(tag[0]);
        // Detect zero byte as potential connection closure (client side closed)
        if tag[0] == 0 {
            if !cycle_summary.is_empty() {
                debug!(target: "pgwire", "conn_id={} cycle(partial): {}", conn_id, cycle_summary.trim());
                cycle_summary.clear();
            }
            tprintln!("[pgwire] conn_id={} received zero byte (likely connection closing), exiting query loop", conn_id);
            tprintln!("[pgwire] conn_id={} snapshot on exit: db='{}' schema='{}' stmts={} portals={} in_error={} last_msg={} last_err='{}'",
                conn_id,
                state.current_database,
                state.current_schema,
                state.statements.len(),
                state.portals.len(),
                state.in_error,
                last_msg.map(|b| (b as char).to_string()).unwrap_or_else(|| "-".into()),
                last_err.clone().unwrap_or_else(|| "".into())
            );
            break;
        }
        match tag[0] {
            b'Q' => {
                tprintln!("[pgwire] conn_id={} handling simple Query message", conn_id);
                let len = match read_u32(socket).await { Ok(v) => v, Err(e) => { error!(target:"pgwire", "read_u32 for Q failed: {}", e); break; } };
                let mut qbuf = vec![0u8; (len - 4) as usize];
                if let Err(e) = socket.read_exact(&mut qbuf).await { error!(target:"pgwire", "read_exact(query payload) failed: {}", e); break; }
                if let Some(pos) = qbuf.iter().position(|&b| b == 0) { qbuf.truncate(pos); }
                let query_str = String::from_utf8(qbuf).unwrap_or_default();
                if let Err(e) = handle_query(socket, store, user, state, &query_str).await {
                    error!(target: "pgwire", "handle_query error: {}", e);
                    let _ = send_error(socket, &format!("{}", e)).await; state.in_error = true;
                    last_err = Some(e.to_string());
                    cycle_summary.push_str("Q err; ");
                } else {
                    cycle_summary.push_str("Q ok; ");
                }
            }
            b'P' => { // Parse
                tprintln!("[pgwire] conn_id={} handling Parse message", conn_id);
                if let Err(e) = handle_parse(socket, state).await {
                    error!(target: "pgwire", "handle_parse error: {}", e);
                    let _ = send_error(socket, &format!("{}", e)).await; state.in_error = true;
                    last_err = Some(e.to_string());
                    cycle_summary.push_str("P err; ");
                } else {
                    cycle_summary.push_str("P ok; ");
                }
            }
            b'B' => { // Bind
                tprintln!("[pgwire] conn_id={} handling Bind message", conn_id);
                if let Err(e) = handle_bind(socket, state).await {
                    error!(target: "pgwire", "handle_bind error: {}", e);
                    let _ = send_error(socket, &format!("{}", e)).await; state.in_error = true;
                    last_err = Some(e.to_string());
                    cycle_summary.push_str("B err; ");
                } else {
                    cycle_summary.push_str("B ok; ");
                }
            }
            b'D' => { // Describe
                tprintln!("[pgwire] conn_id={} handling Describe message", conn_id);
                if let Err(e) = handle_describe(socket, store, state).await {
                    error!(target: "pgwire", "handle_describe error: {}", e);
                    let _ = send_error(socket, &format!("{}", e)).await; state.in_error = true;
                    last_err = Some(e.to_string());
                    cycle_summary.push_str("D err; ");
                } else {
                    cycle_summary.push_str("D ok; ");
                }
            }
            b'E' => { // Execute
                tprintln!("[pgwire] conn_id={} handling Execute message", conn_id);
                if let Err(e) = handle_execute(socket, store, user, state).await {
                    error!(target: "pgwire", "handle_execute error: {}", e);
                    let _ = send_error(socket, &format!("{}", e)).await; state.in_error = true;
                    last_err = Some(e.to_string());
                    cycle_summary.push_str("E err; ");
                } else {
                    cycle_summary.push_str("E ok; ");
                }
            }
            b'H' => { // Flush
                tprintln!("[pgwire] conn_id={} handling Flush message", conn_id);
                // Read and discard the message length (Flush has no additional payload)
                let _len = match read_u32(socket).await { Ok(v) => v, Err(e) => { error!(target:"pgwire", "read_u32 for H failed: {}", e); break; } };
                // Flush pending output; per protocol, no response is sent for Flush itself
                if let Err(e) = socket.flush().await { error!("pgwire: flush error: {}", e); }
                cycle_summary.push_str("H; ");
            }
            b'S' => { // Sync
                tprintln!("[pgwire] conn_id={} handling Sync message", conn_id);
                // Read and discard the message length (Sync has no additional payload)
                let _len = match read_u32(socket).await { Ok(v) => v, Err(e) => { error!(target:"pgwire", "read_u32 for S failed: {}", e); break; } };
                // Clear error state only if not in an explicit transaction. When in_tx and an
                // error occurred, the session remains in failed-transaction state until ROLLBACK.
                if !state.in_tx { state.in_error = false; }
                if let Err(e) = send_ready(socket, state).await { error!(target:"pgwire", "send_ready error: {}", e); break; }
                cycle_summary.push_str("S ready; ");
                // Emit the summary of this extended-protocol cycle
                if !cycle_summary.is_empty() {
                    debug!(target: "pgwire", "conn_id={} cycle: {}", conn_id, cycle_summary.trim());
                    cycle_summary.clear();
                }
            }
            b'C' => { // Close
                tprintln!("[pgwire] conn_id={} handling Close message", conn_id);
                if let Err(e) = handle_close(socket, state).await {
                    error!(target: "pgwire", "handle_close error: {}", e);
                    let _ = send_error(socket, &format!("{}", e)).await; state.in_error = true;
                    last_err = Some(e.to_string());
                    cycle_summary.push_str("C err; ");
                } else {
                    cycle_summary.push_str("C ok; ");
                }
            }
            b'X' => { 
                tprintln!("[pgwire] conn_id={} received Terminate message, closing connection", conn_id);
                // Read and discard the message length (Terminate has no additional payload)
                let _len = match read_u32(socket).await { Ok(v) => v, Err(e) => { error!(target:"pgwire", "read_u32 for X failed: {}", e); break; } };
                tprintln!("[pgwire] conn_id={} snapshot on exit: db='{}' schema='{}' stmts={} portals={} in_error={} last_msg={} last_err='{}'",
                    conn_id,
                    state.current_database,
                    state.current_schema,
                    state.statements.len(),
                    state.portals.len(),
                    state.in_error,
                    last_msg.map(|b| (b as char).to_string()).unwrap_or_else(|| "-".into()),
                    last_err.clone().unwrap_or_else(|| "".into())
                );
                break; 
            }
            _ => { 
                // Unknown message: send ErrorResponse and enter error state; client should follow with Sync
                tprintln!("[pgwire] conn_id={} unknown message type byte={} (as char='{}'), sending ErrorResponse and waiting for Sync", conn_id, tag[0], tag[0] as char);
                if pgwire_trace_enabled() {
                    // Try to read the length to dump some bytes (non-destructive best-effort)
                    if let Ok(len) = read_u32(socket).await { 
                        let mut tmp = vec![0u8; len.saturating_sub(4) as usize];
                        if socket.read_exact(&mut tmp).await.is_ok() {
                            debug!(target: "pgwire", "unknown frame payload (first 64 bytes): {}", hex_dump_prefix(&tmp, 64));
                        }
                    }
                }
                send_error(socket, "unsupported message type").await?;
                state.in_error = true;
                last_err = Some("unsupported message type".to_string());
                cycle_summary.push_str("? err; ");
            }
        }
    }
    debug!(target: "pgwire", "conn_id={} exiting query loop for user '{}'", conn_id, user);
    Ok(())
}

async fn send_ready_with_status(socket: &mut tokio::net::TcpStream, status: u8) -> Result<()> {
    debug!(target: "pgwire", "sending ReadyForQuery (status='{}')", status as char);
    crate::tprintln!("pgwire ReadyForQuery status='{}'", status as char);
    socket.write_all(b"Z").await?;
    write_i32(socket, 5).await?; // len
    socket.write_all(&[status]).await?; // 'I' idle, 'T' in-transaction, 'E' failed txn
    if let Err(e) = socket.flush().await { error!(target:"pgwire", "flush ReadyForQuery failed: {}", e); return Err(e.into()); }
    debug!(target: "pgwire", "ReadyForQuery flushed to client");
    Ok(())
}

#[inline]
async fn send_ready(socket: &mut tokio::net::TcpStream, state: &ConnState) -> Result<()> {
    let status = if state.in_tx {
        if state.in_error { b'E' } else { b'T' }
    } else { b'I' };
    send_ready_with_status(socket, status).await
}

async fn write_parameter(socket: &mut tokio::net::TcpStream, k: &str, v: &str) -> Result<()> {
    socket.write_all(b"S").await?;
    let mut payload = Vec::new();
    payload.extend_from_slice(k.as_bytes()); payload.push(0);
    payload.extend_from_slice(v.as_bytes()); payload.push(0);
    write_i32(socket, (payload.len() + 4) as i32).await?;
    socket.write_all(&payload).await?;
    Ok(())
}


async fn handle_query(socket: &mut tokio::net::TcpStream, store: &SharedStore, _username: &str, state: &mut ConnState, q: &str) -> Result<()> {
    // Simple Query cycle: may contain one or multiple semicolon-separated statements.
    // For each statement: emit RowDescription/DataRow only for SELECT-like; always emit CommandComplete.
    // After processing all statements in the message, emit a single ReadyForQuery.
    let sql = q;
    // A very small splitter that respects semicolons and trims whitespace; it does not handle complex cases
    // like semicolons inside quoted strings (rare in client-generated SQL for our use). Good enough for now.
    let parts: Vec<String> = sql
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    tprintln!("pgwire simple query: {} statement(s)\n {:?}", parts.len(), parts);
    for (idx, stmt) in parts.iter().enumerate() {
        let q_trim = stmt.trim();
        debug!("pgwire simple query [{}]: {}", idx, q_trim);
        // Intercept transaction control and common SHOW/SELECT meta that ORMs send
        let up = q_trim.to_uppercase();

        let q_effective = exec::normalize_query_with_defaults(q_trim, &state.current_database, &state.current_schema);
        let upper = q_trim.chars().take(32).collect::<String>().to_uppercase();
        // Treat SHOW as a row-returning command similar to SELECT for client compatibility
        let is_select_like = upper.starts_with("SELECT") || upper.starts_with("WITH ") || upper.starts_with("SHOW ");
        if is_select_like {
            // Use the query engine directly to preserve schema even for empty results
            match query::parse(&q_effective) {
                Ok(Command::Select(sel)) => {
                    match handle_select(store, &sel) {
                        Ok((df, _into)) => {
                            let cols: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
                            let oids: Vec<i32> = df.get_columns().iter().map(|s| map_polars_dtype_to_pg_oid(s.dtype())).collect();
                            // Emit RowDescription with columns even if there are no rows
                            send_row_description(socket, &cols, &oids).await?;
                            // Emit DataRow frames
                            for row_idx in 0..df.height() {
                                let mut row: Vec<Option<String>> = Vec::with_capacity(cols.len());
                                for s in df.get_columns() {
                                    let v = s.as_materialized_series().get(row_idx);
                                    let cell = match v {
                                        Ok(av) => anyvalue_to_opt_string(&av),
                                        Err(_) => None,
                                    };
                                    row.push(cell);
                                }
                                send_data_row(socket, &row).await?;
                            }
                            let tag = format!("SELECT {}", df.height());
                            send_command_complete(socket, &tag).await?;
                        }
                        Err(e) => { send_error(socket, &format!("{}", e)).await?; state.in_error = true; }
                    }
                }
                Ok(_) | Err(_) => {
                    // Fallback to legacy path
                    match exec::execute_query_safe(store, &q_effective).await {
                        Ok(val) => {
                            let (cols, data) = match &val {
                                serde_json::Value::Array(arr) => to_table(arr.clone())?,
                                serde_json::Value::Object(_) => to_table(vec![val.clone()])?,
                                _ => to_table(vec![val.clone()])?,
                            };
                            // Emit RowDescription even if empty; infer OIDs heuristically from first row or default TEXT
                            let oids: Vec<i32> = if let Some(first) = data.first() { first.iter().map(|v| v.as_deref().map(infer_literal_oid_from_value).unwrap_or(PG_TYPE_TEXT)).collect() } else { vec![PG_TYPE_TEXT; cols.len()] };
                            send_row_description(socket, &cols, &oids).await?;
                            for row in data.iter() { send_data_row(socket, row).await?; }
                            let tag = format!("SELECT {}", data.len());
                            send_command_complete(socket, &tag).await?;
                        }
                        Err(e) => { send_error(socket, &format!("{}", e)).await?; state.in_error = true; }
                    }
                }
            }
        } else {
            match exec::execute_query_safe(store, &q_effective).await {
                Ok(val) => {
                    let (cols, data): (Vec<String>, Vec<Vec<Option<String>>>) = (Vec::new(), Vec::new());
                    let tag = if upper.starts_with("SELECT") { format!("SELECT {}", data.len()) }
                        else if upper.starts_with("CALCULATE") { let saved = match &val { serde_json::Value::Object(m) => m.get("saved").and_then(|v| v.as_u64()).unwrap_or(0), _ => 0 }; format!("CALCULATE {}", saved) }
                        else if upper.starts_with("DELETE") { "DELETE".to_string() }
                        else if upper.starts_with("SHOW ") { format!("SHOW {}", data.len()) }
                        else if upper.starts_with("SCHEMA") || upper.starts_with("DATABASE") { format!("OK {}", data.len()) }
                        else if upper.starts_with("SET") { "SET".to_string() }
                        else if upper.starts_with("CREATE TABLE") { "CREATE TABLE".to_string() }
                        else if data.is_empty() { "OK".to_string() } else { format!("OK {}", data.len()) };
                    debug!("pgwire simple query [{}]: CommandComplete tag='{}'", idx, tag);
                    send_command_complete(socket, &tag).await?;
                }
                Err(e) => {
                    debug!("pgwire simple query [{}]: error: {}", idx, e);
                    send_error(socket, &format!("{}", e)).await?;
                    state.in_error = true;
                }
            }
        }
    }
    // Finish the Simple Query message cycle
    crate::tprintln!("pgwire: simple cycle end; in_tx={} in_error={}", state.in_tx, state.in_error);
    send_ready(socket, state).await?;
    Ok(())
}

fn to_table(rows: Vec<serde_json::Value>) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
    let mut cols: Vec<String> = Vec::new();
    let mut data: Vec<Vec<Option<String>>> = Vec::new();
    for row in rows.into_iter() {
        match row {
            serde_json::Value::Object(map) => {
                if cols.is_empty() {
                    // Preserve insertion order from the first object row (no sorting)
                    for (k, _v) in map.iter() { cols.push(k.clone()); }
                } else {
                    // If later rows introduce new keys, append them and backfill previous rows with NULLs
                    for (k, _v) in map.iter() {
                        if !cols.iter().any(|c| c == k) { cols.push(k.clone()); }
                    }
                    let need = cols.len();
                    for r in data.iter_mut() { if r.len() < need { r.resize(need, None); } }
                }
                let mut r: Vec<Option<String>> = Vec::with_capacity(cols.len());
                for c in &cols {
                    let v = map.get(c).cloned();
                    r.push(match v {
                        None | Some(serde_json::Value::Null) => None,
                        Some(serde_json::Value::String(s)) => Some(s),
                        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
                        Some(serde_json::Value::Bool(b)) => Some(if b {"t".into()} else {"f".into()}),
                        Some(other) => Some(other.to_string()),
                    });
                }
                data.push(r);
            }
            other => {
                if cols.is_empty() { cols = vec!["value".into()]; }
                data.push(vec![Some(other.to_string())]);
            }
        }
    }
    Ok((cols, data))
}

fn map_polars_dtype_to_pg_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => 16,
        DataType::Int8 | DataType::Int16 => 21,
        DataType::Int32 => 23,
        DataType::Int64 => 20,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::Binary => 17, // bytea
        DataType::String | DataType::Categorical(_, _) => 25,
        DataType::Date => 1082, // date
        DataType::Datetime(_, tz) => if tz.is_some() { 1184 } else { 1114 }, // timestamptz or timestamp
        DataType::Time => 1083, // time without tz
        DataType::Duration(_) => 1186, // interval
        DataType::Decimal(_, _) => 1700, // numeric/decimal
        DataType::List(inner) => map_pg_array_oid(inner),
        DataType::Struct(_) => 2249, // record
        _ => PG_TYPE_TEXT,
    }
}

fn map_pg_array_oid(inner: &DataType) -> i32 {
    match inner.as_ref() {
        DataType::Boolean => 1000, // bool[]
        DataType::Int16 | DataType::Int8 => 1005, // int2[]
        DataType::Int32 => 1007, // int4[]
        DataType::Int64 | DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => 1016, // int8[]
        DataType::Float32 => 1021, // float4[]
        DataType::Float64 => 1022, // float8[]
        DataType::String | DataType::Categorical(_, _) => 1009, // text[]
        DataType::Binary => 1001, // bytea[]
        DataType::Date => 1182, // date[]
        DataType::Datetime(_, tz) => if tz.is_some() { 1185 } else { 1115 }, // timestamptz[] or timestamp[]
        DataType::Time => 1183, // time[]
        DataType::Decimal(_, _) => 1231, // numeric[]
        _ => 1009,
    }
}

fn infer_literal_oid_from_value(s: &str) -> i32 {
    // Very small heuristic for constant SELECTs in Describe
    if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") { return 16; }
    if s.parse::<i32>().is_ok() { return 23; }
    if s.parse::<i64>().is_ok() { return 20; }
    if s.parse::<f64>().is_ok() { return 701; }
    25
}

async fn send_row_description(socket: &mut tokio::net::TcpStream, cols: &[String], oids: &[i32]) -> Result<()> {
    debug!(target: "pgwire", "sending RowDescription ({} columns): {:?}", cols.len(), cols);
    socket.write_all(b"T").await?;
    // Build payload
    let mut payload = Vec::new();
    let n: i16 = cols.len() as i16;
    payload.extend_from_slice(&n.to_be_bytes());
    for (idx, name) in cols.iter().enumerate() {
        payload.extend_from_slice(name.as_bytes()); payload.push(0); // field name
        payload.extend_from_slice(&0i32.to_be_bytes()); // table oid
        payload.extend_from_slice(&0i16.to_be_bytes()); // attr number
        let oid = *oids.get(idx).unwrap_or(&PG_TYPE_TEXT);
        payload.extend_from_slice(&oid.to_be_bytes()); // type oid
        payload.extend_from_slice(&(-1i16).to_be_bytes()); // type size (variable)
        payload.extend_from_slice(&0i32.to_be_bytes()); // type modifier
        payload.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    let total_len = (payload.len() + 4) as i32;
    debug!(target: "pgwire", "RowDescription payload_len={} total_frame_len={}", payload.len(), total_len);
    write_i32(socket, total_len).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

async fn send_data_row(socket: &mut tokio::net::TcpStream, row: &[Option<String>]) -> Result<()> {
    socket.write_all(b"D").await?;
    let mut payload = Vec::new();
    let n: i16 = row.len() as i16;
    payload.extend_from_slice(&n.to_be_bytes());
    for cell in row {
        match cell {
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len() as i32;
                payload.extend_from_slice(&len.to_be_bytes());
                payload.extend_from_slice(bytes);
            }
        }
    }
    let total_len = (payload.len() + 4) as i32;
    debug!(target: "pgwire", "DataRow payload_len={} total_frame_len={}", payload.len(), total_len);
    write_i32(socket, total_len).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

async fn send_data_row_binary(socket: &mut tokio::net::TcpStream, anyvalues: &[AnyValue<'_>], oids: &[i32], fmts: &[i16]) -> Result<()> {
    // fmts: effective per-column result format code (0=text, 1=binary)
    socket.write_all(b"D").await?;
    let mut payload = Vec::new();
    let n: i16 = anyvalues.len() as i16;
    payload.extend_from_slice(&n.to_be_bytes());
    for (i, av) in anyvalues.iter().enumerate() {
        let fmt = *fmts.get(i).unwrap_or(&0);
        if matches!(av, AnyValue::Null) {
            payload.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        if fmt == 1 {
            // binary
            let oid = *oids.get(i).unwrap_or(&PG_TYPE_TEXT);
            match (oid, av) {
                (16, AnyValue::Boolean(b)) => { // bool
                    payload.extend_from_slice(&1i32.to_be_bytes());
                    payload.push(if *b { 1 } else { 0 });
                }
                (21, AnyValue::Int16(v)) => {
                    payload.extend_from_slice(&2i32.to_be_bytes());
                    payload.extend_from_slice(&v.to_be_bytes());
                }
                (23, AnyValue::Int32(v)) => {
                    payload.extend_from_slice(&4i32.to_be_bytes());
                    payload.extend_from_slice(&v.to_be_bytes());
                }
                (20, AnyValue::Int64(v)) => {
                    payload.extend_from_slice(&8i32.to_be_bytes());
                    payload.extend_from_slice(&v.to_be_bytes());
                }
                (700, AnyValue::Float32(f)) => {
                    let bits = f.to_bits();
                    payload.extend_from_slice(&4i32.to_be_bytes());
                    payload.extend_from_slice(&bits.to_be_bytes());
                }
                (701, AnyValue::Float64(f)) => {
                    let bits = f.to_bits();
                    payload.extend_from_slice(&8i32.to_be_bytes());
                    payload.extend_from_slice(&bits.to_be_bytes());
                }
                (17, AnyValue::Binary(b)) => {
                    payload.extend_from_slice(&(b.len() as i32).to_be_bytes());
                    payload.extend_from_slice(b);
                }
                // Fallback to text for other combos
                _ => {
                    let s = format!("{}", av);
                    let bytes = s.as_bytes();
                    payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    payload.extend_from_slice(bytes);
                }
            }
        } else {
            // text format
            let s = match anyvalue_to_opt_string(av) { Some(s) => s, None => String::new() };
            let bytes = s.as_bytes();
            payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            payload.extend_from_slice(bytes);
        }
    }
    let total_len = (payload.len() + 4) as i32;
    write_i32(socket, total_len).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

async fn send_command_complete(socket: &mut tokio::net::TcpStream, tag: &str) -> Result<()> {
    socket.write_all(b"C").await?;
    let mut payload = Vec::new();
    payload.extend_from_slice(tag.as_bytes()); payload.push(0);
    let total_len = (payload.len() + 4) as i32;
    debug!(target: "pgwire", "CommandComplete tag='{}' payload_len={} total_frame_len={}", tag, payload.len(), total_len);
    write_i32(socket, total_len).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

async fn send_error(socket: &mut tokio::net::TcpStream, msg: &str) -> Result<()> {
    socket.write_all(b"E").await?;
    // Very simple error: 'S' severity, 'M' message, terminator 0
    let mut payload = Vec::new();
    payload.push(b'S'); payload.extend_from_slice(b"ERROR"); payload.push(0);
    payload.push(b'M'); payload.extend_from_slice(msg.as_bytes()); payload.push(0);
    payload.push(0);
    write_i32(socket, (payload.len() + 4) as i32).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

// Helper: map AppError (when available) to richer pgwire ErrorResponse fields.
// Falls back to generic send_error for non-AppError cases.
async fn send_mapped_error(socket: &mut tokio::net::TcpStream, err: &anyhow::Error) -> Result<()> {
    if let Some(app) = err.downcast_ref::<crate::error::AppError>() {
        let (sqlstate, severity, message) = app.pgwire_fields();
        socket.write_all(b"E").await?;
        let mut payload = Vec::new();
        // Severity
        payload.push(b'S'); payload.extend_from_slice(severity.as_bytes()); payload.push(0);
        // SQLSTATE code
        payload.push(b'C'); payload.extend_from_slice(sqlstate.as_bytes()); payload.push(0);
        // Message
        payload.push(b'M'); payload.extend_from_slice(message.as_bytes()); payload.push(0);
        // Terminator
        payload.push(0);
        write_i32(socket, (payload.len() + 4) as i32).await?;
        socket.write_all(&payload).await?;
        Ok(())
    } else {
        send_error(socket, &format!("{}", err)).await
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0, // extremely unlikely; avoid panic on clock skew
    }
}

#[derive(Debug, Clone)]
struct InsertStmt { database: String, columns: Vec<String>, values: Vec<InsertValue> }

#[derive(Debug, Clone)]
enum InsertValue { Null, Number(i64), String(String) }

fn normalize_object_to_db(name: &str) -> String {
    // Accept three-level db/schema/table (with slashes) and return as-is.
    let s = name.trim().trim_matches('"');
    if s.contains('/') { return s.to_string(); }
    // Support dot-separated identifiers: db.schema.table or schema.table
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() == 3 {
        return format!("{}/{}/{}.time", parts[0].trim_matches('"'), parts[1].trim_matches('"'), parts[2].trim_matches('"'));
    }
    if parts.len() == 2 {
        return format!("{}/{}.time", parts[0].trim_matches('"'), parts[1].trim_matches('"'));
    }
    s.to_string()
}

fn parse_insert(q: &str) -> Option<InsertStmt> {
    // Very small parser: INSERT INTO db (a,b,...) VALUES (x,y,...)
    // Values support: numeric literals, single-quoted strings, NULL
    let ql = q.to_ascii_lowercase();
    if !ql.starts_with("insert into ") { return None; }
    // Extract into parts
    let rest = &q["insert into ".len()..];
    // db name up to space or '('
    let mut chars = rest.chars().peekable();
    let mut db = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() || ch == '(' { break; }
        db.push(ch); chars.next();
    }
    // Normalize db/object identifier into internal path
    let db = normalize_object_to_db(&db);
    // Skip spaces
    while let Some(&ch) = chars.peek() { if ch.is_whitespace() { chars.next(); } else { break; } }
    if chars.peek() != Some(&'(') { return None; }
    chars.next(); // consume '('
    // Read column list
    let mut cols: Vec<String> = Vec::new();
    let mut cur = String::new();
    for ch in chars.by_ref() {
        if ch == ')' { if !cur.trim().is_empty() { cols.push(cur.trim().to_string()); } break; }
        if ch == ',' { cols.push(cur.trim().to_string()); cur.clear(); }
        else { cur.push(ch); }
    }
    // Skip spaces
    while let Some(&ch) = chars.peek() { if ch.is_whitespace() { chars.next(); } else { break; } }
    // Expect VALUES
    let mut rest2: String = chars.collect();
    let rest2l = rest2.to_ascii_lowercase();
    if !rest2l.starts_with("values") { return None; }
    rest2 = rest2["values".len()..].trim().to_string();
    if !rest2.starts_with('(') { return None; }
    let inner = rest2.trim().trim_start_matches('(').trim_end_matches(')').trim().to_string();
    // Split by comma respecting single quotes
    let mut vals: Vec<String> = Vec::new();
    let mut curv = String::new();
    let mut in_str = false;
    let mut esc = false;
    for ch in inner.chars() {
        if in_str {
            if esc { curv.push(ch); esc = false; continue; }
            if ch == '\\' { esc = true; continue; }
            if ch == '\'' { in_str = false; continue; }
            curv.push(ch);
        } else {
            if ch == '\'' { in_str = true; continue; }
            if ch == ',' { vals.push(curv.trim().to_string()); curv.clear(); continue; }
            curv.push(ch);
        }
    }
    if !curv.is_empty() { vals.push(curv.trim().to_string()); }
    let values: Vec<InsertValue> = vals.into_iter().map(|s| parse_value(&s)).collect();
    if cols.len() != values.len() { return None; }
    Some(InsertStmt { database: db, columns: cols, values })
}

fn parse_value(s: &str) -> InsertValue {
    if s.eq_ignore_ascii_case("null") { return InsertValue::Null; }
    if let Ok(n) = s.parse::<i64>() { return InsertValue::Number(n); }
    // If it came from quotes, we should have had only inner content without quotes.
    // But if user wrote unquoted text, treat it as string.
    InsertValue::String(s.trim_matches('\'').to_string())
}


async fn read_u32(socket: &mut tokio::net::TcpStream) -> Result<u32> {
    let mut b = [0u8; 4]; socket.read_exact(&mut b).await?; Ok(u32::from_be_bytes(b))
}

async fn write_msg_header(socket: &mut tokio::net::TcpStream, tag: u8, len: i32) -> Result<()> {
    socket.write_all(&[tag]).await?; write_i32(socket, len).await
}

async fn write_i32(socket: &mut tokio::net::TcpStream, v: i32) -> Result<()> { socket.write_all(&v.to_be_bytes()).await.map_err(|e| e.into()) }



// Extended protocol handlers and helpers

async fn handle_parse(socket: &mut tokio::net::TcpStream, state: &mut ConnState) -> Result<()> {
    let len_total = read_u32(socket).await? as usize;
    let mut buf = vec![0u8; len_total - 4];
    socket.read_exact(&mut buf).await?;
    // parse: statement name (cstring), query (cstring), i16 num_types, i32[*] types
    let mut i = 0usize;
    fn read_cstr_from(buf: &[u8], i: &mut usize) -> Result<String> {
        let start = *i;
        while *i < buf.len() && buf[*i] != 0 { *i += 1; }
        if *i >= buf.len() { return Err(anyhow!("parse: cstring out of bounds")); }
        let s = String::from_utf8_lossy(&buf[start..*i]).into_owned();
        *i += 1; // skip null
        Ok(s)
    }
    fn read_i16_from(buf: &[u8], i: &mut usize) -> Result<i16> {
        if *i + 2 > buf.len() { return Err(anyhow!("parse: i16 out of bounds")); }
        let v = i16::from_be_bytes([buf[*i], buf[*i+1]]);
        *i += 2; Ok(v)
    }
    fn read_i32_from(buf: &[u8], i: &mut usize) -> Result<i32> {
        if *i + 4 > buf.len() { return Err(anyhow!("parse: i32 out of bounds")); }
        let v = i32::from_be_bytes([buf[*i], buf[*i+1], buf[*i+2], buf[*i+3]]);
        *i += 4; Ok(v)
    }
    let stmt_name = read_cstr_from(&buf, &mut i)?;
    let sql = read_cstr_from(&buf, &mut i)?;
    debug!("pgwire parse (stmt='{}'): {}", stmt_name, sql);
    let ntypes = read_i16_from(&buf, &mut i)? as usize;
    let mut param_types: Vec<i32> = Vec::with_capacity(ntypes);
    for _ in 0..ntypes { param_types.push(read_i32_from(&buf, &mut i)?); }
    // If client did not provide parameter types, infer from $n placeholders and casts
    if param_types.is_empty() {
        let re_dollar = Regex::new(r"\$([1-9][0-9]*)")?;
        let mut max_idx = 0usize;
        for cap in re_dollar.captures_iter(&sql) {
            if let Some(m) = cap.get(1) {
                if let Ok(idx) = m.as_str().parse::<usize>() { if idx > max_idx { max_idx = idx; } }
            }
        }
        if max_idx > 0 {
            // default to TEXT
            param_types = vec![PG_TYPE_TEXT; max_idx];
            // refine using explicit casts like $1::int8, $2::float8, etc.
            let re_cast = Regex::new(r"\$([1-9][0-9]*)::([A-Za-z0-9_]+)")?;
            for cap in re_cast.captures_iter(&sql) {
                let idx: usize = cap.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
                let ty = cap.get(2).map(|m| m.as_str().to_ascii_lowercase()).unwrap_or_default();
                let oid = match ty.as_str() {
                    // integers
                    "int" | "int4" | "integer" => 23,
                    "int8" | "bigint" => 20,
                    "float8" | "double" | "double precision" => 701,
                    "text" | "varchar" | "character varying" => 25,
                    "bool" | "boolean" => 16,
                    _ => PG_TYPE_TEXT,
                };
                if idx > 0 && idx - 1 < param_types.len() { param_types[idx - 1] = oid; }
            }
            debug!("pgwire parse: inferred {} parameter(s) with types {:?}", max_idx, param_types);
        }
    }
    // store
    if stmt_name.is_empty() {
        state.statements.insert("".into(), PreparedStatement { name: "".into(), sql, param_types });
    } else {
        state.statements.insert(stmt_name.clone(), PreparedStatement { name: stmt_name, sql, param_types });
    }
    send_parse_complete(socket).await
}

async fn handle_bind(socket: &mut tokio::net::TcpStream, state: &mut ConnState) -> Result<()> {
    let len_total = read_u32(socket).await? as usize;
    let mut buf = vec![0u8; len_total - 4];
    socket.read_exact(&mut buf).await?;
    let mut i = 0usize;
    fn r_cstr(buf: &[u8], i: &mut usize) -> Result<String> { let s = {
        let start = *i; while *i < buf.len() && buf[*i] != 0 { *i += 1; } if *i >= buf.len() { return Err(anyhow!("bind cstr oob")); }
        String::from_utf8_lossy(&buf[start..*i]).into_owned() }; *i += 1; Ok(s) }
    fn r_i16(buf: &[u8], i: &mut usize) -> Result<i16> { if *i+2>buf.len(){return Err(anyhow!("bind i16 oob"));} let v=i16::from_be_bytes([buf[*i],buf[*i+1]]); *i+=2; Ok(v) }
    fn r_i32(buf: &[u8], i: &mut usize) -> Result<i32> { if *i+4>buf.len(){return Err(anyhow!("bind i32 oob"));} let v=i32::from_be_bytes([buf[*i],buf[*i+1],buf[*i+2],buf[*i+3]]); *i+=4; Ok(v) }
    fn r_bytes(buf:&[u8], i:&mut usize, n:usize) -> Result<Vec<u8>> { if *i+n>buf.len(){return Err(anyhow!("bind bytes oob"));} let v=buf[*i..*i+n].to_vec(); *i+=n; Ok(v) }

    let portal_name = r_cstr(&buf, &mut i)?;
    let stmt_name = r_cstr(&buf, &mut i)?;
    debug!("pgwire bind: portal='{}', stmt='{}'", portal_name, stmt_name);

    // parameter format codes
    let n_formats = r_i16(&buf, &mut i)? as usize;
    let mut param_formats: Vec<i16> = Vec::with_capacity(n_formats);
    for _ in 0..n_formats { param_formats.push(r_i16(&buf, &mut i)?); }

    // parameter values
    let n_params = r_i16(&buf, &mut i)? as usize;
    let mut params: Vec<Option<String>> = Vec::with_capacity(n_params);

    // Resolve the prepared statement for OID hints
    let stmt = state.statements.get(&stmt_name).or_else(|| state.statements.get(""));
    let stmt_param_types: Vec<i32> = stmt.map(|s| s.param_types.clone()).unwrap_or_default();

    // Determine effective parameter formats per-parameter per protocol rules:
    // - if n_formats == 0: all text (0)
    // - if n_formats == 1: that single code applies to all parameters
    // - if n_formats == n_params: use per-parameter codes
    // - otherwise: protocol error
    let effective_formats: Vec<i16> = if n_formats == 0 {
        vec![0; n_params]
    } else if n_formats == 1 {
        vec![param_formats.get(0).cloned().unwrap_or(0); n_params]
    } else if n_formats == n_params {
        param_formats.clone()
    } else {
        send_error(socket, "invalid parameter formats").await?;
        state.in_error = true;
        return Ok(());
    };

    // Helper: decode a binary parameter into a text literal representation suitable for our engine
    fn decode_binary_param(oid: i32, bytes: &[u8]) -> Option<String> {
        match oid {
            16 => { // bool
                if bytes.len() == 1 { Some(if bytes[0] != 0 { "true".to_string() } else { "false".to_string() }) } else { None }
            }
            20 => { // int8
                if bytes.len() == 8 { Some(i64::from_be_bytes(bytes.try_into().ok()?).to_string()) } else { None }
            }
            21 => { // int2
                if bytes.len() == 2 { Some(i16::from_be_bytes(bytes.try_into().ok()?).to_string()) } else { None }
            }
            23 => { // int4
                if bytes.len() == 4 { Some(i32::from_be_bytes(bytes.try_into().ok()?).to_string()) } else { None }
            }
            700 => { // float4
                if bytes.len() == 4 { Some(f32::from_bits(u32::from_be_bytes(bytes.try_into().ok()?)).to_string()) } else { None }
            }
            701 => { // float8
                if bytes.len() == 8 { Some(f64::from_bits(u64::from_be_bytes(bytes.try_into().ok()?)).to_string()) } else { None }
            }
            25 | 1043 | 1042 => { // text, varchar, bpchar — in practice binary is just raw string bytes
                Some(String::from_utf8_lossy(bytes).into_owned())
            }
            _ => None,
        }
    }

    for pidx in 0..n_params {
        let sz = r_i32(&buf, &mut i)?;
        if sz < 0 {
            params.push(None);
            continue;
        }
        let bytes = r_bytes(&buf, &mut i, sz as usize)?;
        let fmt = effective_formats.get(pidx).cloned().unwrap_or(0);
        if fmt == 0 {
            // text parameter
            params.push(Some(String::from_utf8_lossy(&bytes).into_owned()));
        } else {
            // binary parameter
            let oid = stmt_param_types.get(pidx).cloned().unwrap_or(0);
            // Try decode using OID first
            let decoded = if oid != 0 { decode_binary_param(oid, &bytes) } else { None };
            let val = if let Some(s) = decoded {
                s
            } else {
                // Fallback heuristics when OID is unknown
                let s_opt = match bytes.len() {
                    8 => bytes.clone().try_into().ok().map(|a: [u8;8]| i64::from_be_bytes(a).to_string()),
                    4 => bytes.clone().try_into().ok().map(|a: [u8;4]| i32::from_be_bytes(a).to_string()),
                    2 => bytes.clone().try_into().ok().map(|a: [u8;2]| i16::from_be_bytes(a).to_string()),
                    1 => Some((bytes[0] != 0).to_string()),
                    _ => None,
                };
                s_opt.unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned())
            };
            params.push(Some(val));
        }
    }

    // result-column formats
    let n_rfmts = r_i16(&buf, &mut i)? as usize;
    let mut result_formats: Vec<i16> = Vec::with_capacity(n_rfmts);
    for _ in 0..n_rfmts { result_formats.push(r_i16(&buf, &mut i)?); }
    // We currently only emit text results; honor requested format but we will still send text rows.

    // Store portal
    let p = Portal { name: portal_name.clone(), stmt_name, params, param_formats, result_formats };
    state.portals.insert(portal_name, p);

    send_bind_complete(socket).await
}

fn escape_sql_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() { if ch == '\'' { out.push('\''); out.push('\''); } else { out.push(ch); } }
    out.push('\'');
    out
}

fn substitute_placeholders(sql: &str, params: &[Option<String>]) -> Result<String> {
    substitute_placeholders_typed(sql, params, None)
}

fn substitute_placeholders_typed(sql: &str, params: &[Option<String>], param_types: Option<&[i32]>) -> Result<String> {
    // Detect named placeholders of the form %(name)s
    let re_named = Regex::new(r"%\(([A-Za-z0-9_]+)\)s")?;
    let mut out = String::new();
    if re_named.is_match(sql) {
        // Collect order of first occurrence per name
        let mut order: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for cap in re_named.captures_iter(sql) {
            let name = match cap.get(1) { Some(m) => m.as_str().to_string(), None => continue };
            if !seen.contains(&name) { seen.insert(name.clone()); order.push(name); }
        }
        if order.len() != params.len() { bail!("mismatch parameter count: expected {} got {}", order.len(), params.len()); }
        let mut map: HashMap<String, Option<String>> = HashMap::new();
        for (i, name) in order.iter().enumerate() { map.insert(name.clone(), params[i].clone()); }
        // Replace all occurrences
        let mut last = 0usize;
        for m in re_named.find_iter(sql) {
            out.push_str(&sql[last..m.start()]);
            let name = re_named
                .captures(m.as_str())
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_string())
                .ok_or_else(|| anyhow!("named placeholder parse error"))?;
            match map.get(&name).and_then(|v| v.clone()) {
                None => out.push_str("NULL"),
                Some(val) => out.push_str(&escape_sql_literal(&val)),
            }
            last = m.end();
        }
        out.push_str(&sql[last..]);
        return Ok(out);
    }

    // $n style placeholders (from extended protocol)
    let re_dollar = Regex::new(r"\$([1-9][0-9]*)")?;
    if re_dollar.is_match(sql) {
        let mut last = 0usize;
        for m in re_dollar.find_iter(sql) {
            out.push_str(&sql[last..m.start()]);
            let cap = re_dollar.captures(m.as_str()).ok_or_else(|| anyhow!("placeholder parse error"))?;
            let idx: usize = cap
                .get(1)
                .ok_or_else(|| anyhow!("missing placeholder index"))?
                .as_str()
                .parse::<usize>()
                .map_err(|_| anyhow!("invalid placeholder index"))?;
            let pos = idx.checked_sub(1).ok_or_else(|| anyhow!("parameter index underflow"))?;
            if pos >= params.len() { bail!("too few parameters: ${} referenced but only {} provided", idx, params.len()); }
            // Decide quoting based on optional type hint
            let want_raw = if let Some(tys) = param_types { match tys.get(pos).cloned().unwrap_or(0) {
                16 | 20 | 21 | 23 | 700 | 701 => true, // bool and numeric types
                _ => false,
            }} else { false };
            match &params[pos] {
                None => out.push_str("NULL"),
                Some(v) => {
                    if want_raw {
                        out.push_str(v);
                    } else {
                        out.push_str(&escape_sql_literal(v));
                    }
                }
            }
            last = m.end();
        }
        out.push_str(&sql[last..]);
        return Ok(out);
    }

    // Positional %s
    // Replace sequentially occurrences of "%s" that are not part of a named placeholder (we already handled named)
    let mut i = 0usize; let mut pi = 0usize;
    while i < sql.len() {
        if i + 2 <= sql.len() && &sql[i..i+2] == "%s" {
            if pi >= params.len() { bail!("too few parameters: found more %s than values"); }
            match &params[pi] {
                None => out.push_str("NULL"),
                Some(v) => out.push_str(&escape_sql_literal(v)),
            }
            pi += 1; i += 2; continue;
        }
        out.push(sql.as_bytes()[i] as char); i += 1;
    }
    if pi != params.len() { bail!("too many parameters: {} values for {} placeholders", params.len(), pi); }
    Ok(out)
}

async fn describe_row_description(socket: &mut tokio::net::TcpStream, store: &SharedStore, state: &ConnState, sql: &str) -> Result<()> {
    // Attempt to infer column names for SELECT-like statements by delegating to the server
    // executor and deriving a table shape from the first row. For non-SELECT, return NoData.
    let q = sql.trim();
    let up = q.to_uppercase();
    if up.starts_with("SELECT") || up.starts_with("WITH ") || up.starts_with("SHOW ") {
        // Normalize and try to parse into a SELECT to retrieve the output schema
        let q_eff = exec::normalize_query_with_defaults(q, &state.current_database, &state.current_schema);
        match query::parse(&q_eff) {
            Ok(Command::Select(sel)) => {
                match handle_select(store, &sel) {
                    Ok((df, _into)) => {
                        let cols: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
                        let oids: Vec<i32> = df.get_columns().iter().map(|s| map_polars_dtype_to_pg_oid(s.dtype())).collect();
                        // Always send RowDescription for SELECT-like statements
                        return send_row_description(socket, &cols, &oids).await;
                    }
                    Err(_) => {
                        // Fallback to legacy JSON path
                        match exec::execute_query_safe(store, &q_eff).await {
                            Ok(val) => {
                                let (cols, data) = match &val {
                                    serde_json::Value::Array(arr) => to_table(arr.clone())?,
                                    serde_json::Value::Object(_) => to_table(vec![val.clone()])?,
                                    _ => to_table(vec![val.clone()])?,
                                };
                                // Heuristic OIDs from first row literal strings
                                let oids: Vec<i32> = if let Some(first) = data.first() { first.iter().map(|v| v.as_deref().map(infer_literal_oid_from_value).unwrap_or(PG_TYPE_TEXT)).collect() } else { vec![PG_TYPE_TEXT; cols.len()] };
                                return send_row_description(socket, &cols, &oids).await;
                            }
                            Err(_) => return send_no_data(socket).await,
                        }
                    }
                }
            }
            _ => {
                // Could not parse; attempt legacy path
                match exec::execute_query_safe(store, &q_eff).await {
                    Ok(val) => {
                        let (cols, data) = match &val {
                            serde_json::Value::Array(arr) => to_table(arr.clone())?,
                            serde_json::Value::Object(_) => to_table(vec![val.clone()])?,
                            _ => to_table(vec![val.clone()])?,
                        };
                        let oids: Vec<i32> = if let Some(first) = data.first() { first.iter().map(|v| v.as_deref().map(infer_literal_oid_from_value).unwrap_or(PG_TYPE_TEXT)).collect() } else { vec![PG_TYPE_TEXT; cols.len()] };
                        return send_row_description(socket, &cols, &oids).await;
                    }
                    Err(_) => return send_no_data(socket).await,
                }
            }
        }
    } else {
        send_no_data(socket).await
    }
}

async fn handle_describe(socket: &mut tokio::net::TcpStream, store: &SharedStore, state: &mut ConnState) -> Result<()> {
    let _len = read_u32(socket).await? as usize;
    let mut tag = [0u8;1]; socket.read_exact(&mut tag).await?;
    let name = read_cstring(socket).await?;
    tprintln!("[pgwire] describe: type='{}', name='{}'", tag[0] as char, name);
    let res = match tag[0] {
        b'S' => {
            tprintln!("[pgwire] describe prepared statement");
            // prepared statement
            if let Some(stmt) = state.statements.get(&name) {
                // ParameterDescription first
                let ptys = if stmt.param_types.is_empty() { Vec::new() } else { stmt.param_types.clone() };
                send_parameter_description(socket, &ptys).await?;
                // RowDescription
                describe_row_description(socket, store, state, &stmt.sql).await
            } else {
                // unnamed prepared statement is "" name
                if name.is_empty() { if let Some(stmt) = state.statements.get("") {
                    send_parameter_description(socket, &stmt.param_types).await?;
                    describe_row_description(socket, store, state, &stmt.sql).await
                } else { send_parameter_description(socket, &[]).await?; send_no_data(socket).await }
                } else { send_parameter_description(socket, &[]).await?; send_no_data(socket).await }
            }
        }
        b'P' => {
            tprintln!("[pgwire] describe portal");
            // portal: find its stmt
            if let Some(portal) = state.portals.get(&name) {
                if let Some(stmt) = state.statements.get(&portal.stmt_name) {
                    // Perform substitution to allow parser to see final forms (aliases, literal exprs)
                    tprintln!("[pgwire] describe portal, substitute placeholders");
                    let sql_eff = match substitute_placeholders_typed(&stmt.sql, &portal.params, Some(&stmt.param_types)) { Ok(s) => s, Err(_) => stmt.sql.clone() };
                    // ParameterDescription is optional for portal Describe; many servers send only RowDescription
                    tprintln!("[pgwire] describe portal, row description");
                    describe_row_description(socket, store, state, &sql_eff).await
                } else { send_no_data(socket).await }
            } else { send_no_data(socket).await }
        }
        _ => send_no_data(socket).await,
    };
    tprintln!("[pgwire] describe done");
    // Ensure frames are pushed promptly for Describe
    if let Err(e) = socket.flush().await { error!("pgwire: flush error after Describe: {}", e); }
    res
}

async fn handle_execute(socket: &mut tokio::net::TcpStream, store: &SharedStore, _user: &str, state: &mut ConnState) -> Result<()> {
    // Extended protocol Execute: run an already bound portal. Keep pgwire thin and delegate
    // execution to the common server executor. Do not send ReadyForQuery here; Sync handles it.

    let _len = read_u32(socket).await? as usize;
    let portal_name = read_cstring(socket).await?;
    let _max_rows = read_i32(socket).await?; // ignored for now

    // Resolve portal and its prepared statement
    let portal = match state.portals.get(&portal_name) { Some(p) => p.clone(), None => { send_error(socket, "unknown portal").await?; state.in_error = true; return Ok(()); } };
    let stmt = match state.statements.get(&portal.stmt_name) { Some(s) => s, None => { send_error(socket, "unknown statement").await?; state.in_error = true; return Ok(()); } };

    // Perform placeholder substitution and normalize with session defaults
    let substituted = match substitute_placeholders_typed(&stmt.sql, &portal.params, Some(&stmt.param_types)) { Ok(s) => s, Err(e) => { send_error(socket, &format!("{}", e)).await?; state.in_error = true; return Ok(()); } };
    let q_trim = substituted.trim().trim_end_matches(';').trim();
    debug!("pgwire execute (portal='{}'): {}", portal_name, q_trim);
    let q_effective = exec::normalize_query_with_defaults(q_trim, &state.current_database, &state.current_schema);
    debug!(target: "pgwire", "execute effective SQL: {}", q_effective);

    // Try to run via parsed Select to obtain typed rows for binary/text encoding.
    let parsed = query::parse(&q_effective);
    let mut rows_sent: usize = 0;
    if let Ok(Command::Select(sel)) = parsed {
        if let Ok((df, _into)) = handle_select(store, &sel) {
            let ncols = df.width();
            let nrows = df.height();
            // Determine per-column result format codes from portal.requested formats
            let fmts: Vec<i16> = if portal.result_formats.is_empty() {
                vec![0; ncols]
            } else if portal.result_formats.len() == 1 {
                vec![portal.result_formats[0]; ncols]
            } else if portal.result_formats.len() == ncols {
                portal.result_formats.clone()
            } else { vec![0; ncols] };
            // OIDs from schema
            let oids: Vec<i32> = df.get_columns().iter().map(|s| map_polars_dtype_to_pg_oid(s.dtype())).collect();
            // Send rows
            for ridx in 0..nrows {
                // Collect AnyValue per column
                let mut avs: Vec<AnyValue> = Vec::with_capacity(ncols);
                for s in df.get_columns() {
                    avs.push(s.get(ridx));
                }
                // Use binary encoder with per-column format (falls back to text for unsupported combos)
                send_data_row_binary(socket, &avs, &oids, &fmts).await?;
                rows_sent += 1;
            }
            // Build CommandComplete
            let tag = format!("SELECT {}", rows_sent);
            debug!(target: "pgwire", "Execute CommandComplete tag='{}'", tag);
            send_command_complete(socket, &tag).await?;
            if let Err(e) = socket.flush().await { error!(target: "pgwire", "flush after Execute failed: {}", e); }
            return Ok(());
        }
    }

    // Fallback: Delegate execution to common server executor and send text rows only
    match exec::execute_query_safe(store, &q_effective).await {
        Ok(val) => {
            let upper = q_trim.chars().take(32).collect::<String>().to_uppercase();
            let is_select_like = upper.starts_with("SELECT") || upper.starts_with("WITH ");
            let (cols, data) = if is_select_like {
                match &val {
                    serde_json::Value::Array(arr) => to_table(arr.clone())?,
                    serde_json::Value::Object(_) => to_table(vec![val.clone()])?,
                    _ => to_table(vec![val.clone()])?,
                }
            } else { (Vec::new(), Vec::new()) };
            if is_select_like && (!data.is_empty() || !cols.is_empty()) {
                for row in data.iter() { send_data_row(socket, row).await?; }
            }
            let tag = if upper.starts_with("SELECT") { format!("SELECT {}", data.len()) }
                else if upper.starts_with("CALCULATE") { let saved = match &val { serde_json::Value::Object(m) => m.get("saved").and_then(|v| v.as_u64()).unwrap_or(0), _ => 0 }; format!("CALCULATE {}", saved) }
                else if upper.starts_with("DELETE") { "DELETE".to_string() }
                else if upper.starts_with("UPDATE") { "UPDATE".to_string() }
                else if upper.starts_with("SCHEMA") || upper.starts_with("DATABASE") { format!("OK {}", data.len()) }
                else if upper.starts_with("SET") { "SET".to_string() }
                else if upper.starts_with("CREATE TABLE") { "CREATE TABLE".to_string() }
                else if data.is_empty() { "OK".to_string() } else { format!("OK {}", data.len()) };
            debug!(target: "pgwire", "Execute CommandComplete tag='{}'", tag);
            send_command_complete(socket, &tag).await?;
            if let Err(e) = socket.flush().await { error!(target: "pgwire", "flush after Execute failed: {}", e); }
        }
        Err(e) => { send_mapped_error(socket, &e).await?; state.in_error = true; }
    }
    Ok(())
}

async fn handle_close(socket: &mut tokio::net::TcpStream, state: &mut ConnState) -> Result<()> {
    let _len = read_u32(socket).await? as usize;
    let mut tag = [0u8;1]; socket.read_exact(&mut tag).await?;
    let name = read_cstring(socket).await?;
    debug!("pgwire close: type='{}', name='{}'", tag[0] as char, name);
    match tag[0] {
        b'S' => { state.statements.remove(&name); }
        b'P' => { state.portals.remove(&name); }
        _ => {}
    }
    send_close_complete(socket).await
}

#[cfg(test)]
mod tests;
