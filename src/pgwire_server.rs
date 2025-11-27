//! Experimental pgwire server integration (feature-gated).
//! Minimal PostgreSQL wire-protocol handler supporting:
//! - Startup (no auth), simple query flow
//! - SELECT: delegates to existing query engine and streams rows
//! - INSERT: basic INSERT INTO <db>(col, ...) VALUES (...)

use anyhow::{anyhow, Result, bail};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, debug};

use crate::{storage::{SharedStore, Record}, server::exec};
use regex::Regex;

const PG_TYPE_TEXT: i32 = 25; // use text for all columns for simplicity

static CONN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn pgwire_trace_enabled() -> bool {
    std::env::var("CLARIUM_PGWIRE_TRACE").map(|v| {
        let s = v.to_lowercase();
        s == "1" || s == "true" || s == "yes" || s == "on"
    }).unwrap_or(false)
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
}

async fn handle_conn(socket: &mut tokio::net::TcpStream, store: SharedStore, conn_id: u64, peer: &str) -> Result<()> {
    debug!(target: "pgwire", "conn_id={} new connection established from {}", conn_id, peer);
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
        debug!(target: "pgwire", "conn_id={} startup packet len={}, first={} bytes: {}", conn_id, len, buf.len().min(32), hex_dump_prefix(&buf, 32));
    } else {
        debug!(target: "pgwire", "conn_id={} received startup packet, len={}", conn_id, len);
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
                .unwrap_or_else(|| "clarium".to_string());
            let mut state = ConnState { current_database: db, current_schema: "public".to_string(), statements: HashMap::new(), portals: HashMap::new(), in_error: false };
            run_query_loop(socket, &store, &user, &mut state).await?;
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
            .unwrap_or_else(|| "clarium".to_string());
        let mut state = ConnState { current_database: db, current_schema: "public".to_string(), statements: HashMap::new(), portals: HashMap::new(), in_error: false };
        run_query_loop(socket, &store, &user, &mut state).await?;
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
    write_parameter(socket, "search_path", "\"$user\", public").await?;
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
    // ReadyForQuery
    send_ready(socket).await
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
async fn send_no_data(socket: &mut tokio::net::TcpStream) -> Result<()> { socket.write_all(b"n").await?; write_i32(socket, 4).await }
async fn send_parameter_description(socket: &mut tokio::net::TcpStream, param_types: &[i32]) -> Result<()> {
    socket.write_all(b"t").await?;
    let mut payload = Vec::new();
    let n = param_types.len() as i16;
    payload.extend_from_slice(&n.to_be_bytes());
    for oid in param_types { payload.extend_from_slice(&oid.to_be_bytes()); }
    write_i32(socket, (payload.len() as i32) + 4).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

async fn run_query_loop(socket: &mut tokio::net::TcpStream, store: &SharedStore, user: &str, state: &mut ConnState) -> Result<()> {
    debug!(target: "pgwire", "entering query loop for user '{}' (db='{}', schema='{}')", user, state.current_database, state.current_schema);
    loop {
        let mut tag = [0u8; 1];
        if socket.read_exact(&mut tag).await.is_err() { 
            debug!(target: "pgwire", "connection closed or read error, exiting query loop");
            break; 
        }
        debug!(target: "pgwire", "received message type byte={} (as char='{}')", tag[0], tag[0] as char);
        // Detect zero byte as potential connection closure (client side closed)
        if tag[0] == 0 {
            debug!(target: "pgwire", "received zero byte (likely connection closing), exiting query loop");
            break;
        }
        match tag[0] {
            b'Q' => {
                debug!(target: "pgwire", "handling simple Query message");
                let len = read_u32(socket).await?;
                let mut qbuf = vec![0u8; (len - 4) as usize];
                socket.read_exact(&mut qbuf).await?;
                if let Some(pos) = qbuf.iter().position(|&b| b == 0) { qbuf.truncate(pos); }
                let query_str = String::from_utf8(qbuf).unwrap_or_default();
                handle_query(socket, store, user, state, &query_str).await?;
            }
            b'P' => { // Parse
                debug!(target: "pgwire", "handling Parse message");
                handle_parse(socket, state).await?;
            }
            b'B' => { // Bind
                debug!(target: "pgwire", "handling Bind message");
                handle_bind(socket, state).await?;
            }
            b'D' => { // Describe
                debug!(target: "pgwire", "handling Describe message");
                handle_describe(socket, store, state).await?;
            }
            b'E' => { // Execute
                debug!(target: "pgwire", "handling Execute message");
                handle_execute(socket, store, user, state).await?;
            }
            b'H' => { // Flush
                debug!(target: "pgwire", "handling Flush message");
                // Flush pending output; per protocol, no response is sent for Flush itself
                if let Err(e) = socket.flush().await { error!("pgwire: flush error: {}", e); }
            }
            b'S' => { // Sync
                debug!(target: "pgwire", "handling Sync message");
                state.in_error = false;
                send_ready(socket).await?;
            }
            b'C' => { // Close
                debug!(target: "pgwire", "handling Close message");
                handle_close(socket, state).await?;
            }
            b'X' => { 
                debug!(target: "pgwire", "received Terminate message, closing connection");
                break; 
            }
            _ => { 
                // Unknown message: send ErrorResponse and enter error state; client should follow with Sync
                debug!(target: "pgwire", "unknown message type byte={} (as char='{}'), sending ErrorResponse and waiting for Sync", tag[0], tag[0] as char);
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
            }
        }
    }
    debug!(target: "pgwire", "exiting query loop for user '{}'", user);
    Ok(())
}

async fn send_ready(socket: &mut tokio::net::TcpStream) -> Result<()> {
    debug!(target: "pgwire", "sending ReadyForQuery (status='I')");
    socket.write_all(b"Z").await?;
    write_i32(socket, 5).await?; // len
    socket.write_all(b"I").await?; // idle
    socket.flush().await?;
    debug!(target: "pgwire", "ReadyForQuery flushed to client");
    Ok(())
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
    // Simple Query cycle: execute one SQL string and always finish with ReadyForQuery.
    // Keep pgwire thin: delegate all command handling to the common server exec.
    let q_trim = q.trim().trim_end_matches(';').trim();
    debug!("pgwire simple query: {}", q_trim);

    // Normalize with session defaults (db/schema)
    let q_effective = exec::normalize_query_with_defaults(q_trim, &state.current_database, &state.current_schema);

    // Delegate execution to server exec; it handles SET/USE/CREATE/SELECT/SHOW/etc.
    match exec::execute_query(store, &q_effective).await {
        Ok(val) => {
            let upper = q_trim.chars().take(16).collect::<String>().to_uppercase();
            // Convert JSON to a simple table for pgwire transport
            let (cols, data) = match &val {
                serde_json::Value::Array(arr) => to_table(arr.clone())?,
                serde_json::Value::Object(_) => to_table(vec![val.clone()])?,
                _ => to_table(vec![val.clone()])?,
            };
            if !data.is_empty() || !cols.is_empty() {
                send_row_description(socket, &cols).await?;
                for row in data.iter() { send_data_row(socket, row).await?; }
            }
            // Build a generic CommandComplete tag. Specific semantics are decided by server exec outputs.
            let tag = if upper.starts_with("SELECT") { format!("SELECT {}", data.len()) }
                else if upper.starts_with("CALCULATE") { let saved = match &val { serde_json::Value::Object(m) => m.get("saved").and_then(|v| v.as_u64()).unwrap_or(0), _ => 0 }; format!("CALCULATE {}", saved) }
                else if upper.starts_with("DELETE") { "DELETE".to_string() }
                else if upper.starts_with("SCHEMA") || upper.starts_with("DATABASE") { format!("OK {}", data.len()) }
                else if upper.starts_with("SET") { "SET".to_string() }
                else if upper.starts_with("CREATE TABLE") { "CREATE TABLE".to_string() }
                else if data.is_empty() { "OK".to_string() } else { format!("OK {}", data.len()) };
            send_command_complete(socket, &tag).await?;
        }
        Err(e) => {
            send_error(socket, &format!("{}", e)).await?;
            state.in_error = true;
        }
    }
    // Always finish simple query with ReadyForQuery
    send_ready(socket).await?;
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

async fn send_row_description(socket: &mut tokio::net::TcpStream, cols: &[String]) -> Result<()> {
    socket.write_all(b"T").await?;
    // Build payload
    let mut payload = Vec::new();
    let n: i16 = cols.len() as i16;
    payload.extend_from_slice(&n.to_be_bytes());
    for name in cols {
        payload.extend_from_slice(name.as_bytes()); payload.push(0); // field name
        payload.extend_from_slice(&0i32.to_be_bytes()); // table oid
        payload.extend_from_slice(&0i16.to_be_bytes()); // attr number
        payload.extend_from_slice(&PG_TYPE_TEXT.to_be_bytes()); // type oid
        payload.extend_from_slice(&(-1i16).to_be_bytes()); // type size (variable)
        payload.extend_from_slice(&0i32.to_be_bytes()); // type modifier
        payload.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    write_i32(socket, (payload.len() + 4) as i32).await?;
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
    write_i32(socket, (payload.len() + 4) as i32).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

async fn send_command_complete(socket: &mut tokio::net::TcpStream, tag: &str) -> Result<()> {
    socket.write_all(b"C").await?;
    let mut payload = Vec::new();
    payload.extend_from_slice(tag.as_bytes()); payload.push(0);
    write_i32(socket, (payload.len() + 4) as i32).await?;
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

async fn do_insert(store: &SharedStore, ins: InsertStmt) -> Result<usize> {
    // Build record(s) from values; support single VALUES row for now
    let mut sensors = serde_json::Map::new();
    let mut ts: Option<i64> = None;
    for (name, val) in ins.columns.iter().zip(ins.values.iter()) {
        if name == "_time" {
            ts = match val {
                InsertValue::Null => None,
                InsertValue::Number(n) => Some(*n),
                InsertValue::String(s) => s.parse::<i64>().ok(),
            };
        } else {
            let jv = match val {
                InsertValue::Null => serde_json::Value::Null,
                InsertValue::Number(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                InsertValue::String(s) => serde_json::Value::String(s.clone()),
            };
            sensors.insert(name.clone(), jv);
        }
    }
    let time_ms = ts.unwrap_or_else(now_ms);
    let rec = Record { _time: time_ms, sensors };
    let guard = store.0.lock();
    guard.write_records(&ins.database, &[rec])?;
    Ok(1)
}

fn now_ms() -> i64 { use std::time::{SystemTime, UNIX_EPOCH}; SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 }

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
    for _ in 0..n_params {
        let sz = r_i32(&buf, &mut i)?;
        if sz < 0 { params.push(None); }
        else {
            let bytes = r_bytes(&buf, &mut i, sz as usize)?;
            // Only text (format 0) supported; if any format=1, reject after reading all
            let s = String::from_utf8_lossy(&bytes).into_owned();
            params.push(Some(s));
        }
    }

    // result-column formats
    let n_rfmts = r_i16(&buf, &mut i)? as usize;
    let mut result_formats: Vec<i16> = Vec::with_capacity(n_rfmts);
    for _ in 0..n_rfmts { result_formats.push(r_i16(&buf, &mut i)?); }

    // Validate formats: only text=0 allowed
    if param_formats.contains(&1) || result_formats.contains(&1) {
        send_error(socket, "binary formats are not supported").await?;
        state.in_error = true;
        return Ok(());
    }

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
    // Detect named placeholders of the form %(name)s
    let re_named = Regex::new(r"%\(([A-Za-z0-9_]+)\)s").unwrap();
    let mut out = String::new();
    if re_named.is_match(sql) {
        // Collect order of first occurrence per name
        let mut order: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for cap in re_named.captures_iter(sql) {
            let name = cap.get(1).unwrap().as_str().to_string();
            if !seen.contains(&name) { seen.insert(name.clone()); order.push(name); }
        }
        if order.len() != params.len() { bail!("mismatch parameter count: expected {} got {}", order.len(), params.len()); }
        let mut map: HashMap<String, Option<String>> = HashMap::new();
        for (i, name) in order.iter().enumerate() { map.insert(name.clone(), params[i].clone()); }
        // Replace all occurrences
        let mut last = 0usize;
        for m in re_named.find_iter(sql) {
            out.push_str(&sql[last..m.start()]);
            let name = re_named.captures(m.as_str()).unwrap().get(1).unwrap().as_str();
            match map.get(name).and_then(|v| v.clone()) {
                None => out.push_str("NULL"),
                Some(val) => out.push_str(&escape_sql_literal(&val)),
            }
            last = m.end();
        }
        out.push_str(&sql[last..]);
        return Ok(out);
    }

    // $n style placeholders (from extended protocol)
    let re_dollar = Regex::new(r"\$([1-9][0-9]*)").unwrap();
    if re_dollar.is_match(sql) {
        let mut last = 0usize;
        for m in re_dollar.find_iter(sql) {
            out.push_str(&sql[last..m.start()]);
            let cap = re_dollar.captures(m.as_str()).unwrap();
            let idx: usize = cap.get(1).unwrap().as_str().parse::<usize>().unwrap();
            let pos = idx.checked_sub(1).ok_or_else(|| anyhow!("parameter index underflow"))?;
            if pos >= params.len() { bail!("too few parameters: ${} referenced but only {} provided", idx, params.len()); }
            match &params[pos] {
                None => out.push_str("NULL"),
                Some(v) => out.push_str(&escape_sql_literal(v)),
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
    // Attempt to get column names by executing with LIMIT 0
    let q = sql.trim();
    let up = q.to_uppercase();
    if up.starts_with("SELECT") || up.starts_with("WITH ") {
        // Prefer parsing and executing via DataFrame path to preserve column order
        let q_eff = exec::normalize_query_with_defaults(q, &state.current_database, &state.current_schema);
        if let Ok(crate::query::Command::Select(qo)) = crate::query::parse(&q_eff) {
            match exec::execute_select_df(store, &qo) {
                Ok(df) => {
                    let cols: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();
                    if cols.is_empty() { return send_no_data(socket).await; }
                    return send_row_description(socket, &cols).await;
                }
                Err(_) => { return send_no_data(socket).await; }
            }
        } else {
            return send_no_data(socket).await;
        }
    } else {
        send_no_data(socket).await
    }
}

async fn handle_describe(socket: &mut tokio::net::TcpStream, store: &SharedStore, state: &mut ConnState) -> Result<()> {
    let _len = read_u32(socket).await? as usize;
    let mut tag = [0u8;1]; socket.read_exact(&mut tag).await?;
    let name = read_cstring(socket).await?;
    debug!("pgwire describe: type='{}', name='{}'", tag[0] as char, name);
    match tag[0] {
        b'S' => {
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
            // portal: find its stmt
            if let Some(portal) = state.portals.get(&name) {
                if let Some(stmt) = state.statements.get(&portal.stmt_name) {
                    // Perform substitution to allow parser to see final forms (aliases, literal exprs)
                    let sql_eff = match substitute_placeholders(&stmt.sql, &portal.params) { Ok(s) => s, Err(_) => stmt.sql.clone() };
                    // ParameterDescription is optional for portal Describe; many servers send only RowDescription
                    describe_row_description(socket, store, state, &sql_eff).await
                } else { send_no_data(socket).await }
            } else { send_no_data(socket).await }
        }
        _ => send_no_data(socket).await,
    }
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
    let substituted = match substitute_placeholders(&stmt.sql, &portal.params) { Ok(s) => s, Err(e) => { send_error(socket, &format!("{}", e)).await?; state.in_error = true; return Ok(()); } };
    let q_trim = substituted.trim().trim_end_matches(';').trim();
    debug!("pgwire execute (portal='{}'): {}", portal_name, q_trim);
    let q_effective = exec::normalize_query_with_defaults(q_trim, &state.current_database, &state.current_schema);

    // Delegate execution to common server executor
    match exec::execute_query(store, &q_effective).await {
        Ok(val) => {
            let upper = q_trim.chars().take(16).collect::<String>().to_uppercase();
            // Convert JSON result to a simple table for transport
            let (cols, data) = match &val {
                serde_json::Value::Array(arr) => to_table(arr.clone())?,
                serde_json::Value::Object(_) => to_table(vec![val.clone()])?,
                _ => to_table(vec![val.clone()])?,
            };
            if !data.is_empty() || !cols.is_empty() {
                send_row_description(socket, &cols).await?;
                for row in data.iter() { send_data_row(socket, row).await?; }
            }
            // Build a generic CommandComplete tag consistent with simple query
            let tag = if upper.starts_with("SELECT") { format!("SELECT {}", data.len()) }
                else if upper.starts_with("CALCULATE") { let saved = match &val { serde_json::Value::Object(m) => m.get("saved").and_then(|v| v.as_u64()).unwrap_or(0), _ => 0 }; format!("CALCULATE {}", saved) }
                else if upper.starts_with("DELETE") { "DELETE".to_string() }
                else if upper.starts_with("SCHEMA") || upper.starts_with("DATABASE") { format!("OK {}", data.len()) }
                else if upper.starts_with("SET") { "SET".to_string() }
                else if upper.starts_with("CREATE TABLE") { "CREATE TABLE".to_string() }
                else if data.is_empty() { "OK".to_string() } else { format!("OK {}", data.len()) };
            send_command_complete(socket, &tag).await?;
        }
        Err(e) => { send_error(socket, &format!("{}", e)).await?; state.in_error = true; }
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
