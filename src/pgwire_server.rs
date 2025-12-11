//! Experimental pgwire server integration (feature-gated).
//! Minimal PostgreSQL wire-protocol handler supporting:
//! - Startup (no auth), simple query flow
//! - SELECT: delegates to existing query engine and streams rows
//! - INSERT: basic INSERT INTO <db>(col, ...) VALUES (...)

use anyhow::{anyhow, Result};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, debug, warn};
use crate::pgwire_server::encodedecode::*;
use crate::pgwire_server::inline::*;
use crate::pgwire_server::misc::*;
use crate::pgwire_server::oids::*;
use crate::pgwire_server::parse::*;
use crate::pgwire_server::security::*;
use crate::pgwire_server::send::*;
use crate::pgwire_server::structs::*;

use crate::tprintln;

use crate::{storage::SharedStore, server::exec};
use crate::identity::{AuthProvider, LocalAuthProvider, SessionManager, LoginRequest, RequestContext, Principal};
use crate::server::query::{self, Command};
use crate::server::exec::exec_select::handle_select;
use polars::prelude::AnyValue;
use crate::ident::{DEFAULT_DB, DEFAULT_SCHEMA};
use std::collections::HashMap;

pub mod encodedecode;
pub mod inline;
pub mod misc;
pub mod oids;
pub mod parse;
pub mod security;
pub mod send;
pub mod structs;



fn pgwire_trace_enabled() -> bool {
    std::env::var("CLARIUM_PGWIRE_TRACE").map(|v| {
        let s = v.to_lowercase();
        s == "1" || s == "true" || s == "yes" || s == "on"
    }).unwrap_or(false)
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
                let db_root = store.root_path().to_string_lossy().to_string();
                let provider = LocalAuthProvider::new(db_root, SessionManager::default());
                let lr = LoginRequest { username: user.clone(), password: password.clone(), db: None, ip: Some(peer.to_string()) };
                match provider.login(&lr) {
                    Ok(resp) => {
                        debug!(target: "pgwire", "conn_id={} login successful for user '{}' (sid={})", conn_id, user, resp.session.session_id);
                        // Initialize session state honoring dbname/database if provided
                        let db = params.get("database").cloned()
                            .or_else(|| params.get("dbname").cloned())
                            .unwrap_or_else(|| env_default_db());
                        let mut state = ConnState { current_database: db, current_schema: env_default_schema(), statements: HashMap::new(), portals: HashMap::new(), in_error: false, in_tx: false, principal: Some(resp.session.principal.clone()), session_token: Some(resp.session.token.clone()) };
                        send_auth_ok_and_params(socket, &params).await?;
                        run_query_loop(socket, &store, &user, &mut state, conn_id).await?;
                        return Ok(());
                    }
                    Err(e) => {
                        debug!(target: "pgwire", "conn_id={} authentication failed for user '{}' ({})", conn_id, user, e);
                        send_error(socket, "authentication failed").await?; 
                        return Ok(());
                    }
                }
            } else {
                debug!(target: "pgwire", "conn_id={} TRUST mode enabled via CLARIUM_PGWIRE_TRUST; skipping password auth for user '{}'", conn_id, user);
                // Initialize state without a principal (trust mode)
                send_auth_ok_and_params(socket, &params).await?;
                let db = params.get("database").cloned()
                    .or_else(|| params.get("dbname").cloned())
                    .unwrap_or_else(|| env_default_db());
                let mut state = ConnState { current_database: db, current_schema: env_default_schema(), statements: HashMap::new(), portals: HashMap::new(), in_error: false, in_tx: false, principal: None, session_token: None };
                run_query_loop(socket, &store, &user, &mut state, conn_id).await?;
                return Ok(());
            }
            // unreachable: handled above
            
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
            let db_root = store.root_path().to_string_lossy().to_string();
            let provider = LocalAuthProvider::new(db_root, SessionManager::default());
            let lr = LoginRequest { username: user.clone(), password: password.clone(), db: None, ip: Some(peer.to_string()) };
            match provider.login(&lr) {
                Ok(resp) => {
                    debug!(target: "pgwire", "conn_id={} login successful for user '{}' (sid={})", conn_id, user, resp.session.session_id);
                    send_auth_ok_and_params(socket, &params).await?;
                    let db = params.get("database").cloned()
                        .or_else(|| params.get("dbname").cloned())
                        .unwrap_or_else(|| env_default_db());
                    let mut state = ConnState { current_database: db, current_schema: env_default_schema(), statements: HashMap::new(), portals: HashMap::new(), in_error: false, in_tx: false, principal: Some(resp.session.principal.clone()), session_token: Some(resp.session.token.clone()) };
                    run_query_loop(socket, &store, &user, &mut state, conn_id).await?;
                    return Ok(());
                }
                Err(e) => {
                    debug!(target: "pgwire", "conn_id={} authentication failed for user '{}' ({})", conn_id, user, e);
                    send_error(socket, "authentication failed").await?; 
                    return Ok(());
                }
            }
        } else {
            debug!(target: "pgwire", "conn_id={} TRUST mode enabled via CLARIUM_PGWIRE_TRUST; skipping password auth for user '{}'", conn_id, user);
            send_auth_ok_and_params(socket, &params).await?;
            let db = params.get("database").cloned()
                .or_else(|| params.get("dbname").cloned())
                .unwrap_or_else(|| env_default_db());
            let mut state = ConnState { current_database: db, current_schema: env_default_schema(), statements: HashMap::new(), portals: HashMap::new(), in_error: false, in_tx: false, principal: None, session_token: None };
            run_query_loop(socket, &store, &user, &mut state, conn_id).await?;
            return Ok(());
        }
    }
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
        // Special-case SHOW CURRENT_USER for convenience
        if upper == "SHOW CURRENT_USER" || upper == "SELECT CURRENT_USER" {
            let cols = vec!["current_user".to_string()];
            let oids = vec![PG_TYPE_TEXT];
            send_row_description(socket, &cols, &oids).await?;
            let who = state.principal.as_ref().map(|p| p.user_id.clone()).unwrap_or_else(|| _username.to_string());
            send_data_row(socket, &vec![Some(who)]).await?;
            send_command_complete(socket, "SELECT 1").await?;
            continue;
        }
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
                    let ctx = RequestContext { principal: state.principal.clone(), request_id: None, database: Some(state.current_database.clone()), filestore: None };
                    match exec::execute_query_safe_with_ctx(store, &q_effective, &ctx).await {
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
            let ctx = RequestContext { principal: state.principal.clone(), request_id: None, database: Some(state.current_database.clone()), filestore: None };
            match exec::execute_query_safe_with_ctx(store, &q_effective, &ctx).await {
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
            1186 => { // interval: 16 bytes (microseconds i64, days i32, months i32)
                if bytes.len() != 16 { return None; }
                let micros = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
                let days = i32::from_be_bytes(bytes[8..12].try_into().ok()?);
                let months = i32::from_be_bytes(bytes[12..16].try_into().ok()?);
                // Compose a conservative ISO8601 duration string PnM nDTnS (seconds with fractional)
                let mut total_seconds = micros as f64 / 1_000_000f64;
                // We cannot reliably convert months to days without calendar; keep as months component
                // Build string parts
                let mut s = String::from("P");
                if months != 0 { s.push_str(&format!("{}M", months)); }
                if days != 0 { s.push_str(&format!("{}D", days)); }
                s.push('T');
                // keep seconds with up to 6 fractional digits
                if total_seconds < 0.0 { total_seconds = -total_seconds; s.insert(0, '-'); }
                s.push_str(&format!("{:.6}S", total_seconds));
                Some(s)
            }
            1700 => { // numeric/decimal (NUMERIC binary format)
                decode_pg_numeric_to_string(bytes)
            }
            // 1-D arrays for supported element types
            1000|1005|1007|1016|1021|1022|1009|1001|1182|1115|1185|1183 => {
                decode_pg_array_to_literal(bytes, oid)
            }
            _ => None,
        }
    }

    // Helper: parse a Postgres brace array text into vector of optional element strings (unescaped, without surrounding quotes).
    fn parse_brace_array_elements(txt: &str) -> Option<Vec<Option<String>>> {
        let s = txt.trim();
        if !s.starts_with('{') || !s.ends_with('}') { return None; }
        let inner = &s[1..s.len()-1];
        let mut out: Vec<Option<String>> = Vec::new();
        let mut cur = String::new();
        let mut in_q = false; let mut esc = false;
        for ch in inner.chars() {
            if in_q {
                if esc { cur.push(ch); esc = false; continue; }
                if ch == '\\' { esc = true; continue; }
                if ch == '"' { in_q = false; continue; }
                cur.push(ch);
            } else {
                match ch {
                    '"' => { in_q = true; }
                    ',' => {
                        let t = cur.trim();
                        if t.eq_ignore_ascii_case("NULL") { out.push(None); }
                        else if t.is_empty() { out.push(Some(String::new())); }
                        else { out.push(Some(t.to_string())); }
                        cur.clear();
                    }
                    _ => cur.push(ch),
                }
            }
        }
        let t = cur.trim();
        if t.eq_ignore_ascii_case("NULL") { out.push(None); }
        else if !t.is_empty() { out.push(Some(t.to_string())); }
        else if !inner.is_empty() { out.push(Some(String::new())); }
        Some(out)
    }

    // Helper: format an element (unescaped text value) for an element type OID into canonical array literal cell.
    // For text-like, quote and escape; for numeric/bool, validate and return bare; for NULL use bare NULL when input is None.
    fn format_array_element_for_oid(elem_oid: i32, val_opt: &Option<String>) -> Option<String> {
        use crate::pgwire_server::inline::anyvalue_to_opt_string; // not used directly but keep import style consistent
        match val_opt {
            None => Some("NULL".to_string()),
            Some(v) => {
                let t = v.trim();
                match elem_oid {
                    16 => { // bool
                        let low = t.to_ascii_lowercase();
                        if low == "t" || low == "true" { Some("true".to_string()) }
                        else if low == "f" || low == "false" { Some("false".to_string()) }
                        else { None }
                    }
                    21 | 23 | 20 => { // int2/int4/int8
                        if t.parse::<i64>().is_ok() { Some(t.to_string()) } else { None }
                    }
                    700 => { // float4
                        if t.parse::<f32>().is_ok() { Some(t.to_string()) } else { None }
                    }
                    701 | 1700 => { // float8 / numeric
                        if t.parse::<f64>().is_ok() { Some(t.to_string()) } else { None }
                    }
                    1082 | 1083 | 1114 | 1184 => {
                        // Temporal types: keep as quoted text; server side handles casts
                        let mut s = String::new(); s.push('"');
                        for ch in t.chars() { match ch { '"' => s.push_str("\\\""), '\\' => s.push_str("\\\\"), _ => s.push(ch) } }
                        s.push('"'); Some(s)
                    }
                    17 => { // bytea: expect either already in \x.. or plain; quote and preserve
                        let mut s = String::new(); s.push('"');
                        for ch in t.chars() { match ch { '"' => s.push_str("\\\""), '\\' => s.push_str("\\\\"), _ => s.push(ch) } }
                        s.push('"'); Some(s)
                    }
                    _ => { // text and others: quoted with escapes
                        let mut s = String::new(); s.push('"');
                        for ch in t.chars() { match ch { '"' => s.push_str("\\\""), '\\' => s.push_str("\\\\"), _ => s.push(ch) } }
                        s.push('"'); Some(s)
                    }
                }
            }
        }
    }

    // Helper: canonicalize array text parameters to Postgres brace form {..}; also normalize per element OID.
    fn canonicalize_array_text_param_with_oid(array_oid: i32, s: &str) -> Option<String> {
        // Convert ARRAY[...] to {..}
        let trimmed = s.trim();
        let base = if trimmed.is_empty() {
            "{}".to_string()
        } else if trimmed.starts_with('{') && trimmed.ends_with('}') {
            trimmed.to_string()
        } else {
            let up = trimmed.to_ascii_uppercase();
            if up.starts_with("ARRAY[") && trimmed.ends_with(']') {
                format!("{{{}}}", &trimmed[6..trimmed.len()-1])
            } else {
                // Not an array text
                return None;
            }
        };
        // Parse cells
        let elems = parse_brace_array_elements(&base)?;
        let elem_oid = crate::pgwire_server::inline::array_elem_oid(array_oid);
        let mut out_cells: Vec<String> = Vec::with_capacity(elems.len());
        for e in elems.iter() {
            if let Some(cell) = format_array_element_for_oid(elem_oid, e) { out_cells.push(cell); } else { return None; }
        }
        Some(format!("{{{}}}", out_cells.join(",")))
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
            let mut val = String::from_utf8_lossy(&bytes).into_owned();
            // If this parameter has an array OID, try to canonicalize to brace notation
            if let Some(oid) = stmt_param_types.get(pidx) {
                if crate::pgwire_server::inline::is_array_oid(*oid) {
                    // Try robust canonicalization and normalization per element OID first
                    if let Some(canon) = canonicalize_array_text_param_with_oid(*oid, &val) { val = canon; }
                    else if let Some(canon) = {
                        // Fallback to simpler canonicalization (ARRAY[...] -> {...})
                        let t = val.trim();
                        if t.starts_with('{') && t.ends_with('}') { Some(t.to_string()) }
                        else {
                            let up = t.to_ascii_uppercase();
                            if up.starts_with("ARRAY[") && t.ends_with(']') { Some(format!("{{{}}}", &t[6..t.len()-1])) } else { None }
                        }
                    } { val = canon; }
                }
            }
            params.push(Some(val));
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
                // Collect AnyValue per column; default to Null on error
                let mut avs: Vec<AnyValue> = Vec::with_capacity(ncols);
                for s in df.get_columns() {
                    match s.as_materialized_series().get(ridx) {
                        Ok(av) => avs.push(av),
                        Err(_) => avs.push(AnyValue::Null),
                    };
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
