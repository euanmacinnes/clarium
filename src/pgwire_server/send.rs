use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tracing::{error, debug};
use crate::pgwire_server::inline::*;
use crate::pgwire_server::misc::*;
use crate::pgwire_server::encodedecode::*;

use polars::prelude::{AnyValue, TimeUnit};

pub async fn send_row_description(socket: &mut tokio::net::TcpStream, cols: &[String], oids: &[i32]) -> Result<()> {
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

pub async fn send_data_row(socket: &mut tokio::net::TcpStream, row: &[Option<String>]) -> Result<()> {
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

pub async fn send_data_row_binary(socket: &mut tokio::net::TcpStream, anyvalues: &[AnyValue<'_>], oids: &[i32], fmts: &[i16]) -> Result<()> {
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
                // interval (1186): 16-byte struct -> microseconds (i64), days (i32), months (i32)
                (1186, AnyValue::Duration(val, unit)) => {
                    let micros: i64 = match unit {
                        TimeUnit::Nanoseconds => (*val) / 1000,
                        TimeUnit::Microseconds => *val,
                        TimeUnit::Milliseconds => (*val) * 1000,
                    };
                    let days: i32 = 0;
                    let months: i32 = 0;
                    payload.extend_from_slice(&16i32.to_be_bytes());
                    payload.extend_from_slice(&micros.to_be_bytes());
                    payload.extend_from_slice(&days.to_be_bytes());
                    payload.extend_from_slice(&months.to_be_bytes());
                }
                // date (1082): int32 days since PG epoch (2000-01-01)
                (1082, AnyValue::Date(days_since_unix)) => {
                    const DAYS_BETWEEN_UNIX_AND_PG_EPOCH: i32 = 10957; // 1970-01-01 -> 2000-01-01
                    let pg_days: i32 = days_since_unix - DAYS_BETWEEN_UNIX_AND_PG_EPOCH;
                    payload.extend_from_slice(&4i32.to_be_bytes());
                    payload.extend_from_slice(&pg_days.to_be_bytes());
                }
                // time (1083): int64 microseconds since midnight
                (1083, AnyValue::Time(nanos_since_midnight)) => {
                    let micros: i64 = nanos_since_midnight / 1000;
                    payload.extend_from_slice(&8i32.to_be_bytes());
                    payload.extend_from_slice(&micros.to_be_bytes());
                }
                // timestamp (1114) and timestamptz (1184): int64 microseconds since PG epoch (2000-01-01)
                (oid @ 1114, AnyValue::Datetime(val, unit, _)) | (oid @ 1184, AnyValue::Datetime(val, unit, _)) => {
                    let micros_since_unix: i64 = match unit {
                        TimeUnit::Nanoseconds => (*val) / 1000,
                        TimeUnit::Microseconds => *val,
                        TimeUnit::Milliseconds => (*val) * 1000,
                    };
                    const MICROS_BETWEEN_UNIX_AND_PG_EPOCH: i64 = 946_684_800_i64 * 1_000_000; // 1970->2000
                    let pg_micros: i64 = micros_since_unix - MICROS_BETWEEN_UNIX_AND_PG_EPOCH;
                    let _oid_check = oid; // silence unused pattern var warning
                    payload.extend_from_slice(&8i32.to_be_bytes());
                    payload.extend_from_slice(&pg_micros.to_be_bytes());
                }
                // numeric/decimal (1700): PostgreSQL NUMERIC binary format
                // We implement a conservative encoder from string/ints/floats.
                (1700, any) => {
                    if let Some(s) = anyvalue_to_opt_string(any) {
                        if let Some(bin) = encode_pg_numeric_from_str(&s) {
                            payload.extend_from_slice(&(bin.len() as i32).to_be_bytes());
                            payload.extend_from_slice(&bin);
                        } else {
                            let bytes = s.as_bytes();
                            payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            payload.extend_from_slice(bytes);
                        }
                    } else {
                        payload.extend_from_slice(&(-1i32).to_be_bytes());
                    }
                }
                // arrays: one-dimensional arrays for common element types
                (arr_oid, AnyValue::List(series)) if is_array_oid(arr_oid) => {
                    let inner_oid = array_elem_oid(arr_oid);
                    let n = series.len();
                    // Build array binary payload into a temporary vec, then prefix with its length
                    let mut arr = Vec::new();
                    // ndims (i32)
                    arr.extend_from_slice(&1i32.to_be_bytes());
                    // hasnull (i32)
                    let mut has_null = 0i32;
                    for i in 0..n { if series.get(i).map(|v| matches!(v, AnyValue::Null)).unwrap_or(true) { has_null = 1; break; } }
                    arr.extend_from_slice(&has_null.to_be_bytes());
                    // elemtype (i32)
                    arr.extend_from_slice(&inner_oid.to_be_bytes());
                    // dimensions: for 1-D: length and lower bound (1)
                    arr.extend_from_slice(&(n as i32).to_be_bytes());
                    arr.extend_from_slice(&1i32.to_be_bytes()); // lbound=1
                    // elements
                    for i in 0..n {
                        let cell = series.get(i).unwrap_or(AnyValue::Null);
                        if matches!(cell, AnyValue::Null) {
                            arr.extend_from_slice(&(-1i32).to_be_bytes());
                        } else {
                            // encode element in binary form according to inner_oid
                            encode_element_binary(&mut arr, inner_oid, &cell);
                        }
                    }
                    payload.extend_from_slice(&(arr.len() as i32).to_be_bytes());
                    payload.extend_from_slice(&arr);
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

pub async fn send_command_complete(socket: &mut tokio::net::TcpStream, tag: &str) -> Result<()> {
    socket.write_all(b"C").await?;
    let mut payload = Vec::new();
    payload.extend_from_slice(tag.as_bytes()); payload.push(0);
    let total_len = (payload.len() + 4) as i32;
    debug!(target: "pgwire", "CommandComplete tag='{}' payload_len={} total_frame_len={}", tag, payload.len(), total_len);
    write_i32(socket, total_len).await?;
    socket.write_all(&payload).await?;
    Ok(())
}

pub async fn send_error(socket: &mut tokio::net::TcpStream, msg: &str) -> Result<()> {
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

pub async fn send_parse_complete(socket: &mut tokio::net::TcpStream) -> Result<()> {
    debug!("pgwire: sending ParseComplete");
    socket.write_all(b"1").await?;
    write_i32(socket, 4).await
}

pub async fn send_bind_complete(socket: &mut tokio::net::TcpStream) -> Result<()> {
    debug!("pgwire: sending BindComplete");
    socket.write_all(b"2").await?;
    write_i32(socket, 4).await
}

pub async fn send_close_complete(socket: &mut tokio::net::TcpStream) -> Result<()> { socket.write_all(b"3").await?; write_i32(socket, 4).await }

pub async fn send_no_data(socket: &mut tokio::net::TcpStream) -> Result<()> {
    debug!(target: "pgwire", "sending NoData (len=4)");
    socket.write_all(b"n").await?; write_i32(socket, 4).await
}

pub async fn send_parameter_description(socket: &mut tokio::net::TcpStream, param_types: &[i32]) -> Result<()> {
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

// Helper: map AppError (when available) to richer pgwire ErrorResponse fields.
// Falls back to generic send_error for non-AppError cases.
pub async fn send_mapped_error(socket: &mut tokio::net::TcpStream, err: &anyhow::Error) -> Result<()> {
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

pub async fn send_ready_with_status(socket: &mut tokio::net::TcpStream, status: u8) -> Result<()> {
    debug!(target: "pgwire", "sending ReadyForQuery (status='{}')", status as char);
    crate::tprintln!("pgwire ReadyForQuery status='{}'", status as char);
    socket.write_all(b"Z").await?;
    write_i32(socket, 5).await?; // len
    socket.write_all(&[status]).await?; // 'I' idle, 'T' in-transaction, 'E' failed txn
    if let Err(e) = socket.flush().await { error!(target:"pgwire", "flush ReadyForQuery failed: {}", e); return Err(e.into()); }
    debug!(target: "pgwire", "ReadyForQuery flushed to client");
    Ok(())
}
