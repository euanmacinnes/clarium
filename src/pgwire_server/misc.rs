use anyhow::{anyhow, Result, bail};
use std::sync::atomic::AtomicU64;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use regex::Regex;
use std::collections::HashMap;
pub fn hex_dump_prefix(data: &[u8], max: usize) -> String {
    let take = data.len().min(max);
    data.iter().take(take).map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ")
}

pub const PG_TYPE_TEXT: i32 = 25; // use text for all columns for simplicity

pub static CONN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub async fn read_i16(socket: &mut tokio::net::TcpStream) -> Result<i16> { let mut b = [0u8;2]; socket.read_exact(&mut b).await?; Ok(i16::from_be_bytes(b)) }
pub async fn read_i32(socket: &mut tokio::net::TcpStream) -> Result<i32> { let mut b = [0u8;4]; socket.read_exact(&mut b).await?; Ok(i32::from_be_bytes(b)) }
pub async fn read_u32(socket: &mut tokio::net::TcpStream) -> Result<u32> {
    let mut b = [0u8; 4]; socket.read_exact(&mut b).await?; Ok(u32::from_be_bytes(b))
}

pub async fn write_i32(socket: &mut tokio::net::TcpStream, v: i32) -> Result<()> { socket.write_all(&v.to_be_bytes()).await.map_err(|e| e.into()) }
pub async fn read_cstring(socket: &mut tokio::net::TcpStream) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut byte = [0u8;1];
    loop {
        socket.read_exact(&mut byte).await?;
        if byte[0] == 0 { break; }
        buf.push(byte[0]);
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}


pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0, // extremely unlikely; avoid panic on clock skew
    }
}

pub fn normalize_object_to_db(name: &str) -> String {
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



pub async fn write_msg_header(socket: &mut tokio::net::TcpStream, tag: u8, len: i32) -> Result<()> {
    socket.write_all(&[tag]).await?; write_i32(socket, len).await
}

pub fn escape_sql_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() { if ch == '\'' { out.push('\''); out.push('\''); } else { out.push(ch); } }
    out.push('\'');
    out
}


pub fn substitute_placeholders(sql: &str, params: &[Option<String>]) -> Result<String> {
    substitute_placeholders_typed(sql, params, None)
}

pub fn substitute_placeholders_typed(sql: &str, params: &[Option<String>], param_types: Option<&[i32]>) -> Result<String> {
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
