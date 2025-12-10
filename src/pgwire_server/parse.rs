pub fn parse_insert(q: &str) -> Option<InsertStmt> {
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

pub fn parse_value(s: &str) -> InsertValue {
    if s.eq_ignore_ascii_case("null") { return InsertValue::Null; }
    if let Ok(n) = s.parse::<i64>() { return InsertValue::Number(n); }
    // If it came from quotes, we should have had only inner content without quotes.
    // But if user wrote unquoted text, treat it as string.
    InsertValue::String(s.trim_matches('\'').to_string())
}

// Extended protocol handlers and helpers

pub async fn handle_parse(socket: &mut tokio::net::TcpStream, state: &mut ConnState) -> Result<()> {
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


pub fn parse_startup_params(payload: &[u8]) -> std::collections::HashMap<String, String> {
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
