use crate::server::query::query_common::*;
use crate::server::query::*;



pub fn parse_use(s: &str) -> Result<Command> {
    let rest = s[3..].trim(); // after USE
    let up = rest.to_uppercase();
    if up.starts_with("DATABASE ") {
        let name = rest[9..].trim();
        if name.is_empty() { anyhow::bail!("USE DATABASE: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::UseDatabase { name: normalized_name });
    }
    if up.starts_with("SCHEMA ") {
        let name = rest[7..].trim();
        if name.is_empty() { anyhow::bail!("USE SCHEMA: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::UseSchema { name: normalized_name });
    }
    anyhow::bail!("Unsupported USE command")
}

pub fn parse_set(s: &str) -> Result<Command> {
    // SET variable TO value | SET variable = value
    let rest = s[3..].trim(); // after SET
    // Split by TO or = (case-insensitive for TO)
    let up = rest.to_uppercase();
    let (variable, value) = if let Some(pos) = up.find(" TO ") {
        let var = rest[..pos].trim();
        let val = rest[pos + 4..].trim();
        (var, val)
    } else if let Some(pos) = rest.find('=') {
        let var = rest[..pos].trim();
        let val = rest[pos + 1..].trim();
        (var, val)
    } else {
        anyhow::bail!("Invalid SET syntax. Use: SET variable TO value or SET variable = value");
    };
    
    if variable.is_empty() { anyhow::bail!("SET: missing variable name"); }
    if value.is_empty() { anyhow::bail!("SET: missing value"); }
    
    // Strip quotes from value if present
    let value_clean = if (value.starts_with('\'') && value.ends_with('\'')) || (value.starts_with('"') && value.ends_with('"')) {
        if value.len() >= 2 { &value[1..value.len()-1] } else { value }
    } else {
        value
    };
    
    Ok(Command::Set { 
        variable: variable.to_string(), 
        value: value_clean.to_string() 
    })
}




pub fn parse_manual_cell(tok: &str) -> ManualLabel {
    let t = tok.trim();
    if t.is_empty() { return ManualLabel{ name: None, value: None }; }
    if let Some(pos) = t.find(":=") {
        let name = t[..pos].trim().trim_matches(['"','\'']).to_string();
        let val_raw = t[pos+2..].trim();
        let v = if val_raw.eq_ignore_ascii_case("NULL") { None } else if (val_raw.starts_with('\'') && val_raw.ends_with('\'')) || (val_raw.starts_with('"') && val_raw.ends_with('"')) { Some(val_raw[1..val_raw.len()-1].to_string()) } else { Some(val_raw.to_string()) };
        ManualLabel{ name: if name.is_empty() { None } else { Some(name) }, value: v }
    } else {
        let v = if t.eq_ignore_ascii_case("NULL") { None } else if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) { Some(t[1..t.len()-1].to_string()) } else { Some(t.to_string()) };
        ManualLabel{ name: None, value: v }
    }
}

pub fn parse_date_token_to_ms(tok: &str) -> Result<i64> {
    let t = tok.trim();
    if let Some(ms) = parse_iso8601_to_ms(t) { return Ok(ms); }
    // numeric milliseconds
    if let Ok(n) = t.parse::<i64>() { return Ok(n); }
    anyhow::bail!("Invalid date token in manual SLICE row: {}", tok)
}




// --- KV STORE/KEY parsing helpers ---
pub fn parse_store_addr(addr: &str) -> Result<(String, String)> {
    // Expect <database>.store.<store>
    let parts: Vec<&str> = addr.split('.').collect();
    if parts.len() != 3 { anyhow::bail!(format!("Invalid store address '{}'. Expected <database>.store.<store>", addr)); }
    if parts[1].to_lowercase() != "store" { anyhow::bail!("Invalid store address: missing literal 'store' segment"); }
    let db = parts[0].trim();
    let store = parts[2].trim();
    if db.is_empty() || store.is_empty() { anyhow::bail!("Invalid store address: empty database or store name"); }
    Ok((db.to_string(), store.to_string()))
}

pub fn parse_key_in_clause(rest: &str) -> Result<(String, String, String)> {
    // Expect: KEY <key> IN <database>.store.<store>
    let up = rest.to_uppercase();
    if !up.starts_with("KEY ") { anyhow::bail!("Invalid syntax: expected KEY <name> IN <database>.store.<store>"); }
    let after_key = &rest[4..];
    let parts: Vec<&str> = after_key.splitn(2, " IN ").collect();
    if parts.len() != 2 { anyhow::bail!("Invalid KEY syntax: expected 'IN <database>.store.<store>'"); }
    let key = parts[0].trim();
    let store_addr = parts[1].trim();
    if key.is_empty() { anyhow::bail!("Invalid KEY syntax: missing key name"); }
    let (db, store) = parse_store_addr(store_addr)?;
    Ok((db, store, key.to_string()))
}

pub fn parse_read(s: &str) -> Result<Command> {
    // READ KEY <key> IN <database>.store.<store>
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("KEY ") {
        let (db, store, key) = parse_key_in_clause(rest)?;
        return Ok(Command::ReadKey { database: db, store, key });
    }
    anyhow::bail!("Invalid READ syntax")
}

pub fn parse_list(s: &str) -> Result<Command> {
    // LIST STORES <db>
    // LIST KEYS IN <database>.store.<store>
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("STORES ") {
        let db = rest[7..].trim();
        if db.is_empty() { anyhow::bail!("Invalid LIST STORES: missing database name"); }
        return Ok(Command::ListStores { database: db.to_string() });
    }
    if up == "STORES" {
        anyhow::bail!("Invalid LIST STORES: missing database name");
    }
    if up.starts_with("KEYS ") {
        let after = &rest[5..];
        let up2 = after.to_uppercase();
        if let Some(i) = up2.find(" IN ") {
            let addr = after[i+4..].trim();
            let (db, store) = parse_store_addr(addr)?;
            return Ok(Command::ListKeys { database: db, store });
        } else {
            anyhow::bail!("Invalid LIST KEYS syntax: expected 'LIST KEYS IN <database>.store.<store>'");
        }
    }
    anyhow::bail!("Invalid LIST syntax")
}

pub fn parse_describe(s: &str) -> Result<Command> {
    // DESCRIBE KEY <key> IN <database>.store.<store>
    // or: DESCRIBE <object>
    let rest = s[9..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("KEY ") {
        let (db, store, key) = parse_key_in_clause(rest)?;
        return Ok(Command::DescribeKey { database: db, store, key });
    }
    // Fallback: treat the remainder as an object identifier (table or view)
    if rest.is_empty() { anyhow::bail!("Invalid DESCRIBE syntax: missing object name"); }
    // Keep the name as provided; qualification is applied at execution time
    Ok(Command::DescribeObject { name: rest.to_string() })
}

pub fn parse_write(s: &str) -> Result<Command> {
    // WRITE KEY <key> IN <database>.store.<store> = <value_or_address> [TTL <duration>] [RESET ON ACCESS|NO RESET]
    let rest = s[5..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("KEY ") {
        // split around '=' first
        let eq_pos = rest.find('=');
        if eq_pos.is_none() { anyhow::bail!("Invalid WRITE KEY: missing '=' assignment"); }
        let eq_pos = eq_pos.unwrap();
        let left = rest[..eq_pos].trim();
        let right_all = rest[eq_pos+1..].trim();
        let (db, store, key) = parse_key_in_clause(left)?;
        // Extract optional TTL/RESET flags from right-hand side tail
        // We'll split value and options by looking for ' TTL ' or ' RESET '
        let mut ttl_ms: Option<i64> = None;
        let mut reset_on_access: Option<bool> = None;
        let mut value_str = right_all.to_string();
        // Normalize spaces for matching
        let up_right = right_all.to_uppercase();
        let mut opt_start = up_right.len();
        if let Some(i) = up_right.find(" TTL ") { opt_start = opt_start.min(i); }
        if let Some(i) = up_right.find(" RESET ") { opt_start = opt_start.min(i); }
        if opt_start < up_right.len() {
            value_str = right_all[..opt_start].trim().to_string();
            let opts = right_all[opt_start..].trim();
            let up_opts = opts.to_uppercase();
            // TTL
            if let Some(i) = up_opts.find("TTL ") {
                let after = &opts[i+4..];
                let token = after.split_whitespace().next().unwrap_or("");
                if token.is_empty() { anyhow::bail!("Invalid TTL: missing duration (e.g., 10s, 5m)"); }
                let ms = parse_window(token)?;
                ttl_ms = Some(ms);
            }
            // RESET flags
            if up_opts.contains("RESET ON ACCESS") { reset_on_access = Some(true); }
            else if up_opts.contains("NO RESET") { reset_on_access = Some(false); }
        } else {
            value_str = right_all.to_string();
        }
        return Ok(Command::WriteKey { database: db, store, key, value: value_str, ttl_ms, reset_on_access });
    }
    anyhow::bail!("Invalid WRITE syntax")
}


pub fn parse_window(s: &str) -> Result<i64> {
    // e.g. 1s, 5m, 1h
    let re = Regex::new(r"^(?i)(\d+)(ms|s|m|h|d)$")?;
    let caps = re.captures(s.trim()).ok_or_else(|| anyhow::anyhow!("Invalid window: {}", s))?;
    let n: i64 = caps.get(1).unwrap().as_str().parse()?;
    let unit = caps.get(2).unwrap().as_str().to_lowercase();
    let ms = match unit.as_str() {
        "ms" => n,
        "s" => n * 1000,
        "m" => n * 60_000,
        "h" => n * 3_600_000,
        "d" => n * 86_400_000,
        _ => n,
    };
    Ok(ms)
}




// Parse a PostgreSQL type name from the given source substring, returning the parsed SqlType and
// the number of bytes consumed. Supports multi-word names (e.g., "double precision"), schema-qualified
// names (e.g., pg_catalog.regclass), and optional parameters: varchar(10), numeric(10,2).
pub fn parse_pg_type_keyword(s: &str) -> Option<(&str, usize)> {
    // Consume identifier tokens and spaces, stopping before '(' or other delimiter
    let bytes = s.as_bytes();
    let mut i = 0usize;
    // Collect up to two words for multi-word types like "double precision" or "character varying"
    let mut words: Vec<String> = Vec::new();
    while i < bytes.len() {
        // skip spaces
        while i < bytes.len() && bytes[i].is_ascii_whitespace() { i += 1; }
        if i >= bytes.len() { break; }
        // if next is '(', stop - parameters handled separately
        let ch = bytes[i] as char;
        if ch == '(' { break; }
        // read an identifier token (letters, digits, underscore, dot)
        let start = i;
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' { i += 1; } else { break; }
        }
        if start == i { break; }
        words.push(s[start..i].to_string());
        // lookahead for another word (for multi-word type names)
        let mut j = i; while j < bytes.len() && bytes[j].is_ascii_whitespace() { j += 1; }
        if j < bytes.len() {
            let c2 = bytes[j] as char;
            if c2.is_ascii_alphabetic() { i = j; continue; }
        }
        // otherwise stop
        break;
    }
    if words.is_empty() { return None; }
    let consumed = i;
    let name = words.join(" ");
    Some((Box::leak(name.into_boxed_str()), consumed))
}

pub fn parse_type_name(s: &str) -> Option<(SqlType, usize)> {
    let s_trim = s;
    let (kw, mut consumed_kw) = parse_pg_type_keyword(s_trim)?;
    let kw_lc = kw.to_ascii_lowercase();
    // Default: no params
    let mut rest = &s_trim[consumed_kw..];
    // Parse optional ( ... ) parameters
    let mut params: Option<Vec<i32>> = None;
    {
        let mut k = 0usize; while k < rest.len() && rest.as_bytes()[k].is_ascii_whitespace() { k += 1; }
        if k < rest.len() && rest.as_bytes()[k] as char == '(' {
            // find closing ')'
            let mut j = k + 1; let bytes = rest.as_bytes(); let mut buf = String::new(); let mut parts = Vec::new();
            while j < rest.len() {
                let ch = bytes[j] as char; j += 1;
                if ch == ')' { break; }
                buf.push(ch);
            }
            if !buf.is_empty() {
                for p in buf.split(',') { if let Ok(v) = p.trim().parse::<i32>() { parts.push(v); } }
                if !parts.is_empty() { params = Some(parts); }
            }
            consumed_kw += j; // include ')'
        }
    }

    let ty = match kw_lc.as_str() {
        "bool" | "boolean" => SqlType::Boolean,
        "smallint" | "int2" => SqlType::SmallInt,
        "int" | "integer" | "int4" => SqlType::Integer,
        "bigint" | "int8" => SqlType::BigInt,
        "real" | "float4" => SqlType::Real,
        "double precision" | "float8" => SqlType::Double,
        "text" => SqlType::Text,
        // character types
        "varchar" | "character varying" => SqlType::Varchar(params.as_ref().and_then(|v| v.get(0).cloned())),
        "character" | "char" | "bpchar" => SqlType::Char(params.as_ref().and_then(|v| v.get(0).cloned())),
        // binary and JSON-like
        "bytea" => SqlType::Bytea,
        "uuid" => SqlType::Uuid,
        "json" => SqlType::Json,
        "jsonb" => SqlType::Jsonb,
        "varchar" | "character varying" => SqlType::Varchar(params.as_ref().and_then(|v| v.get(0).cloned())),
        "date" => SqlType::Date,
        "timestamp" | "timestamp without time zone" => SqlType::Timestamp,
        "timestamptz" | "timestamp with time zone" => SqlType::TimestampTz,
        // time of day (without/with time zone)
        "time" | "time without time zone" => SqlType::Time,
        "timetz" | "time with time zone" => SqlType::TimeTz,
        // interval duration
        "interval" => SqlType::Interval,
        "numeric" | "decimal" => {
            let ps = params.as_ref().and_then(|v| {
                if v.len() == 2 { Some((v[0], v[1])) } else if v.len() == 1 { Some((v[0], 0)) } else { None }
            });
            SqlType::Numeric(ps)
        },
        // Schema-qualified regclass (e.g., pg_catalog.regclass) or bare regclass
        x if x.ends_with(".regclass") || x == "regclass" => SqlType::Regclass,
        // Schema-qualified regtype (e.g., pg_catalog.regtype) or bare regtype
        x if x.ends_with(".regtype") || x == "regtype" => SqlType::Regtype,
        _ => return None,
    };
    Some((ty, consumed_kw))
}

