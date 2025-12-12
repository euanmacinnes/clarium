use crate::server::query::query_common::*;
use crate::server::query::*;

fn is_ws(ch: char) -> bool { ch.is_whitespace() }

// Find the "AS" keyword position in a case-insensitive way, tolerating arbitrary
// whitespace/newlines around it, and ensuring token boundaries (ws before and after).
fn find_as_token(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        let c0 = bytes[i] as char;
        // require token boundary before
        let before_ok = if i == 0 { true } else { is_ws(bytes[i - 1] as char) };
        if (c0 == 'A' || c0 == 'a') && before_ok {
            let c1 = bytes[i + 1] as char;
            if c1 == 'S' || c1 == 's' {
                // after must be boundary as well
                let after_ok = if i + 2 >= bytes.len() { true } else { is_ws(bytes[i + 2] as char) };
                if after_ok { return Some(i); }
            }
        }
        i += 1;
    }
    None
}

pub fn parse_create(s: &str) -> Result<Command> {
    // CREATE DATABASE <db>
    // CREATE SCHEMA <db>/<schema> | <schema>
    // CREATE TIME TABLE <db>/<schema>/<table>.time
    // CREATE TABLE <db>/<schema>/<table> | <table>
    let rest = s[6..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("DATABASE ") {
        let mut name = rest[9..].trim();
        let mut if_not_exists = false;
        let up2 = name.to_uppercase();
        if up2.starts_with("IF NOT EXISTS ") { if_not_exists = true; name = &name["IF NOT EXISTS ".len()..]; }
        if name.trim().is_empty() { anyhow::bail!("Invalid CREATE DATABASE: missing database name"); }
        return Ok(Command::CreateDatabase { name: name.trim().to_string(), if_not_exists });
    }
    // CREATE MATCH VIEW <name> AS MATCH ...
    if up.starts_with("MATCH VIEW ") || up.starts_with("OR ALTER MATCH VIEW ") || up.starts_with("OR REPLACE MATCH VIEW ") {
        // Normalize optional OR ALTER
        let mut or_alter = false;
        let after = if up.starts_with("OR ALTER MATCH VIEW ") {
            or_alter = true;
            &rest["OR ALTER MATCH VIEW ".len()..]
        } else if up.starts_with("OR REPLACE MATCH VIEW ") {
            // treat OR REPLACE as not altering existing semantics here; execution layer can interpret
            &rest["OR REPLACE MATCH VIEW ".len()..]
        } else {
            &rest["MATCH VIEW ".len()..]
        };
        let after = after.trim();
        let as_pos = find_as_token(after).ok_or_else(|| anyhow::anyhow!("Invalid CREATE MATCH VIEW: expected AS"))?;
        let name = after[..as_pos].trim();
        // skip the 2-letter AS and any subsequent whitespace
        let mut k = as_pos + 2;
        while k < after.len() && is_ws(after.as_bytes()[k] as char) { k += 1; }
        let body = after[k..].trim();
        if name.is_empty() { anyhow::bail!("Invalid CREATE MATCH VIEW: missing view name"); }
        if body.is_empty() { anyhow::bail!("Invalid CREATE MATCH VIEW: missing MATCH definition after AS"); }
        // Body must start with MATCH ...
        let body_up = body.to_uppercase();
        if !body_up.starts_with("MATCH") { anyhow::bail!("Invalid CREATE MATCH VIEW: expected MATCH statement after AS"); }
        // Use the MATCH rewriter to get a SELECT definition
        match crate::server::query::parse_match(body) {
            Ok(crate::server::query::Command::MatchRewrite { sql }) => {
                let normalized_name = crate::ident::normalize_identifier(name);
                return Ok(Command::CreateView { name: normalized_name, or_alter, if_not_exists: false, definition_sql: sql });
            }
            Ok(other) => {
                anyhow::bail!("CREATE MATCH VIEW: internal error, expected MatchRewrite, got {:?}", other);
            }
            Err(e) => {
                anyhow::bail!("CREATE MATCH VIEW: failed to parse MATCH body: {}", e);
            }
        }
    }
    if up.starts_with("VIEW ") || up.starts_with("OR ALTER VIEW ") || up.starts_with("OR REPLACE VIEW ") {
        // CREATE [OR ALTER] VIEW [IF NOT EXISTS] <name> AS <SELECT...>
        // Capture the definition SQL verbatim after AS (can be SELECT or SELECT UNION)
        let mut or_alter = false;
        let mut after = if up.starts_with("OR ALTER VIEW ") {
            or_alter = true;
            &rest["OR ALTER VIEW ".len()..]
        } else if up.starts_with("OR REPLACE VIEW ") {
            // treat OR REPLACE similarly to plain CREATE; engine can interpret replace semantics
            &rest["OR REPLACE VIEW ".len()..]
        } else {
            &rest["VIEW ".len()..]
        };
        let mut if_not_exists = false;
        let mut a = after.trim();
        let a_up = a.to_uppercase();
        if a_up.starts_with("IF NOT EXISTS ") { if_not_exists = true; a = &a["IF NOT EXISTS ".len()..]; }
        let after = a.trim();
        // Split on AS (case-insensitive, token with whitespace boundaries)
        let as_pos = find_as_token(after).ok_or_else(|| anyhow::anyhow!("Invalid CREATE VIEW: expected AS"))?;
        let name = after[..as_pos].trim();
        // advance past AS + following whitespace
        let mut k = as_pos + 2;
        while k < after.len() && is_ws(after.as_bytes()[k] as char) { k += 1; }
        let def_sql = after[k..].trim();
        if name.is_empty() { anyhow::bail!("Invalid CREATE VIEW: missing view name"); }
        if def_sql.is_empty() { anyhow::bail!("Invalid CREATE VIEW: missing SELECT definition after AS"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::CreateView { name: normalized_name, or_alter, if_not_exists, definition_sql: def_sql.to_string() });
    }
    if up.starts_with("VECTOR INDEX ") {
        // CREATE VECTOR INDEX <name> ON <table>(<column>) USING hnsw [WITH (k=v, ...)]
        let after = &rest["VECTOR INDEX ".len()..];
        let after = after.trim();
        // name
        let (name_tok, mut i) = read_word(after, 0);
        if name_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing index name"); }
        let name_norm = crate::ident::normalize_identifier(&name_tok);
        i = skip_ws(after, i);
        let rem = &after[i..];
        let rem_up = rem.to_uppercase();
        if !rem_up.starts_with("ON ") { anyhow::bail!("Invalid CREATE VECTOR INDEX: expected ON <table>(<column>)"); }
        let mut j = 3; // after ON 
        // table name is everything up to the opening '(' (allowing qualified paths like db/schema/table)
        // Optional whitespace before '(' is tolerated
        let after_on = &rem[j..];
        let paren_pos = after_on.find('(').ok_or_else(|| anyhow::anyhow!("Invalid CREATE VECTOR INDEX: expected (column) after table name"))?;
        let table_tok = after_on[..paren_pos].trim();
        if table_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing table after ON"); }
        j = j + paren_pos + 1; // past '('
        // read column until ')'
        let mut col_end = j;
        while col_end < rem.len() && rem.as_bytes()[col_end] as char != ')' { col_end += 1; }
        if col_end >= rem.len() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing ')' after column"); }
        let column_tok = rem[j..col_end].trim();
        if column_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing column name"); }
        j = col_end + 1;
        j = skip_ws(rem, j);
        let rem2 = &rem[j..];
        let rem2_up = rem2.to_uppercase();
        if !rem2_up.starts_with("USING ") { anyhow::bail!("Invalid CREATE VECTOR INDEX: expected USING <algo>"); }
        let mut k = 6; // after USING 
        let (algo_tok, k2) = read_word(rem2, k);
        if algo_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing algorithm after USING"); }
        k = k2; k = skip_ws(rem2, k);
        let mut options: Vec<(String, String)> = Vec::new();
        if k < rem2.len() {
            let rem3 = &rem2[k..];
            let rem3_up = rem3.to_uppercase();
            if rem3_up.starts_with("WITH ") {
                let mut x = 5; // after WITH 
                x = skip_ws(rem3, x);
                if x >= rem3.len() || rem3.as_bytes()[x] as char != '(' { anyhow::bail!("Invalid CREATE VECTOR INDEX: expected WITH (k=v,...)"); }
                x += 1;
                // parse until closing ')'
                let mut buf = String::new();
                let mut depth = 1i32;
                let mut y = x;
                while y < rem3.len() {
                    let ch = rem3.as_bytes()[y] as char;
                    if ch == '(' { depth += 1; }
                    else if ch == ')' { depth -= 1; if depth == 0 { break; } }
                    buf.push(ch);
                    y += 1;
                }
                if depth != 0 { anyhow::bail!("Invalid CREATE VECTOR INDEX: unterminated WITH (...)"); }
                // split buf on commas into k=v pairs
                for part in buf.split(',') {
                    let p = part.trim(); if p.is_empty() { continue; }
                    if let Some(eq) = p.find('=') {
                        let k = p[..eq].trim().to_string();
                        let v = p[eq+1..].trim().trim_matches('\'').to_string();
                        options.push((k, v));
                    } else {
                        anyhow::bail!("Invalid option in WITH: expected k=v, got '{}'", p);
                    }
                }
            }
        }
        return Ok(Command::CreateVectorIndex { name: name_norm, table: crate::ident::normalize_identifier(&table_tok), column: column_tok.to_string(), algo: algo_tok.to_lowercase(), options });
    }
    if up.starts_with("GRAPH ") {
        // CREATE GRAPH <name> NODES (...) EDGES (...) [USING TABLES (nodes=..., edges=...)]
        let after = &rest["GRAPH ".len()..];
        let after = after.trim();
        let (name_tok, mut i) = read_word(after, 0);
        if name_tok.is_empty() { anyhow::bail!("Invalid CREATE GRAPH: missing name"); }
        i = skip_ws(after, i);
        let rem = &after[i..]; let rem_up = rem.to_uppercase();
        if !rem_up.starts_with("NODES ") { anyhow::bail!("Invalid CREATE GRAPH: expected NODES (...)"); }
        let mut j = 6; // after NODES 
        j = skip_ws(rem, j);
        if j >= rem.len() || rem.as_bytes()[j] as char != '(' { anyhow::bail!("Invalid CREATE GRAPH: expected '(' after NODES"); }
        j += 1; let start_nodes = j;
        let mut depth = 1i32;
        while j < rem.len() && depth > 0 {
            let ch = rem.as_bytes()[j] as char; if ch == '(' { depth += 1; } else if ch == ')' { depth -= 1; }
            j += 1;
        }
        if depth != 0 { anyhow::bail!("Invalid CREATE GRAPH: unterminated NODES(...)"); }
        let nodes_block = &rem[start_nodes..j-1];
        // parse nodes of form Label KEY(col)
        let mut nodes: Vec<(String, String)> = Vec::new();
        for part in nodes_block.split(',') { let p = part.trim(); if p.is_empty() { continue; }
            let up = p.to_uppercase();
            if let Some(kpos) = up.find(" KEY(") {
                let label = p[..kpos].trim();
                if let Some(rp) = p[kpos+5..].find(')') { let key = p[kpos+5..kpos+5+rp].trim(); nodes.push((label.to_string(), key.to_string())); } else { anyhow::bail!("Invalid NODES entry: expected KEY(...)"); }
            } else { anyhow::bail!("Invalid NODES entry: expected Label KEY(col)"); }
        }
        // After nodes, expect EDGES
        let rem2 = &rem[j..]; let rem2 = rem2.trim_start(); let rem2_up = rem2.to_uppercase();
        if !rem2_up.starts_with("EDGES ") { anyhow::bail!("Invalid CREATE GRAPH: expected EDGES (...)"); }
        let mut k = 6; k = skip_ws(rem2, k);
        if k >= rem2.len() || rem2.as_bytes()[k] as char != '(' { anyhow::bail!("Invalid CREATE GRAPH: expected '(' after EDGES"); }
        k += 1; let start_edges = k; let mut d2 = 1i32; while k < rem2.len() && d2 > 0 { let ch = rem2.as_bytes()[k] as char; if ch == '(' { d2 += 1; } else if ch == ')' { d2 -= 1; } k += 1; }
        if d2 != 0 { anyhow::bail!("Invalid CREATE GRAPH: unterminated EDGES(...)"); }
        let edges_block = &rem2[start_edges..k-1];
        // parse edges of form Type FROM A TO B
        let mut edges: Vec<(String, String, String)> = Vec::new();
        for part in edges_block.split(',') { let p = part.trim(); if p.is_empty() { continue; }
            let up = p.to_uppercase();
            if let Some(fp) = up.find(" FROM ") { if let Some(tp) = up[fp+6..].find(" TO ") {
                let et = p[..fp].trim();
                let from = p[fp+6..fp+6+tp].trim();
                let to = p[fp+6+tp+4..].trim();
                edges.push((et.to_string(), from.to_string(), to.to_string()));
            } else { anyhow::bail!("Invalid EDGES entry: expected FROM ... TO ..."); } } else { anyhow::bail!("Invalid EDGES entry: expected Type FROM A TO B"); }
        }
        // Optional USING clauses:
        // - USING TABLES (nodes=..., edges=...)
        // - USING GRAPHSTORE [CONFIG <name>] [WITH (k=v, ...)]
        let rem3 = &rem2[k..]; let rem3 = rem3.trim_start(); let rem3_up = rem3.to_uppercase();
        let mut nodes_table: Option<String> = None; let mut edges_table: Option<String> = None;
        let mut graph_engine: Option<String> = None; let mut graphstore_config: Option<String> = None; let mut graphstore_options: Option<Vec<(String, String)>> = None;

        if rem3_up.starts_with("USING ") {
            // Determine variant
            let after_using = &rem3[6..]; let after_using_up = after_using.to_uppercase();
            if after_using_up.starts_with("TABLES ") {
                let mut x = 7; x = skip_ws(after_using, x);
                if x >= after_using.len() || after_using.as_bytes()[x] as char != '(' { anyhow::bail!("Invalid USING TABLES: expected (nodes=..., edges=...)"); }
                x += 1; let mut buf = String::new(); let mut depth3 = 1i32; let mut y = x; while y < after_using.len() { let ch = after_using.as_bytes()[y] as char; if ch == '(' { depth3 += 1; } else if ch == ')' { depth3 -= 1; if depth3 == 0 { break; } } buf.push(ch); y += 1; }
                if depth3 != 0 { anyhow::bail!("Invalid USING TABLES: unterminated (...) block"); }
                for part in buf.split(',') { let p = part.trim(); if p.is_empty() { continue; }
                    if let Some(eq) = p.find('=') { let k = p[..eq].trim().to_lowercase(); let v = p[eq+1..].trim(); if k == "nodes" { nodes_table = Some(v.to_string()); } else if k == "edges" { edges_table = Some(v.to_string()); } }
                }
            } else if after_using_up.starts_with("GRAPHSTORE") {
                graph_engine = Some("graphstore".to_string());
                // advance past GRAPHSTORE
                let mut x = "GRAPHSTORE".len();
                // Optional CONFIG <name>
                x = skip_ws(after_using, x);
                let tail = &after_using[x..]; let tail_up = tail.to_uppercase();
                let mut consumed = 0usize;
                if tail_up.starts_with("CONFIG ") {
                    let c = 7; // after CONFIG 
                    let (cfg_name, c2) = read_word(tail, c);
                    if cfg_name.is_empty() { anyhow::bail!("Invalid USING GRAPHSTORE: expected config name after CONFIG"); }
                    graphstore_config = Some(cfg_name.to_string());
                    consumed = c2;
                }
                // Optional WITH (k=v,...)
                let tail2 = &tail[consumed..]; let tail2 = tail2.trim_start(); let tail2_up = tail2.to_uppercase();
                if tail2_up.starts_with("WITH ") {
                    let mut w = 5; w = skip_ws(tail2, w);
                    if w >= tail2.len() || tail2.as_bytes()[w] as char != '(' { anyhow::bail!("Invalid USING GRAPHSTORE WITH: expected WITH (k=v,...)"); }
                    w += 1; let mut buf = String::new(); let mut depth4 = 1i32; let mut y = w; while y < tail2.len() { let ch = tail2.as_bytes()[y] as char; if ch == '(' { depth4 += 1; } else if ch == ')' { depth4 -= 1; if depth4 == 0 { break; } } buf.push(ch); y += 1; }
                    if depth4 != 0 { anyhow::bail!("Invalid USING GRAPHSTORE WITH: unterminated (...) block"); }
                    let mut opts: Vec<(String, String)> = Vec::new();
                    for part in buf.split(',') { let p = part.trim(); if p.is_empty() { continue; }
                        if let Some(eq) = p.find('=') { let k = p[..eq].trim().to_string(); let v = p[eq+1..].trim().trim_matches('\'').to_string(); opts.push((k, v)); } else { anyhow::bail!("Invalid option in WITH: expected k=v, got '{}'", p); }
                    }
                    graphstore_options = Some(opts);
                }
            } else {
                // Unknown USING variant; be strict
                anyhow::bail!("Invalid CREATE GRAPH: expected USING TABLES (...) or USING GRAPHSTORE ...");
            }
        }

        return Ok(Command::CreateGraph {
            name: crate::ident::normalize_identifier(&name_tok),
            nodes,
            edges,
            nodes_table,
            edges_table,
            graph_engine,
            graphstore_config,
            graphstore_options,
        });
    }
    if up.starts_with("SCRIPT ") {
        // CREATE SCRIPT [SCALAR|AGGREGATE|TVF|PACKAGE] <db>/<schema>/<name> AS 'code'
        let after = &rest[7..];
        let parts: Vec<&str> = after.splitn(2, " AS ").collect();
        if parts.len() != 2 { anyhow::bail!("Invalid CREATE SCRIPT syntax. Use: CREATE SCRIPT [SCALAR|AGGREGATE|TVF|PACKAGE] <db>/<schema>/<name> AS '<code>'"); }
        let mut name_part = parts[0].trim();
        let code = parts[1].trim();
        // Optional kind prefix
        let mut kind: Option<crate::server::query::ScriptCreateKind> = None;
        let np_up = name_part.to_uppercase();
        for (kw, k) in [
            ("SCALAR", crate::server::query::ScriptCreateKind::Scalar),
            ("AGGREGATE", crate::server::query::ScriptCreateKind::Aggregate),
            ("TVF", crate::server::query::ScriptCreateKind::Tvf),
            ("PACKAGE", crate::server::query::ScriptCreateKind::Package),
        ] {
            if np_up.starts_with(kw) {
                name_part = name_part[kw.len()..].trim_start();
                kind = Some(k);
                break;
            }
        }
        // strip single quotes around code if present
        let code_s = if code.starts_with('\'') && code.ends_with('\'') && code.len() >= 2 { &code[1..code.len()-1] } else { code };
        if name_part.is_empty() { anyhow::bail!("Invalid CREATE SCRIPT: missing name"); }
        return Ok(Command::CreateScript { kind, path: name_part.to_string(), code: code_s.to_string() });
    }
    if up.starts_with("SCHEMA ") {
        let mut path = rest[7..].trim();
        let up2 = path.to_uppercase();
        let mut if_not_exists = false;
        if up2.starts_with("IF NOT EXISTS ") { if_not_exists = true; path = &path["IF NOT EXISTS ".len()..]; }
        if path.is_empty() { anyhow::bail!("Invalid CREATE SCHEMA: missing schema name"); }
        let normalized_path = crate::ident::normalize_identifier(path);
        return Ok(Command::CreateSchema { path: normalized_path, if_not_exists });
    }
    if up.starts_with("STORE ") {
        // CREATE STORE <db>.store.<store>
        let addr = rest[6..].trim();
        let (db, st) = parse_store_addr(addr)?;
        return Ok(Command::CreateStore { database: db, store: st });
    }
    if up.starts_with("TIME TABLE ") || up == "TIME TABLE" {
        let mut db = if up == "TIME TABLE" { "" } else { &rest[11..] };
        let mut if_not_exists = false;
        let db_up = db.to_uppercase();
        let mut t = db.trim();
        if db_up.starts_with("IF NOT EXISTS ") {
            if_not_exists = true;
            t = db["IF NOT EXISTS ".len()..].trim();
        }
        let table = t;
        if table.is_empty() { anyhow::bail!("Invalid CREATE TIME TABLE: missing time table name"); }
        if !table.ends_with(".time") { anyhow::bail!("CREATE TIME TABLE target must end with .time"); }
        // Prefer new variant while keeping legacy Command::DatabaseAdd path available elsewhere
        return Ok(Command::CreateTimeTable { table: table.to_string(), if_not_exists });
    }
    if up.starts_with("TABLE ") || up == "TABLE" {
        let arg0 = if up == "TABLE" { "" } else { &rest[6..] };
        let mut arg = arg0.trim();
        let mut if_not_exists = false;
        let arg_up = arg.to_uppercase();
        if arg_up.starts_with("IF NOT EXISTS ") { if_not_exists = true; arg = &arg["IF NOT EXISTS ".len()..]; }
        let t = arg.trim();
        if t.is_empty() { anyhow::bail!("Invalid CREATE TABLE: missing table name"); }
        // Split table name and optional clauses
        let mut parts = t.splitn(2, char::is_whitespace);
        let table_name = parts.next().unwrap().trim();
        if table_name.ends_with(".time") { anyhow::bail!("CREATE TABLE cannot target a .time table; use CREATE TIME TABLE"); }
        let mut primary_key: Option<Vec<String>> = None;
        let mut partitions: Option<Vec<String>> = None;
        if let Some(tail) = parts.next() {
            let tail_up = tail.to_uppercase();
            let parse_list = |s: &str| -> Vec<String> { s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect() };
            if let Some(i) = tail_up.find("PRIMARY KEY") {
                if let Some(p1) = tail[i..].find('(') { if let Some(p2) = tail[i+p1+1..].find(')') {
                    let start = i + p1 + 1; let end = i + p1 + 1 + p2; let cols = parse_list(&tail[start..end]); if !cols.is_empty() { primary_key = Some(cols); }
                }}
            }
            if let Some(i) = tail_up.find("PARTITION BY") {
                if let Some(p1) = tail[i..].find('(') { if let Some(p2) = tail[i+p1+1..].find(')') {
                    let start = i + p1 + 1; let end = i + p1 + 1 + p2; let cols = parse_list(&tail[start..end]); if !cols.is_empty() { partitions = Some(cols); }
                }}
            }
        }
        return Ok(Command::CreateTable { table: table_name.to_string(), primary_key, partitions, if_not_exists });
    }
    anyhow::bail!("Invalid CREATE syntax")
}
