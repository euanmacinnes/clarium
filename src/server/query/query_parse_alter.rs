use anyhow::{anyhow, Result};

use crate::server::query::{Command, AlterOp};

fn normalize_ident(name: &str) -> String {
    let qd = crate::system::current_query_defaults();
    crate::ident::qualify_regular_ident(name, &qd)
}

fn sql_type_to_key(ty: &str) -> String {
    let t = ty.to_ascii_lowercase();
    if t.contains("char") || t.contains("text") || t.contains("json") || t.contains("bool") { "string".to_string() }
    else if t.contains("int") { "int64".to_string() }
    else if t.contains("double") || t.contains("real") || t.contains("float") || t.contains("numeric") || t.contains("decimal") { "float64".to_string() }
    else if t.contains("time") || t.contains("date") { "int64".to_string() }
    else { "string".to_string() }
}

/// Parse comma-separated operations inside an ALTER TABLE statement tail
fn parse_ops(s: &str) -> Result<Vec<AlterOp>> {
    let mut ops: Vec<AlterOp> = Vec::new();
    // Split by commas not inside parentheses
    let mut cur = String::new();
    let mut depth: i32 = 0;
    for ch in s.chars() {
        match ch {
            '(' => { depth += 1; cur.push(ch); }
            ')' => { depth -= 1; cur.push(ch); }
            ',' if depth == 0 => { if !cur.trim().is_empty() { ops.push(parse_one_op(cur.trim())?); } cur.clear(); }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() { ops.push(parse_one_op(cur.trim())?); }
    Ok(ops)
}

fn parse_one_op(s: &str) -> Result<AlterOp> {
    let up = s.to_ascii_uppercase();
    if up.starts_with("ADD COLUMN ") { return parse_add_column(&s["ADD COLUMN ".len()..]); }
    if up.starts_with("ADD PRIMARY KEY") {
        // ADD PRIMARY KEY (col[, ...])
        let start = s.find('(').ok_or_else(|| anyhow!("ADD PRIMARY KEY expects column list"))?;
        let end = s.rfind(')').ok_or_else(|| anyhow!("ADD PRIMARY KEY expects closing )"))?;
        let inside = &s[start+1..end];
        let cols: Vec<String> = inside.split(',').map(|x| x.trim().trim_matches('"').to_string()).filter(|x| !x.is_empty()).collect();
        return Ok(AlterOp::AddPrimaryKey { columns: cols });
    }
    if up.starts_with("DROP PRIMARY KEY") { return Ok(AlterOp::DropPrimaryKey); }
    if up.starts_with("RENAME COLUMN ") {
        // RENAME COLUMN <old> TO <new>
        let rest = &s["RENAME COLUMN ".len()..];
        let parts: Vec<&str> = rest.splitn(2, " TO ").collect();
        if parts.len() != 2 { return Err(anyhow!("Invalid RENAME COLUMN syntax")); }
        let from = parts[0].trim().trim_matches('"').to_string();
        let to = parts[1].trim().trim_matches('"').to_string();
        return Ok(AlterOp::RenameColumn { from, to });
    }
    if up.starts_with("ALTER COLUMN ") {
        // ALTER COLUMN <name> TYPE <type>
        let rest = &s["ALTER COLUMN ".len()..];
        let rup = rest.to_ascii_uppercase();
        if let Some(pos) = rup.find(" TYPE ") {
            let name = rest[..pos].trim().trim_matches('"').to_string();
            let ty = rest[pos+" TYPE ".len()..].trim();
            return Ok(AlterOp::AlterColumnType { name, type_key: sql_type_to_key(ty) });
        }
        return Err(anyhow!("Invalid ALTER COLUMN syntax; expected TYPE"));
    }
    if up.starts_with("ADD CONSTRAINT ") {
        // ADD CONSTRAINT <name> USING <udf>
        let rest = &s["ADD CONSTRAINT ".len()..];
        let rup = rest.to_ascii_uppercase();
        if let Some(pos) = rup.find(" USING ") {
            let name = rest[..pos].trim().trim_matches('"').to_string();
            let udf = rest[pos+" USING ".len()..].trim().to_string();
            return Ok(AlterOp::AddConstraint { name, udf });
        }
        return Err(anyhow!("Invalid ADD CONSTRAINT syntax; expected USING <udf>"));
    }
    if up.starts_with("DROP CONSTRAINT ") {
        let name = s["DROP CONSTRAINT ".len()..].trim().trim_matches('"').to_string();
        return Ok(AlterOp::DropConstraint { name });
    }
    Err(anyhow!(format!("Unsupported ALTER operation: {}", s)))
}

fn parse_add_column(s: &str) -> Result<AlterOp> {
    // <name> <type> [NULL|NOT NULL] [DEFAULT <expr>]
    let tokens: Vec<&str> = s.split_whitespace().collect();
    if tokens.len() < 2 { return Err(anyhow!("ADD COLUMN requires name and type")); }
    let name = tokens[0].trim_matches('"').to_string();
    let mut ty_parts: Vec<&str> = Vec::new();
    let mut i = 1usize;
    while i < tokens.len() {
        let up = tokens[i].to_ascii_uppercase();
        if up == "NULL" || up == "NOT" || up == "DEFAULT" { break; }
        ty_parts.push(tokens[i]); i += 1;
    }
    if ty_parts.is_empty() { return Err(anyhow!("ADD COLUMN missing type")); }
    let mut nullable = true;
    let mut default_expr: Option<String> = None;
    while i < tokens.len() {
        let up = tokens[i].to_ascii_uppercase();
        if up == "NOT" {
            if i + 1 < tokens.len() && tokens[i+1].eq_ignore_ascii_case("NULL") { nullable = false; i += 2; continue; }
        }
        if up == "NULL" { nullable = true; i += 1; continue; }
        if up == "DEFAULT" {
            let expr = s.splitn(2, "DEFAULT").nth(1).unwrap_or("").trim();
            if !expr.is_empty() { default_expr = Some(expr.to_string()); }
            break;
        }
        i += 1;
    }
    Ok(AlterOp::AddColumn { name, type_key: sql_type_to_key(&ty_parts.join(" ")), nullable, default_expr })
}

pub fn parse_alter(s: &str) -> Result<Command> {
    // ALTER TABLE <ident> <ops>
    let rest = s["ALTER ".len()..].trim();
    let up = rest.to_ascii_uppercase();
    if !up.starts_with("TABLE ") { return Err(anyhow!("Only ALTER TABLE is supported")); }
    let tail = &rest["TABLE ".len()..];
    // split first space to get table ident
    let mut parts = tail.splitn(2, ' ');
    let table_ident = parts.next().unwrap_or("").trim();
    if table_ident.is_empty() { return Err(anyhow!("ALTER TABLE requires a table name")); }
    let table = normalize_ident(table_ident);
    let ops_str = parts.next().unwrap_or("").trim();
    if ops_str.is_empty() { return Err(anyhow!("ALTER TABLE requires at least one operation")); }
    let ops = parse_ops(ops_str)?;
    Ok(Command::AlterTable { table, ops })
}
