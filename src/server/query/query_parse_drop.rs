use crate::server::query::query_common::Query;
use crate::server::query::query_common::WhereExpr;
use crate::server::query::query_common::CompOp;
use crate::server::query::query_common::ArithExpr as AE;
use crate::server::query::query_common::ArithTerm as AT;
use crate::server::query::query_common::WhereExpr as WE;
use crate::server::query::query_common::ArithTerm;
use crate::server::query::query_common::ArithExpr;
use crate::server::query::query_common::DateFunc;
use crate::server::query::query_common::StrSliceBound;
use crate::server::query::query_common::JoinType;
use crate::server::query::Command;

fn parse_drop(s: &str) -> Result<Command> {
    // DROP DATABASE <db>
    // DROP SCHEMA <db>/<schema>
    // DROP TIME TABLE <db>/<schema>/<table>.time
    // DROP TABLE <db>/<schema>/<table> | <table>
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("VIEW ") || up.starts_with("VIEW") {
        // DROP VIEW [IF EXISTS] <name>
        let mut tail = if up == "VIEW" { "" } else { &rest["VIEW ".len()..] };
        tail = tail.trim();
        let tail_up = tail.to_uppercase();
        let mut if_exists = false;
        if tail_up.starts_with("IF EXISTS ") {
            if_exists = true;
            tail = &tail["IF EXISTS ".len()..].trim();
        }
        if tail.is_empty() { anyhow::bail!("Invalid DROP VIEW: missing view name"); }
        let normalized_name = crate::ident::normalize_identifier(tail);
        return Ok(Command::DropView { name: normalized_name, if_exists });
    }
    if up.starts_with("VECTOR INDEX ") {
        // DROP VECTOR INDEX <name>
        let name = rest["VECTOR INDEX ".len()..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP VECTOR INDEX: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::DropVectorIndex { name: normalized_name });
    }
    if up.starts_with("GRAPH ") {
        let name = rest["GRAPH ".len()..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP GRAPH: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::DropGraph { name: normalized_name });
    }
    if up.starts_with("DATABASE ") {
        let name = rest[9..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP DATABASE: missing database name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::DropDatabase { name: normalized_name });
    }
    if up.starts_with("SCRIPT ") {
        let name = rest[7..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP SCRIPT: missing name"); }
        // Script names already normalized in scripts.rs, pass as-is for now
        return Ok(Command::DropScript { path: name.to_string() });
    }
    if up.starts_with("SCHEMA ") {
        let path = rest[7..].trim();
        if path.is_empty() { anyhow::bail!("Invalid DROP SCHEMA: missing schema name"); }
        let normalized_path = crate::ident::normalize_identifier(path);
        return Ok(Command::DropSchema { path: normalized_path });
    }
    if up.starts_with("TIME TABLE ") {
        let table = rest[11..].trim();
        if table.is_empty() { anyhow::bail!("Invalid DROP TIME TABLE: missing time table name"); }
        if !table.ends_with(".time") { anyhow::bail!("DROP TIME TABLE target must end with .time"); }
        return Ok(Command::DropTimeTable { table: table.to_string() });
    }
    if up.starts_with("STORE ") {
        // DROP STORE <db>.store.<store>
        let addr = rest[6..].trim();
        let (db, st) = parse_store_addr(addr)?;
        return Ok(Command::DropStore { database: db, store: st });
    }
    if up.starts_with("TABLE ") {
        let mut table = rest[6..].trim();
        let mut if_exists = false;
        // Check for optional IF EXISTS
        let table_up = table.to_uppercase();
        if table_up.starts_with("IF EXISTS ") {
            if_exists = true;
            table = &table[10..].trim();
        }
        if table.is_empty() { anyhow::bail!("Invalid DROP TABLE: missing table name"); }
        if table.ends_with(".time") { anyhow::bail!("DROP TABLE cannot target a .time table; use DROP TIME TABLE"); }
        return Ok(Command::DropTable { table: table.to_string(), if_exists });
    }
    anyhow::bail!("Invalid DROP syntax")
}