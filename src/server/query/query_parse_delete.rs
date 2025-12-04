use crate::server::query::query_common::split_once_any;
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

fn parse_delete(s: &str) -> Result<Command> {
    // DELETE FROM <db> [WHERE ...]
    // or DELETE COLUMNS (<c1>, <c2>, ...) FROM <db> [WHERE ...]
    let sup = s.to_uppercase();
    // strip leading DELETE
    let rest = s[6..].trim();
    let rest_up = sup[6..].trim().to_string();
    if rest_up.starts_with("COLUMNS ") {
        // Expect COLUMNS (<list>) FROM <db> [WHERE ...]
        let after = &rest[8..].trim();
        // Expect parentheses
        let (cols_part, tail_start) = if let Some(p1) = after.find('(') {
            if let Some(p2) = after[p1+1..].find(')') { let end = p1 + 1 + p2; (&after[p1+1..end], &after[end+1..]) } else { anyhow::bail!("Invalid DELETE COLUMNS: missing )"); }
        } else { anyhow::bail!("Invalid DELETE COLUMNS: expected (list)"); };
        let mut columns: Vec<String> = cols_part.split(',').map(|t| t.trim().to_string()).filter(|s| !s.is_empty()).collect();
        columns.dedup();
        let tail = tail_start.trim();
        let tail_up = tail.to_uppercase();
        if !tail_up.starts_with("FROM ") { anyhow::bail!("Invalid DELETE COLUMNS: missing FROM"); }
        let after_from = &tail[5..];
        // Split db and optional WHERE
        let (db_part, where_part_opt) = split_once_any(after_from, &[" WHERE "]); // prefer WHERE
        let database = db_part.trim().to_string();
        let where_clause = where_part_opt.map(|w| w.trim()).and_then(|w| parse_where_expr(w).ok());
        Ok(Command::DeleteColumns { database, columns, where_clause })
    } else if rest_up.starts_with("FROM ") {
        let after_from = &rest[5..];
        let (db_part, where_part_opt) = split_once_any(after_from, &[" WHERE "]); // optional WHERE
        let database = db_part.trim().to_string();
        let where_clause = where_part_opt.map(|w| w.trim()).and_then(|w| parse_where_expr(w).ok());
        Ok(Command::DeleteRows { database, where_clause })
    } else {
        anyhow::bail!("Invalid DELETE syntax");
    }
}
