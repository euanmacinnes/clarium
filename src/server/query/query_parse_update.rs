use crate::server::query::query_common::*;
use crate::server::query::*;

pub fn parse_update(s: &str) -> Result<Command> {
    // UPDATE <table> SET col = value[, ...] [WHERE ...]
    let rest = s[6..].trim(); // after UPDATE
    if rest.is_empty() { anyhow::bail!("Invalid UPDATE syntax: missing table name"); }
    // Split at SET (case-insensitive)
    let rest_up = rest.to_uppercase();
    let pos_set = rest_up.find(" SET ").ok_or_else(|| anyhow::anyhow!("Invalid UPDATE syntax: missing SET"))?;
    let mut table = rest[..pos_set].trim().to_string();
    // Strip optional quotes around table
    if (table.starts_with('"') && table.ends_with('"')) || (table.starts_with('\'') && table.ends_with('\'')) {
        if table.len() >= 2 { table = table[1..table.len()-1].to_string(); }
    }
    let after_set = &rest[pos_set + 5..];
    // Optional WHERE: split once on WHERE
    let (assign_part, where_part_opt) = split_once_any(after_set, &[" WHERE "]);
    let assign_part = assign_part.trim();
    if assign_part.is_empty() { anyhow::bail!("Invalid UPDATE syntax: empty SET assignments"); }
    // Parse assignments: comma-separated col = value
    let mut assignments: Vec<(String, ArithTerm)> = Vec::new();
    for chunk in split_csv_ignoring_quotes(assign_part) {
        let t = chunk.trim();
        if t.is_empty() { continue; }
        // split on first '='
        let eq_pos = t.find('=');
        let Some(eq) = eq_pos else { anyhow::bail!("Invalid assignment in UPDATE: {}", t); };
        let left = t[..eq].trim().trim_matches('"').to_string();
        let right = t[eq+1..].trim();
        if left.is_empty() { anyhow::bail!("Invalid assignment: missing column name"); }
        let term = if right.eq_ignore_ascii_case("NULL") {
            ArithTerm::Null
        } else if right.starts_with('\'') && right.ends_with('\'') && right.len() >= 2 {
            ArithTerm::Str(right[1..right.len()-1].to_string())
        } else if let Ok(num) = right.parse::<f64>() {
            ArithTerm::Number(num)
        } else {
            // treat as bare identifier string literal for now
            ArithTerm::Str(right.to_string())
        };
        assignments.push((left, term));
    }
    if assignments.is_empty() { anyhow::bail!("UPDATE: no assignments parsed"); }
    let where_clause = where_part_opt
        .map(|w| w.trim())
        .filter(|w| !w.is_empty())
        .and_then(|w| parse_where_expr(w).ok());
    Ok(Command::Update { table, assignments, where_clause })
}






