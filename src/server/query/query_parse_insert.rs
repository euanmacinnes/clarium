use crate::server::query::query_common::*;
use crate::server::query::*;

pub fn parse_manual_row(s: &str) -> Result<ManualRow> {
    let parts = split_csv_ignoring_quotes(s);
    if parts.len() < 2 { anyhow::bail!("Manual SLICE row must start with start and end dates"); }
    let start = parse_date_token_to_ms(&parts[0])?;
    let end = parse_date_token_to_ms(&parts[1])?;
    let mut labels: Vec<ManualLabel> = Vec::new();
    for p in parts.into_iter().skip(2) { if p.is_empty() { continue; } labels.push(parse_manual_cell(&p)); }
    Ok(ManualRow{ start, end, labels })
}

pub fn parse_manual_rows(s: &str) -> Result<Option<(SliceSource, usize)>> {
    // s starts with '(' already trimmed
    if let Some((inner, used1)) = extract_paren_block(s) {
        let inner_trim = inner.trim_start();
        if inner_trim.starts_with('(') {
            // multi-row: a sequence of (row) items separated by commas
            let mut rest = inner;
            let mut rows: Vec<ManualRow> = Vec::new();
            loop {
                let rest_trim = rest.trim_start();
                if rest_trim.is_empty() { break; }
                if let Some((row_inside, row_used)) = extract_paren_block(rest_trim) {
                    let row = parse_manual_row(row_inside)?;
                    rows.push(row);
                    let lead_ws = rest.len() - rest_trim.len();
                    let consumed = lead_ws + row_used;
                    rest = &rest[consumed..];
                    // optional comma
                    let rest2 = rest.trim_start();
                    if rest2.starts_with(',') { rest = &rest2[1..]; } else { rest = rest2; }
                } else {
                    // not a row block
                    break;
                }
            }
            return Ok(Some((SliceSource::Manual{ rows }, used1)));
        } else {
            // single row
            let row = parse_manual_row(inner)?;
            return Ok(Some((SliceSource::Manual{ rows: vec![row] }, used1)));
        }
    }
    Ok(None)
}


pub fn parse_insert(s: &str) -> Result<Command> {
    // INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...), (val3, val4, ...), ...
    let rest = s[6..].trim(); // after "INSERT"
    let up = rest.to_uppercase();
    
    // Expect INTO
    if !up.starts_with("INTO ") {
        anyhow::bail!("INSERT syntax error: expected INTO");
    }
    let after_into = rest[5..].trim();
    
    // Find table name (everything before the opening paren or VALUES keyword)
    let table_end = after_into.find('(').or_else(|| {
        after_into.to_uppercase().find(" VALUES")
    }).ok_or_else(|| anyhow::anyhow!("INSERT syntax error: expected column list or VALUES"))?;
    
    let mut table = after_into[..table_end].trim().to_string();
    // Strip quotes from table name if present
    if (table.starts_with('"') && table.ends_with('"')) || (table.starts_with('\'') && table.ends_with('\'')) {
        if table.len() >= 2 {
            table = table[1..table.len()-1].to_string();
        }
    }
    if table.is_empty() {
        anyhow::bail!("INSERT syntax error: missing table name");
    }
    
    let remaining = after_into[table_end..].trim();
    
    // Parse column list
    let (columns, values_start) = if remaining.starts_with('(') {
        // Extract column list
        let (cols_inner, cols_used) = extract_paren_block(remaining)
            .ok_or_else(|| anyhow::anyhow!("INSERT syntax error: incomplete column list"))?;
        
        let cols: Vec<String> = split_csv_ignoring_quotes(cols_inner)
            .into_iter()
            .map(|c| c.trim().trim_matches('"').to_string())
            .collect();
        
        let after_cols = remaining[cols_used..].trim();
        (cols, after_cols)
    } else {
        // No column list, we'll infer later or error
        (Vec::new(), remaining)
    };
    
    // Expect VALUES keyword
    let values_up = values_start.to_uppercase();
    if !values_up.starts_with("VALUES ") {
        anyhow::bail!("INSERT syntax error: expected VALUES clause");
    }
    let after_values = values_start[7..].trim();
    
    // Parse value tuples: (v1, v2, ...), (v3, v4, ...), ...
    let mut values: Vec<Vec<ArithTerm>> = Vec::new();
    let mut remaining_vals = after_values;
    
    loop {
        let remaining_trim = remaining_vals.trim();
        if remaining_trim.is_empty() {
            break;
        }
        
        // Extract one value tuple
        if !remaining_trim.starts_with('(') {
            anyhow::bail!("INSERT syntax error: expected '(' for value tuple");
        }
        
        let (vals_inner, vals_used) = extract_paren_block(remaining_trim)
            .ok_or_else(|| anyhow::anyhow!("INSERT syntax error: incomplete value tuple"))?;
        
        // Parse individual values as ArithTerm
        let val_strings = split_csv_ignoring_quotes(vals_inner);
        let mut row_values: Vec<ArithTerm> = Vec::new();
        
        for val_str in val_strings {
            let val_trim = val_str.trim();
            if val_trim.is_empty() {
                continue;
            }
            
            // Parse as ArithTerm
            let term = if val_trim.eq_ignore_ascii_case("NULL") {
                ArithTerm::Null
            } else if val_trim.starts_with('\'') && val_trim.ends_with('\'') && val_trim.len() >= 2 {
                // String literal
                ArithTerm::Str(val_trim[1..val_trim.len()-1].to_string())
            } else if let Ok(num) = val_trim.parse::<f64>() {
                // Numeric literal
                ArithTerm::Number(num)
            } else {
                // Try to parse as string without quotes
                ArithTerm::Str(val_trim.to_string())
            };
            
            row_values.push(term);
        }
        
        values.push(row_values);
        
        // Move past this tuple
        remaining_vals = &remaining_trim[vals_used..].trim();
        
        // Check for comma separator
        if remaining_vals.starts_with(',') {
            remaining_vals = &remaining_vals[1..];
        } else {
            // No more tuples
            break;
        }
    }
    
    if values.is_empty() {
        anyhow::bail!("INSERT syntax error: no values provided");
    }
    
    Ok(Command::Insert { table, columns, values })
}