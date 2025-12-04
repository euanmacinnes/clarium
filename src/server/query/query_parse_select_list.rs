use crate::server::query::query_common::*;
use crate::server::query::query_parse_arith_expr::parse_arith_expr;
use crate::server::query::*;

pub fn parse_select_list(s: &str) -> Result<Vec<SelectItem>> {
    let mut items = Vec::new();
    // split on commas at top-level only (ignore commas inside parentheses)
    let mut parts: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut depth: i32 = 0;
    for ch in s.chars() {
        match ch {
            '(' => { depth += 1; buf.push(ch); }
            ')' => { depth -= 1; buf.push(ch); }
            ',' if depth == 0 => { parts.push(buf.trim().to_string()); buf.clear(); }
            _ => buf.push(ch),
        }
    }
    if !buf.is_empty() { parts.push(buf.trim().to_string()); }
    for tok in parts.into_iter() {
        let mut t = tok.trim();
        if t.is_empty() { continue; }
        // Extract optional alias (case-insensitive " AS ")
        let t_up = t.to_uppercase();
        let mut alias: Option<String> = None;
        if let Some(i) = t_up.rfind(" AS ") {
            let (lhs, rhs_all) = t.split_at(i);
            // rhs_all starts with " AS ", skip 4 including space
            let mut rhs = rhs_all[4..].trim().to_string();
            if rhs.is_empty() { anyhow::bail!("Empty alias in SELECT"); }
            // Allow double-quoted alias names; treat single quotes as invalid for alias
            if rhs.len() >= 2 {
                let first = rhs.as_bytes()[0] as char;
                let last = rhs.as_bytes()[rhs.len()-1] as char;
                if first == '"' && last == '"' { rhs = rhs[1..rhs.len()-1].to_string(); }
                else if first == '\'' && last == '\'' { anyhow::bail!("Invalid alias: use double quotes for named aliases, not single quotes"); }
            }
            alias = Some(rhs);
            t = lhs.trim();
        }
        if t == "_time" {
            if alias.is_some() { anyhow::bail!("Alias is not allowed on _time"); }
            items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: "_time".into(), expr: None, alias: None});
            continue;
        }
        if t == "*" {
            if alias.is_some() { anyhow::bail!("Alias is not allowed on *"); }
            items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: "*".into(), expr: None, alias: None});
            continue;
        }
        // Qualified wildcard like t.* (or schema-qualified alias like t/* not expected here)
        // Recognize patterns that end with ".*" and treat them as a wildcard projection tied to a qualifier.
        if t.ends_with(".*") {
            if alias.is_some() { anyhow::bail!("Alias is not allowed on qualified wildcard (e.g., t.*)"); }
            let qual = t[..t.len()-2].trim();
            if qual.is_empty() {
                anyhow::bail!("Syntax error: expected qualifier before .* in SELECT list");
            }
            // Keep original text for qualifier (may include dots or quotes), executor will expand based on alias mapping
            items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: format!("{}.*", qual), expr: None, alias: None});
            continue;
        }
        if (t == "_start_time" || t == "_end_time") && alias.is_some() {
            anyhow::bail!("Alias is not allowed on _start_time or _end_time");
        }
        // Try function form FUNC(expr)
        if let Some(p1) = t.find('(') {
            if t.ends_with(')') {
                let func_name = t[..p1].trim().to_uppercase();
                let inner = t[p1+1..t.len()-1].trim();
                // Recognize QUANTILE with optional cutoff parameter inside parentheses
                if func_name == "QUANTILE" {
                    // Expect: QUANTILE(expr) or QUANTILE(expr, cutoff)
                    // Split on the last comma to allow expressions in first arg
                    let mut cutoff: i64 = 50;
                    let (expr_txt, cutoff_opt) = if let Some(idx) = inner.rfind(',') {
                        let (a, b) = inner.split_at(idx);
                        let p = b[1..].trim();
                        if !p.is_empty() { cutoff = p.parse::<i64>().map_err(|_| anyhow::anyhow!(format!("Invalid QUANTILE cutoff: {}", p)))?; }
                        (a.trim(), Some(p))
                    } else { (inner, None) };
                    let ar = parse_arith_expr(&expr_txt.split_whitespace().map(|s| s.to_string()).collect::<Vec<String>>())?;
                    items.push(SelectItem{ func: Some(AggFunc::Quantile(cutoff)), str_func: None, window_func: None, window_spec: None, column: expr_txt.into(), expr: Some(ar), alias });
                    continue;
                }
                // Recognize numeric aggs and string funcs
                let agg = match func_name.as_str() {
                    "AVG" => Some(AggFunc::Avg),
                    "MAX" => Some(AggFunc::Max),
                    "MIN" => Some(AggFunc::Min),
                    "SUM" => Some(AggFunc::Sum),
                    "COUNT" => Some(AggFunc::Count),
                    "FIRST" => Some(AggFunc::First),
                    "LAST" => Some(AggFunc::Last),
                    "STDEV" => Some(AggFunc::Stdev),
                    "DELTA" => Some(AggFunc::Delta),
                    "HEIGHT" => Some(AggFunc::Height),
                    "GRADIENT" => Some(AggFunc::Gradient),
                    "ARRAY_AGG" => Some(AggFunc::ArrayAgg),
                    _ => None,
                };
                if let Some(a) = agg {
                    // Special-case COUNT(*) to support row counting semantics
                    if a == AggFunc::Count && inner.trim() == "*" {
                        items.push(SelectItem{ func: Some(AggFunc::Count), str_func: None, window_func: None, window_spec: None, column: "*".into(), expr: None, alias });
                        continue;
                    }
                    // Parse inner as arithmetic expression allowing sensor-1 etc.
                    let ar = parse_arith_expr(&inner.split_whitespace().map(|s| s.to_string()).collect::<Vec<String>>())?;
                    items.push(SelectItem{ func: Some(a), str_func: None, window_func: None, window_spec: None, column: inner.into(), expr: Some(ar), alias });
                    continue;
                }
                let sfunc = match func_name.as_str() {
                    "UPPER" => Some(StrFunc::Upper),
                    "LOWER" => Some(StrFunc::Lower),
                    _ => None,
                };
                if let Some(sf) = sfunc {
                    // For string funcs, keep legacy column parsing
                    items.push(SelectItem{ func: None, str_func: Some(sf), window_func: None, window_spec: None, column: inner.into(), expr: None, alias });
                    continue;
                }
                // Recognize window functions: ROW_NUMBER() OVER (...)
                let wfunc = match func_name.as_str() {
                    "ROW_NUMBER" => Some(WindowFunc::RowNumber),
                    _ => None,
                };
                if let Some(wf) = wfunc {
                    // Window functions require OVER clause after the function
                    // Find OVER in the original tok (before alias extraction)
                    // We need to find where ROW_NUMBER() appears in tok and search after its closing paren
                    let tok_up = tok.to_uppercase();
                    let func_start_in_tok = tok_up.find(&func_name).unwrap_or(0);
                    // Find the matching closing paren for the function in tok
                    let paren_start = func_start_in_tok + func_name.len();
                    let mut depth = 0;
                    let mut close_paren_pos = paren_start;
                    for (i, ch) in tok[paren_start..].char_indices() {
                        if ch == '(' { depth += 1; }
                        else if ch == ')' { depth -= 1; if depth == 0 { close_paren_pos = paren_start + i + 1; break; } }
                    }
                    let after_func = &tok[close_paren_pos..];
                    let after_func_up = after_func.to_uppercase();
                    if let Some(over_pos) = after_func_up.find("OVER") {
                        // Check word boundary
                        let abs_over = over_pos;
                        let prev_ok = abs_over == 0 || !after_func.as_bytes()[abs_over-1].is_ascii_alphanumeric();
                        let next_ok = abs_over + 4 >= after_func.len() || !after_func.as_bytes()[abs_over+4].is_ascii_alphanumeric();
                        if prev_ok && next_ok {
                            // Find matching parentheses for OVER (...)
                            let after_over = &after_func[abs_over + 4..].trim_start();
                            if after_over.starts_with('(') {
                                let mut depth = 0;
                                let mut end_pos = 0;
                                for (i, ch) in after_over.char_indices() {
                                    if ch == '(' { depth += 1; }
                                    else if ch == ')' { depth -= 1; if depth == 0 { end_pos = i + 1; break; } }
                                }
                                if depth == 0 && end_pos > 0 {
                                    let over_clause = &after_over[1..end_pos-1].trim();
                                    // Parse PARTITION BY and ORDER BY within OVER clause
                                    let mut partition_by: Option<Vec<String>> = None;
                                    let mut order_by: Option<Vec<(ArithExpr, bool)>> = None;
                                    
                                    let clause_up = over_clause.to_uppercase();
                                    // Find PARTITION BY
                                    if let Some(part_pos) = clause_up.find("PARTITION BY") {
                                        let after_part = &over_clause[part_pos + 12..].trim();
                                        // Find end of PARTITION BY (either ORDER BY or end of string)
                                        let part_end = if let Some(order_pos) = after_part.to_uppercase().find("ORDER BY") {
                                            order_pos
                                        } else {
                                            after_part.len()
                                        };
                                        let part_cols_str = &after_part[..part_end].trim();
                                        let part_cols: Vec<String> = part_cols_str.split(',').map(|s| s.trim().to_string()).collect();
                                        partition_by = Some(part_cols);
                                    }
                                    // Find ORDER BY
                                    if let Some(order_pos) = clause_up.find("ORDER BY") {
                                        let after_order = &over_clause[order_pos + 8..].trim();
                                        let order_cols_str = after_order;
                                        let mut order_cols: Vec<(ArithExpr, bool)> = Vec::new();
                                        // Split by comma at top level (respecting parentheses)
                                        let mut parts: Vec<String> = Vec::new();
                                        let mut buf = String::new();
                                        let mut depth = 0;
                                        for ch in order_cols_str.chars() {
                                            match ch {
                                                '(' => { depth += 1; buf.push(ch); }
                                                ')' => { depth -= 1; buf.push(ch); }
                                                ',' if depth == 0 => { parts.push(buf.trim().to_string()); buf.clear(); }
                                                _ => buf.push(ch),
                                            }
                                        }
                                        if !buf.is_empty() { parts.push(buf.trim().to_string()); }
                                        
                                        for col_str in parts {
                                            let col_trim = col_str.trim();
                                            let col_up = col_trim.to_uppercase();
                                            let (expr_str, asc) = if col_up.ends_with(" DESC") {
                                                (col_trim[..col_trim.len()-5].trim(), false)
                                            } else if col_up.ends_with(" ASC") {
                                                (col_trim[..col_trim.len()-4].trim(), true)
                                            } else {
                                                (col_trim, true)
                                            };
                                            // Parse expression
                                            let tokens: Vec<String> = expr_str.split_whitespace().map(|s| s.to_string()).collect();
                                            let expr = parse_arith_expr(&tokens)?;
                                            order_cols.push((expr, asc));
                                        }
                                        order_by = Some(order_cols);
                                    }
                                    
                                    let window_spec = WindowSpec { partition_by, order_by };
                                    items.push(SelectItem{ 
                                        func: None, 
                                        str_func: None, 
                                        window_func: Some(wf), 
                                        window_spec: Some(window_spec), 
                                        column: t.into(), 
                                        expr: None, 
                                        alias 
                                    });
                                    continue;
                                }
                            }
                        }
                    }
                    anyhow::bail!("Window function {} requires OVER clause", func_name);
                }
                // Support date functions as arithmetic expressions
                if matches!(func_name.as_str(), "DATEPART" | "DATEADD" | "DATEDIFF") {
                    let ar = parse_arith_expr(&[t.to_string()])?;
                    items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: t.into(), expr: Some(ar), alias });
                    continue;
                }
                // Unknown functions: allow as arithmetic expression (may resolve to Lua UDF at execution)
                let ar = parse_arith_expr(&[t.to_string()])?;
                items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: t.into(), expr: Some(ar), alias });
                continue;
            }
        }
        // Otherwise parse as arithmetic expression projection
        let tokens: Vec<String> = t.split_whitespace().map(|s| s.to_string()).collect();
        if tokens.len() == 1 && tokens[0].as_str() != "_time" && !tokens[0].contains(['+','*','/','(',')']) {
            // Single token case: decide between numeric/datetime literal, slice expression, string literal, or simple column name
            let tok = &tokens[0];
            let is_numeric = tok.parse::<f64>().is_ok() || (tok.starts_with('-') && tok.len() > 1 && tok[1..].parse::<f64>().is_ok());
            let is_datetime = parse_iso8601_to_ms(tok).is_some();
            let looks_like_slice = tok.contains('[') && tok.contains(']');
            let is_single_quoted_literal = tok.len() >= 2 && tok.starts_with('\'') && tok.ends_with('\'');
            let is_null_literal = tok.eq_ignore_ascii_case("NULL");
            // PostgreSQL-style cast within a single token: e.g., '\'1\'::int' or (expr)::type without spaces
            let contains_pg_cast = tok.contains("::");
            if contains_pg_cast || is_numeric || is_datetime || looks_like_slice || tok.starts_with("f'") || is_single_quoted_literal || is_null_literal {
                // Defer to arithmetic expression parser to correctly build literal/expr nodes
                let ar = parse_arith_expr(&tokens)?;
                items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: t.into(), expr: Some(ar), alias });
            } else {
                // simple column name
                items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: t.into(), expr: None, alias });
            }
        } else {
            let ar = parse_arith_expr(&tokens)?;
            items.push(SelectItem{ func: None, str_func: None, window_func: None, window_spec: None, column: t.into(), expr: Some(ar), alias });
        }
    }
    Ok(items)
}


