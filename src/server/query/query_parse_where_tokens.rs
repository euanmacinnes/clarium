use tracing::debug;
use crate::server::query::query_common::*;
use crate::server::query::*;
use crate::server::query::query_parse_arith_expr::parse_arith_expr;

pub fn parse_where_tokens(tokens: &[String], original: &str) -> Result<WhereExpr> {
    // Split by OR (lowest precedence)
    if let Some(idx) = find_token_ci(tokens, "OR") {
        let left = parse_where_tokens(&tokens[..idx], original)?;
        let right = parse_where_tokens(&tokens[idx+1..], original)?;
        return Ok(WhereExpr::Or(Box::new(left), Box::new(right)));
    }

    // Handle BETWEEN before splitting by AND because BETWEEN contains an AND internally
    if let Some(bi) = find_token_ci(tokens, "BETWEEN") {
        // Find the AND that separates low and high bounds of BETWEEN
        if let Some(ai) = tokens.iter().enumerate().skip(bi + 1).find(|(_, t)| t.to_uppercase() == "AND").map(|(i, _)| i) {
            let left_expr = parse_arith_expr(&tokens[..bi])?;
            let low_expr = parse_arith_expr(&tokens[bi+1..ai])?;
            // Determine the end of the high bound: stop before the next top-level AND/OR if present
            let mut hi_end = tokens.len();
            if let Some(next_and) = tokens.iter().enumerate().skip(ai + 1).find(|(_, t)| {
                let up = t.to_uppercase(); up == "AND" || up == "OR"
            }).map(|(i, _)| i) {
                hi_end = next_and;
            }
            let high_expr = parse_arith_expr(&tokens[ai+1..hi_end])?;
            let ge = WhereExpr::Comp { left: left_expr.clone(), op: CompOp::Ge, right: low_expr };
            let le = WhereExpr::Comp { left: left_expr, op: CompOp::Le, right: high_expr };
            let between_expr = WhereExpr::And(Box::new(ge), Box::new(le));
            // If trailing tokens exist (e.g., AND y = 1), combine recursively preserving the operator
            if hi_end < tokens.len() {
                let op_tok = tokens[hi_end].to_uppercase();
                let rest = &tokens[hi_end+1..];
                let rest_expr = parse_where_tokens(rest, original)?;
                return Ok(match op_tok.as_str() {
                    "AND" => WhereExpr::And(Box::new(between_expr), Box::new(rest_expr)),
                    "OR" => WhereExpr::Or(Box::new(between_expr), Box::new(rest_expr)),
                    _ => between_expr,
                });
            }
            return Ok(between_expr);
        } else {
            anyhow::bail!("Invalid BETWEEN syntax: expected AND");
        }
    }

    // Then split by AND
    if let Some(idx) = find_token_ci(tokens, "AND") {
        let left = parse_where_tokens(&tokens[..idx], original)?;
        let right = parse_where_tokens(&tokens[idx+1..], original)?;
        return Ok(WhereExpr::And(Box::new(left), Box::new(right)));
    }

    // Handle [NOT] EXISTS (subquery)
    if let Some(i) = find_token_ci(tokens, "EXISTS") {
        let negated = i > 0 && tokens[i-1].to_uppercase() == "NOT";
        // Find EXISTS position in original string (case-insensitive)
        let original_up = original.to_uppercase();
        if let Some(exists_pos) = original_up.find("EXISTS") {
            // Find the opening paren after EXISTS in original string
            let after_exists = &original[exists_pos + 6..]; // 6 = len("EXISTS")
            if let Some(pstart) = after_exists.find('(') {
                // Find matching closing paren by tracking depth, respecting string literals
                let bytes = after_exists.as_bytes();
                let mut depth = 1; // Start at depth 1 since we've found the opening paren
                let mut pend: Option<usize> = None;
                let mut in_squote = false;
                let mut in_dquote = false;
                // Start iteration AFTER the opening paren
                for (idx, &b) in bytes.iter().enumerate().skip(pstart + 1) {
                    let ch = b as char;
                    match ch {
                        '\'' if !in_dquote => in_squote = !in_squote,
                        '"' if !in_squote => in_dquote = !in_dquote,
                        '(' if !in_squote && !in_dquote => {
                            depth += 1;
                        }
                        ')' if !in_squote && !in_dquote => {
                            depth -= 1;
                            if depth == 0 {
                                pend = Some(idx);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(pend) = pend {
                    let inner = &after_exists[pstart+1..pend];
                    let subquery = parse_select(inner)?;
                    return Ok(WhereExpr::Exists { negated, subquery: Box::new(subquery) });
                } else {
                    anyhow::bail!("EXISTS clause missing closing parenthesis");
                }
            } else {
                anyhow::bail!("EXISTS requires parenthesized subquery");
            }
        } else {
            anyhow::bail!("EXISTS keyword not found in original string");
        }
    }

    // Handle value op ALL (subquery) and value op ANY (subquery)
    // Look for ALL or ANY keywords
    if let Some(all_idx) = find_token_ci(tokens, "ALL") {
        // Expect: value op ALL (subquery) or value op NOT ALL (subquery)
        // Find comparison operator before ALL
        if all_idx >= 2 {
            let op_idx = all_idx - 1;
            let op_str = tokens[op_idx].to_uppercase();
            let comp_op = match op_str.as_str() {
                "=" | "==" => Some(CompOp::Eq),
                "!=" | "<>" => Some(CompOp::Ne),
                ">" => Some(CompOp::Gt),
                ">=" => Some(CompOp::Ge),
                "<" => Some(CompOp::Lt),
                "<=" => Some(CompOp::Le),
                "LIKE" => Some(CompOp::Like),
                _ => None,
            };
            if let Some(op) = comp_op {
                let left_expr = parse_arith_expr(&tokens[..op_idx])?;
                // Check for NOT before ALL
                let negated = false; // NOT ALL is handled as "value op NOT ALL (...)" which is uncommon; typically NOT (value op ALL (...))
                // Parse subquery after ALL
                if all_idx + 1 < tokens.len() {
                    let subq_str = tokens[all_idx+1..].join(" ");
                    if let Some(pstart) = subq_str.find('(') {
                        if let Some(pend) = subq_str.rfind(')') {
                            let inner = &subq_str[pstart+1..pend];
                            let subquery = parse_select(inner)?;
                            return Ok(WhereExpr::All { left: left_expr, op, subquery: Box::new(subquery), negated });
                        } else {
                            anyhow::bail!("ALL clause missing closing parenthesis");
                        }
                    } else {
                        anyhow::bail!("ALL requires parenthesized subquery");
                    }
                } else {
                    anyhow::bail!("ALL requires subquery");
                }
            }
        }
    }
    if let Some(any_idx) = find_token_ci(tokens, "ANY") {
        // Expect: value op ANY (subquery)
        if any_idx >= 2 {
            let op_idx = any_idx - 1;
            let op_str = tokens[op_idx].to_uppercase();
            let comp_op = match op_str.as_str() {
                "=" | "==" => Some(CompOp::Eq),
                "!=" | "<>" => Some(CompOp::Ne),
                ">" => Some(CompOp::Gt),
                ">=" => Some(CompOp::Ge),
                "<" => Some(CompOp::Lt),
                "<=" => Some(CompOp::Le),
                "LIKE" => Some(CompOp::Like),
                _ => None,
            };
            if let Some(op) = comp_op {
                let left_expr = parse_arith_expr(&tokens[..op_idx])?;
                let negated = false;
                if any_idx + 1 < tokens.len() {
                    let subq_str = tokens[any_idx+1..].join(" ");
                    if let Some(pstart) = subq_str.find('(') {
                        if let Some(pend) = subq_str.rfind(')') {
                            let inner = &subq_str[pstart+1..pend];
                            let subquery = parse_select(inner)?;
                            return Ok(WhereExpr::Any { left: left_expr, op, subquery: Box::new(subquery), negated });
                        } else {
                            anyhow::bail!("ANY clause missing closing parenthesis");
                        }
                    } else {
                        anyhow::bail!("ANY requires parenthesized subquery");
                    }
                } else {
                    anyhow::bail!("ANY requires subquery");
                }
            }
        }
    }

    // Handle NOT IN clause: col NOT IN (val1, val2, val3) -> NOT (col = val1 OR col = val2 OR col = val3)
    // Check for NOT IN before checking for IN alone
    if let Some(i) = find_token_ci(tokens, "NOT") {
        if i + 1 < tokens.len() && tokens[i + 1].to_uppercase() == "IN" {
            let left_expr = parse_arith_expr(&tokens[..i])?;
            // Expect tokens[i+2] onwards to be a parenthesized list
            if i + 2 < tokens.len() {
                let list_str = tokens[i+2..].join(" ");
                // Simple parser: extract content between ( and )
                if let Some(start) = list_str.find('(') {
                    if let Some(end) = list_str.rfind(')') {
                        let inner = &list_str[start+1..end];
                        // Split by comma and parse each value
                        let values: Vec<ArithExpr> = inner.split(',')
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .map(|s| {
                                // Try to parse as a quoted string or number
                                let s_trimmed = s.trim();
                                if (s_trimmed.starts_with('\'') && s_trimmed.ends_with('\'')) ||
                                   (s_trimmed.starts_with('"') && s_trimmed.ends_with('"')) {
                                    let unquoted = &s_trimmed[1..s_trimmed.len()-1];
                                    Ok(ArithExpr::Term(ArithTerm::Str(unquoted.to_string())))
                                } else if let Ok(n) = s_trimmed.parse::<f64>() {
                                    Ok(ArithExpr::Term(ArithTerm::Number(n)))
                                } else {
                                    // Try parsing as column or expression
                                    parse_arith_expr(&[s_trimmed.to_string()])
                                }
                            })
                            .collect::<Result<Vec<_>>>()?;
                        
                        if values.is_empty() {
                            anyhow::bail!("NOT IN clause requires at least one value");
                        }
                        
                        // Build AND chain: (col != val1) AND (col != val2) AND ...
                        let mut result = WhereExpr::Comp {
                            left: left_expr.clone(),
                            op: CompOp::Ne,
                            right: values[0].clone(),
                        };
                        for val in &values[1..] {
                            let cmp = WhereExpr::Comp {
                                left: left_expr.clone(),
                                op: CompOp::Ne,
                                right: val.clone(),
                            };
                            result = WhereExpr::And(Box::new(result), Box::new(cmp));
                        }
                        return Ok(result);
                    } else {
                        anyhow::bail!("NOT IN clause missing closing parenthesis");
                    }
                } else {
                    anyhow::bail!("NOT IN clause requires parenthesized list");
                }
            } else {
                anyhow::bail!("NOT IN clause requires value list");
            }
        }
    }

    // Handle IN clause: col IN (val1, val2, val3) -> col = val1 OR col = val2 OR col = val3
    if let Some(i) = find_token_ci(tokens, "IN") {
        let left_expr = parse_arith_expr(&tokens[..i])?;
        // Expect tokens[i+1] to be a parenthesized list
        if i + 1 < tokens.len() {
            let list_str = tokens[i+1..].join(" ");
            // Simple parser: extract content between ( and )
            if let Some(start) = list_str.find('(') {
                if let Some(end) = list_str.rfind(')') {
                    let inner = &list_str[start+1..end];
                    // Split by comma and parse each value
                    let values: Vec<ArithExpr> = inner.split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| {
                            // Try to parse as a quoted string or number
                            let s_trimmed = s.trim();
                            if (s_trimmed.starts_with('\'') && s_trimmed.ends_with('\'')) ||
                               (s_trimmed.starts_with('"') && s_trimmed.ends_with('"')) {
                                let unquoted = &s_trimmed[1..s_trimmed.len()-1];
                                Ok(ArithExpr::Term(ArithTerm::Str(unquoted.to_string())))
                            } else if let Ok(n) = s_trimmed.parse::<f64>() {
                                Ok(ArithExpr::Term(ArithTerm::Number(n)))
                            } else {
                                // Try parsing as column or expression
                                parse_arith_expr(&[s_trimmed.to_string()])
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    
                    if values.is_empty() {
                        anyhow::bail!("IN clause requires at least one value");
                    }
                    
                    // Build OR chain: (col = val1) OR (col = val2) OR ...
                    let mut result = WhereExpr::Comp {
                        left: left_expr.clone(),
                        op: CompOp::Eq,
                        right: values[0].clone(),
                    };
                    for val in &values[1..] {
                        let cmp = WhereExpr::Comp {
                            left: left_expr.clone(),
                            op: CompOp::Eq,
                            right: val.clone(),
                        };
                        result = WhereExpr::Or(Box::new(result), Box::new(cmp));
                    }
                    return Ok(result);
                } else {
                    anyhow::bail!("IN clause missing closing parenthesis");
                }
            } else {
                anyhow::bail!("IN clause requires parenthesized list");
            }
        } else {
            anyhow::bail!("IN clause requires value list");
        }
    }

    // Handle IS [NOT] NULL (unary predicate)
    if let Some(i) = find_token_ci(tokens, "IS") {
        let left = parse_arith_expr(&tokens[..i])?;
        let mut j = i + 1;
        let mut neg = false;
        if j < tokens.len() && tokens[j].to_uppercase() == "NOT" { neg = true; j += 1; }
        if j < tokens.len() && tokens[j].to_uppercase() == "NULL" {
            return Ok(WhereExpr::IsNull { expr: left, negated: neg });
        } else {
            anyhow::bail!("IS/IS NOT only supports NULL");
        }
    }

    // Find comparison operator, including LIKE/NOT LIKE
    // First detect NOT LIKE (two-token operator)
    if let Some(i) = tokens.iter().position(|t| t.to_uppercase() == "NOT") {
        if i + 1 < tokens.len() && tokens[i + 1].to_uppercase() == "LIKE" {
            debug!("[PARSE LIKE] Detected NOT LIKE at token position {}, tokens={:?}", i, tokens);
            let left = parse_arith_expr(&tokens[..i])?;
            let right = parse_arith_expr(&tokens[i + 2..])?;
            debug!("[PARSE LIKE] NOT LIKE parsed: left={:?}, right={:?}", left, right);
            return Ok(WhereExpr::Comp { left, op: CompOp::NotLike, right });
        }
    }
    // Then detect single-token LIKE
    if let Some(i) = tokens.iter().position(|t| t.to_uppercase() == "LIKE") {
        debug!("[PARSE LIKE] Detected LIKE at token position {}, tokens={:?}", i, tokens);
        let left = parse_arith_expr(&tokens[..i])?;
        let right = parse_arith_expr(&tokens[i + 1..])?;
        debug!("[PARSE LIKE] LIKE parsed: left={:?}, right={:?}", left, right);
        return Ok(WhereExpr::Comp { left, op: CompOp::Like, right });
    }

    // Fallback to symbolic comparison operators
    let mut cmp_idx: Option<usize> = None;
    let mut cmp_op: Option<CompOp> = None;
    for (i, tok) in tokens.iter().enumerate() {
        let op = match tok.as_str() {
            ">=" => Some(CompOp::Ge),
            "<=" => Some(CompOp::Le),
            "!=" => Some(CompOp::Ne),
            "==" => Some(CompOp::Eq),
            "=" => Some(CompOp::Eq),
            ">" => Some(CompOp::Gt),
            "<" => Some(CompOp::Lt),
            _ => None,
        };
        if let Some(o) = op {
            cmp_idx = Some(i); cmp_op = Some(o); break;
        }
    }
    if let (Some(idx), Some(op)) = (cmp_idx, cmp_op) {
        let left = parse_arith_expr(&tokens[..idx])?;
        let right = parse_arith_expr(&tokens[idx+1..])?;
        return Ok(WhereExpr::Comp { left, op, right });
    }

    // No explicit comparison: treat the entire input as a boolean predicate expression
    // We encode it as `<expr> = 1` and let the executor handle boolean-typed expressions specially.
    let expr = parse_arith_expr(tokens)?;
    Ok(WhereExpr::Comp { left: expr, op: CompOp::Eq, right: ArithExpr::Term(ArithTerm::Number(1.0)) })
}
