use tracing::debug;
use regex::Regex;

use crate::server::query::query_common::*;
use crate::server::query::*;
use crate::server::query::query_parse_select_list::parse_select_list;




pub fn split_union_queries(input: &str) -> Result<(Vec<&str>, bool)> {
    // Split top-level SELECT statements by UNION or UNION ALL, respecting parentheses and quotes.
    let mut parts: Vec<&str> = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let bytes = input.as_bytes();
    let mut depth: i32 = 0;
    let mut in_squote = false;
    let mut in_dquote = false;
    let mut all = false;
    while i + 5 < bytes.len() {
        let c = bytes[i] as char;
        // track quotes and parentheses
        if !in_squote && !in_dquote {
            if c == '(' { depth += 1; i += 1; continue; }
            if c == ')' { depth -= 1; i += 1; continue; }
        }
        if c == '\'' && !in_dquote { in_squote = !in_squote; i += 1; continue; }
        if c == '"' && !in_squote { in_dquote = !in_dquote; i += 1; continue; }
        if depth == 0 && !in_squote && !in_dquote {
            // check for UNION or UNION ALL starting here (case-insensitive)
            let rest = &input[i..].to_uppercase();
            if rest.starts_with(" UNION ALL ") {
                if i > start { parts.push(&input[start..i]); }
                all = true;
                i += " UNION ALL ".len();
                start = i;
                continue;
            } else if rest.starts_with(" UNION ") {
                if i > start { parts.push(&input[start..i]); }
                i += " UNION ".len();
                start = i;
                continue;
            }
        }
        i += 1;
    }
    if start == 0 {
        // No UNION delimiters found; treat the whole input as a single part
        return Ok((vec![input.trim()], false));
    }
    if start < input.len() { parts.push(&input[start..]); }
    Ok((parts.into_iter().map(|p| p.trim()).collect(), all))
}

pub fn parse_select(s: &str) -> Result<Query> {
    debug!("[PARSE SELECT] Starting parse_select with SQL: '{}'", s);
    // Parse optional WITH clause for CTEs
    let mut with_ctes: Option<Vec<CTE>> = None;
    let mut query_sql = s;
    
    let s_up = s.to_uppercase();
    if s_up.trim_start().starts_with("WITH ") {
        // Extract WITH clause and main SELECT
        let with_start = s_up.trim_start().find("WITH ").unwrap();
        let after_with = &s[with_start + 5..].trim();
        
        // Find the main SELECT that follows the CTE definitions
        // CTEs are: name AS (query), name AS (query), ... SELECT ...
        let mut ctes: Vec<CTE> = Vec::new();
        let mut pos = 0usize;
        loop {
            // Skip whitespace
            while pos < after_with.len() && after_with.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
            if pos >= after_with.len() { break; }
            
            // Check if we've reached the main SELECT
            let remaining_up = after_with[pos..].to_uppercase();
            if remaining_up.starts_with("SELECT ") {
                query_sql = &after_with[pos..];
                break;
            }
            
            // Parse CTE: name AS (query)
            // Read CTE name
            let name_start = pos;
            while pos < after_with.len() && !after_with.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
            let cte_name = after_with[name_start..pos].trim().to_string();
            
            // Skip whitespace and expect AS
            while pos < after_with.len() && after_with.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
            let rem_up = after_with[pos..].to_uppercase();
            if !rem_up.starts_with("AS") {
                anyhow::bail!("Expected AS after CTE name");
            }
            pos += 2;
            
            // Skip whitespace and expect (
            while pos < after_with.len() && after_with.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
            if pos >= after_with.len() || after_with.as_bytes()[pos] as char != '(' {
                anyhow::bail!("Expected ( after AS in CTE definition");
            }
            pos += 1;
            
            // Find matching )
            let mut depth = 1;
            let query_start = pos;
            while pos < after_with.len() && depth > 0 {
                let ch = after_with.as_bytes()[pos] as char;
                if ch == '(' { depth += 1; }
                else if ch == ')' { depth -= 1; }
                pos += 1;
            }
            
            if depth != 0 {
                anyhow::bail!("Unmatched parentheses in CTE definition");
            }
            
            let cte_query_sql = after_with[query_start..pos-1].trim();
            let cte_query = parse_select(cte_query_sql)?;
            ctes.push(CTE { name: cte_name, query: Box::new(cte_query) });
            
            // Skip optional comma
            while pos < after_with.len() && after_with.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
            if pos < after_with.len() && after_with.as_bytes()[pos] as char == ',' {
                pos += 1;
            }
        }
        
        if !ctes.is_empty() {
            with_ctes = Some(ctes);
        }
    }
    
    // helper to parse FROM with optional JOINs
    fn parse_from_with_joins(input: &str) -> Result<(TableRef, Vec<JoinClause>)> {
        // Tokenize by whitespace but we need to preserve ON predicate spans; we'll scan manually
        let up = input.to_uppercase();
        let mut i = 0usize;
        let bytes = input.as_bytes();
        // parse base table
        // read first word as name
        fn read_word(s: &str, start: usize) -> (String, usize) {
            let b = s.as_bytes();
            let mut j = start;
            while j < b.len() && !b[j].is_ascii_whitespace() { j += 1; }
            (s[start..j].to_string(), j)
        }
        fn skip_ws(s: &str, mut idx: usize) -> usize { let b = s.as_bytes(); while idx < b.len() && b[idx].is_ascii_whitespace() { idx += 1; } idx }
        i = skip_ws(input, i);
        if i >= input.len() { anyhow::bail!("Missing table after FROM"); }
        
        // Check if base source is a subquery (starts with parenthesis)
        let base = if bytes[i] as char == '(' {
            // Parse subquery: find matching closing parenthesis
            let mut depth = 1;
            let mut j = i + 1;
            while j < input.len() && depth > 0 {
                let ch = bytes[j] as char;
                if ch == '(' { depth += 1; }
                else if ch == ')' { depth -= 1; }
                j += 1;
            }
            if depth != 0 {
                anyhow::bail!("Unmatched parentheses in FROM subquery");
            }
            
            // Extract subquery SQL (without outer parentheses)
            let subquery_sql = input[i+1..j-1].trim();
            
            // Parse the subquery as a SELECT statement
            let subquery = parse_select(subquery_sql)?;
            
            // Subquery MUST have an alias
            j = skip_ws(input, j);
            let rem_up = input[j..].to_uppercase();
            let alias = if rem_up.starts_with("AS ") {
                let k0 = j + 3;
                let (al, k1) = read_word(input, k0);
                j = k1;
                al
            } else {
                // Alias without AS keyword
                let (al, k1) = read_word(input, j);
                j = k1;
                al
            };
            
            if alias.is_empty() {
                anyhow::bail!("Subquery in FROM clause must have an alias");
            }
            
            (TableRef::Subquery { query: Box::new(subquery), alias }, j)
        } else {
            // Regular table name
            let (base_name, mut j) = read_word(input, i);
            
            // Strip quotes from table name
            let mut table_name = base_name.trim();
            if (table_name.starts_with('"') && table_name.ends_with('"')) || (table_name.starts_with('\'') && table_name.ends_with('\'')) {
                if table_name.len() >= 2 {
                    table_name = &table_name[1..table_name.len()-1];
                }
            }
            
            let mut base_alias: Option<String> = None;
            j = skip_ws(input, j);
            let rem_up = up[j..].to_string();
            if rem_up.starts_with("AS ") {
                let k0 = j + 3;
                let (al, k1) = read_word(input, k0);
                base_alias = Some(al);
                j = k1;
            } else if !rem_up.starts_with("INNER ") && !rem_up.starts_with("LEFT ") && !rem_up.starts_with("RIGHT ") && !rem_up.starts_with("OUTER ") && !rem_up.starts_with("FULL ") && !rem_up.starts_with("JOIN ") {
                // treat next word as alias if present
                if j < input.len() {
                    let (al, k1) = read_word(input, j);
                    if !al.is_empty() { base_alias = Some(al); j = k1; }
                }
            }
            (TableRef::Table { name: table_name.to_string(), alias: base_alias.filter(|a| !a.is_empty()) }, j)
        };
        
        let (base, mut j) = base;
        let mut joins: Vec<JoinClause> = Vec::new();
        // loop joins
        loop {
            j = skip_ws(input, j);
            if j >= input.len() { break; }
            let rest_up = input[j..].to_uppercase();
            let mut jt = None;
            let mut adv = 0usize;
            if rest_up.starts_with("INNER ") { jt = Some(JoinType::Inner); adv = 6; }
            else if rest_up.starts_with("LEFT ") { jt = Some(JoinType::Left); adv = 5; }
            else if rest_up.starts_with("RIGHT ") { jt = Some(JoinType::Right); adv = 6; }
            else if rest_up.starts_with("OUTER ") || rest_up.starts_with("FULL ") { jt = Some(JoinType::Full); adv = if rest_up.starts_with("OUTER ") { 6 } else { 5 }; }
            // allow optional leading JOIN keyword without type (default INNER)
            if rest_up.starts_with("JOIN ") { jt = Some(jt.unwrap_or(JoinType::Inner)); adv = 0; }
            if jt.is_none() && !rest_up.starts_with("JOIN ") { break; }
            // consume type token if present (INNER/LEFT/RIGHT/OUTER/FULL)
            if adv > 0 { j += adv; j = skip_ws(input, j); }
            // accept optional OUTER before JOIN (e.g., LEFT OUTER JOIN)
            let rest_after_type = input[j..].to_uppercase();
            if rest_after_type.starts_with("OUTER ") { j += 6; j = skip_ws(input, j); }
            // expect JOIN
            let rest_up2 = input[j..].to_uppercase();
            let join_kw = if rest_up2.starts_with("JOIN ") { 5 } else {
                // Provide a more helpful error message with context
                let ctx = &input[j..input.len().min(j+20)];
                anyhow::bail!("Expected JOIN after join type at position {} near '{}'. Hint: use 'LEFT JOIN' or 'LEFT OUTER JOIN'.", j, ctx);
            };
            j += join_kw;
            j = skip_ws(input, j);
            // right table name
            let (right_name, mut k) = read_word(input, j);
            let mut right_alias: Option<String> = None;
            k = skip_ws(input, k);
            let rem_u = input[k..].to_uppercase();
            if rem_u.starts_with("AS ") {
                let k0 = k + 3; let (al, k1) = read_word(input, k0); right_alias = Some(al); k = k1;
            } else if !rem_u.starts_with("ON ") {
                // alias without AS
                let (al, k1) = read_word(input, k); if !al.is_empty() { right_alias = Some(al); k = k1; }
            }
            // expect ON
            k = skip_ws(input, k);
            let rem_u2 = input[k..].to_uppercase();
            if !rem_u2.starts_with("ON ") {
                let ctx = &input[k..input.len().min(k+20)];
                anyhow::bail!("Expected ON after JOIN table at position {} near '{}'.", k, ctx);
            }
            k += 3;
            // predicate until next JOIN keyword (INNER/LEFT/RIGHT/OUTER/FULL or JOIN) or end
            let up_tail = input[k..].to_uppercase();
            let mut end = input.len();
            // Stop ON at the next JOIN or at the start of the global clauses (WHERE/GROUP BY/HAVING/ORDER BY/LIMIT)
            // Use a regex to handle arbitrary whitespace/newlines and mixed casing.
            if let Ok(re) = Regex::new(r"(?i)\b(INNER|LEFT|RIGHT|OUTER|FULL|JOIN|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT)\b") {
                if let Some(m) = re.find(&up_tail) { end = k + m.start(); }
            }
            let on_str = input[k..end].trim();
            let on = parse_where_expr(on_str)?;
            joins.push(JoinClause { join_type: jt.unwrap_or(JoinType::Inner), right: TableRef::Table { name: right_name.trim().to_string(), alias: right_alias.filter(|a| !a.is_empty()) }, on });
            j = end;
        }
        Ok((base, joins))
    }
    // SELECT ... [FROM db ...]
    // Find keyword at depth 0 (outside parentheses) and outside quotes, case-insensitive.
    fn find_keyword_ci_depth0(s: &str, kw: &str) -> Option<usize> {
        let kw_up = kw.to_uppercase();
        let klen = kw_up.len();
        let sb = s.as_bytes();
        let mut i = 0usize;
        let mut depth: i32 = 0;
        let mut in_squote = false;
        let mut in_dquote = false;
        while i + klen <= sb.len() {
            let ch = sb[i] as char;
            // manage state
            if ch == '\'' && !in_dquote { in_squote = !in_squote; i += 1; continue; }
            if ch == '"' && !in_squote { in_dquote = !in_dquote; i += 1; continue; }
            if !in_squote && !in_dquote {
                if ch == '(' { depth += 1; i += 1; continue; }
                if ch == ')' { depth -= 1; i += 1; continue; }
                if depth == 0 {
                    // compare slice case-insensitively
                    let slice = &s[i..i+klen];
                    if slice.eq_ignore_ascii_case(&kw_up) {
                        // word boundary check
                        let prev_ok = if i == 0 { true } else {
                            let pc = sb[i-1] as char; !(pc.is_alphanumeric() || pc == '_')
                        };
                        let next_ok = if i + klen >= sb.len() { true } else {
                            let nc = sb[i+klen] as char; !(nc.is_alphanumeric() || nc == '_')
                        };
                        if prev_ok && next_ok { return Some(i); }
                    }
                }
            }
            i += 1;
        }
        None
    }

    let from_pos = find_keyword_ci_depth0(query_sql, "FROM");
    debug!(target: "clarium::parser", "parse SELECT: FROM found?={} (sql starts with='{}...')", from_pos.is_some(), &query_sql[..query_sql.len().min(80)]);

    // Sourceless SELECT (e.g., SELECT 1) when no FROM clause is present
    if from_pos.is_none() {
        let sel_fields = query_sql[7..].trim();
        debug!(target: "clarium::parser", "sourceless SELECT detected; fields='{}'", sel_fields);
        let select = parse_select_list(sel_fields)?;
        return Ok(Query {
            select,
            by_window_ms: None,
            by_slices: None,
            group_by_cols: None,
            group_by_notnull_cols: None,
            where_clause: None,
            having_clause: None,
            rolling_window_ms: None,
            order_by: None,
            order_by_hint: None,
            order_by_raw: None,
            limit: None,
            into_table: None,
            into_mode: None,
            base_table: None,
            joins: None,
            with_ctes,
            original_sql: s.trim().to_string(),
        });
    }

    let from_idx = from_pos.ok_or_else(|| anyhow::anyhow!("Missing FROM"))?;
    let (sel_part, rest) = query_sql.split_at(from_idx);
    let sel_fields = sel_part[7..].trim();
    // skip the keyword itself and following whitespace
    let mut rest = &rest[4..];
    rest = rest.trim_start();

    // Parse database name until BY/GROUP BY/WHERE/HAVING or end
    let mut database = rest.trim();
    let mut by_window_ms: Option<i64> = None;
    let mut by_slices: Option<SlicePlan> = None;
    let mut group_by_cols: Option<Vec<String>> = None;
    let mut group_by_notnull_cols: Option<Vec<String>> = None;
    let mut where_clause: Option<WhereExpr> = None;
    let mut having_clause: Option<WhereExpr> = None;
    let mut rolling_window_ms: Option<i64> = None;
    let mut order_by: Option<Vec<(String, bool)>> = None;
    let mut limit: Option<i64> = None;
    let mut order_by_hint: Option<String> = None;
    let mut order_by_raw: Option<Vec<(String, bool)>> = None;
    // Optional INTO target and mode
    let mut into_table: Option<String> = None;
    let mut into_mode: Option<IntoMode> = None;

    // Determine cut for database token
    let up_db = database.to_uppercase();
    let mut cut_idx = up_db.len();
    if let Some(i) = up_db.find(" GROUP BY ") { cut_idx = cut_idx.min(i); }
    if let Some(i) = up_db.find(" ROLLING BY ") { cut_idx = cut_idx.min(i); }
    // find standalone BY (not part of GROUP BY or ROLLING BY)
    if let Some(i_by) = up_db.find(" BY ") {
        let is_group = if i_by >= 6 { &up_db[i_by-6..i_by] == " GROUP" } else { false };
        let is_rolling = if i_by >= 9 { &up_db[i_by-9..i_by] == " ROLLING" } else { false };
        if !is_group && !is_rolling { cut_idx = cut_idx.min(i_by); }
    }
    if let Some(i) = up_db.find(" WHERE ") { cut_idx = cut_idx.min(i); }
    if let Some(i) = up_db.find(" HAVING ") { cut_idx = cut_idx.min(i); }
    if let Some(i) = up_db.find(" ORDER BY ") { cut_idx = cut_idx.min(i); }
    if let Some(i) = up_db.find(" LIMIT ") { cut_idx = cut_idx.min(i); }
    if let Some(i) = up_db.find(" INTO ") { cut_idx = cut_idx.min(i); }
    let mut tail = "";
    if cut_idx < up_db.len() {
        tail = &database[cut_idx..];
        database = &database[..cut_idx];
    }

    // Now iteratively parse optional clauses in any of these orders:
    // WHERE ... GROUP BY ... HAVING ... OR GROUP BY ... WHERE ... HAVING ...
    let mut t = tail.trim_start();
    loop {
        if t.is_empty() { break; }
        let t_up = t.to_uppercase();
        if t_up.starts_with("ROLLING BY ") {
            // ROLLING BY <window>
            let after = &t[11..];
            let after_up = after.to_uppercase();
            let mut win_end = after.len();
            if let Some(i) = after_up.find(" WHERE ") { win_end = win_end.min(i); }
            if let Some(i) = after_up.find(" HAVING ") { win_end = win_end.min(i); }
            if let Some(i) = after_up.find(" GROUP BY ") { win_end = win_end.min(i); }
            rolling_window_ms = Some(parse_window(after[..win_end].trim())?);
            t = after[win_end..].trim_start();
            continue;
        } else if t_up.starts_with("BY ") {
            // window form begins with BY (note: this occurs only if no leading space)
            let after_by = &t[3..];
            let after_trim = after_by.trim_start();
            let after_up = after_trim.to_uppercase();
            if after_up.starts_with("SLICE") {
                // Expect SLICE( ... ) or SLICE{ ... }
                let kw_len = 5; // len("SLICE")
                let (inner, consumed) = extract_slice_block(&after_trim[kw_len..])?;
                let plan = parse_slice(inner)?;
                by_slices = Some(plan);
                // advance t by consumed
                let lead_ws = after_by.len() - after_trim.len();
                let adv = 3 + lead_ws + kw_len + consumed; // include initial BY 
                t = t[adv..].trim_start();
                continue;
            }
            // numeric window e.g. 1s, 5m — only if the next non-space token looks numeric
            let next_tok = after_trim.split_whitespace().next().unwrap_or("");
            if next_tok.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                let mut win_end = after_by.len();
                let after_up2 = after_by.to_uppercase();
                if let Some(i) = after_up2.find(" WHERE ") { win_end = win_end.min(i); }
                if let Some(i) = after_up2.find(" HAVING ") { win_end = win_end.min(i); }
                if let Some(i) = after_up2.find(" GROUP BY ") { win_end = win_end.min(i); }
                by_window_ms = Some(parse_window(after_by[..win_end].trim())?);
                t = after_by[win_end..].trim_start();
                continue;
            }
        } else if t_up.starts_with("GROUP BY ") {
            let after = &t[9..];
            let after_up = after.to_uppercase();
            let mut end = after.len();
            if let Some(i) = after_up.find(" WHERE ") { end = end.min(i); }
            if let Some(i) = after_up.find(" HAVING ") { end = end.min(i); }
            if let Some(i) = after_up.find(" ORDER BY ") { end = end.min(i); }
            if let Some(i) = after_up.find(" LIMIT ") { end = end.min(i); }
            debug!("[PARSE GROUP BY] Raw GROUP BY text: '{}'", &after[..end]);
            // parse columns list between start..end comma-separated, supporting optional NOTNULL modifier per column
            let mut cols: Vec<String> = Vec::new();
            let mut notnull_cols: Vec<String> = Vec::new();
            for raw in after[..end].split(',') {
                let part = raw.trim();
                if part.is_empty() { continue; }
                // allow forms: col, col NOTNULL (case-insensitive)
                let mut tokens = part.split_whitespace();
                if let Some(name) = tokens.next() {
                    let base = name.trim().to_string();
                    let mut is_notnull = false;
                    if let Some(mod1) = tokens.next() {
                        if mod1.eq_ignore_ascii_case("NOTNULL") { is_notnull = true; }
                    }
                    cols.push(base.clone());
                    if is_notnull { notnull_cols.push(base); }
                }
            }
            if cols.is_empty() { anyhow::bail!("Invalid GROUP BY: no columns"); }
            debug!("[PARSE GROUP BY] Parsed columns: {:?}, notnull columns: {:?}", cols, notnull_cols);
            group_by_cols = Some(cols);
            if !notnull_cols.is_empty() { group_by_notnull_cols = Some(notnull_cols); }
            t = after[end..].trim_start();
            continue;
        } else if t_up.starts_with("WHERE ") {
            let after = &t[6..];
            // WHERE may be followed by GROUP BY, HAVING, ORDER BY, or LIMIT
            // But these keywords might also appear inside nested subqueries, so we must respect parenthesis depth
            let after_up = after.to_uppercase();
            let mut end = after.len();
            
            // Helper to find keyword at depth 0 (not inside parentheses)
            let find_at_depth_zero = |haystack: &str, needle: &str| -> Option<usize> {
                let bytes = haystack.as_bytes();
                let needle_bytes = needle.as_bytes();
                let mut depth = 0;
                let mut i = 0;
                while i < bytes.len() {
                    let ch = bytes[i] as char;
                    if ch == '(' { depth += 1; }
                    else if ch == ')' { depth -= 1; }
                    else if depth == 0 && i + needle_bytes.len() <= bytes.len() {
                        // Check for match at current position
                        if &bytes[i..i+needle_bytes.len()] == needle_bytes {
                            return Some(i);
                        }
                    }
                    i += 1;
                }
                None
            };
            
            if let Some(i) = find_at_depth_zero(&after_up, " GROUP BY ") { end = end.min(i); }
            if let Some(i) = find_at_depth_zero(&after_up, " HAVING ") { end = end.min(i); }
            if let Some(i) = find_at_depth_zero(&after_up, " ORDER BY ") { end = end.min(i); }
            if let Some(i) = find_at_depth_zero(&after_up, " LIMIT ") { end = end.min(i); }
            let w_txt = after[..end].trim();
            debug!("[PARSE WHERE] Raw WHERE text: '{}'", w_txt);
            match parse_where_expr(w_txt) {
                Ok(wexpr) => {
                    debug!("[PARSE WHERE] Successfully parsed WHERE: {:?}", wexpr);
                    where_clause = Some(wexpr);
                }
                Err(e) => {
                    debug!("[PARSE WHERE] WHERE parse error: {}", e);
                    eprintln!("[PARSER dbg] WHERE parse error: {}\nSQL: '{}'", e, w_txt);
                    where_clause = None;
                }
            }
            t = after[end..].trim_start();
            continue;
        } else if t_up.starts_with("HAVING ") {
            let after = &t[7..];
            // HAVING may be followed by ORDER BY or LIMIT; do not consume the tail
            let after_up = after.to_uppercase();
            let mut end = after.len();
            if let Some(i) = after_up.find(" ORDER BY ") { end = end.min(i); }
            if let Some(i) = after_up.find(" LIMIT ") { end = end.min(i); }
            // Extract only the HAVING predicate text
            let h_txt = after[..end].trim();
            having_clause = parse_where_expr(h_txt).ok();
            // Advance t past the HAVING predicate and continue parsing remaining clauses
            t = after[end..].trim_start();
            continue;
        } else if t_up.starts_with("ORDER BY ") {
            // ORDER BY col [ASC|DESC], col2 ...
            let after = &t[9..];
            let after_up = after.to_uppercase();
            let mut end = after.len();
            if let Some(i) = after_up.find(" LIMIT ") { end = end.min(i); }
            // Allow ORDER BY to be the last clause, so no further trims
            let mut inside = after[..end].trim().to_string();
            // Optional trailing USING ANN|EXACT hint
            {
                let up_inside = inside.to_uppercase();
                if let Some(pos) = up_inside.rfind(" USING ") {
                    // Ensure it is a trailing hint: only whitespace after ANN|EXACT
                    let hint_part = inside[pos + 7..].trim(); // after ' USING '
                    let hint_up = hint_part.to_uppercase();
                    if hint_up == "ANN" || hint_up == "EXACT" {
                        order_by_hint = Some(hint_up.to_lowercase());
                        // strip the hint from inside
                        inside = inside[..pos].trim_end().to_string();
                    }
                }
            }
            let mut list: Vec<(String, bool)> = Vec::new();
            let mut raw_list: Vec<(String, bool)> = Vec::new();
            // Split by comma respecting parenthesis depth and quotes to handle function calls correctly
            let mut parts: Vec<String> = Vec::new();
            let mut buf = String::new();
            let mut depth = 0i32;
            let mut in_squote = false;
            let mut in_dquote = false;
            for ch in inside.chars() {
                match ch {
                    '\'' if !in_dquote => { in_squote = !in_squote; buf.push(ch); }
                    '"' if !in_squote => { in_dquote = !in_dquote; buf.push(ch); }
                    '(' if !in_squote && !in_dquote => { depth += 1; buf.push(ch); }
                    ')' if !in_squote && !in_dquote => { depth -= 1; buf.push(ch); }
                    ',' if depth == 0 && !in_squote && !in_dquote => { parts.push(buf.trim().to_string()); buf.clear(); }
                    _ => buf.push(ch),
                }
            }
            if !buf.is_empty() { parts.push(buf.trim().to_string()); }
            // If ANN/EXACT hint was attached to the first key (e.g., "expr USING ANN, id DESC"),
            // detect and strip it here and record the hint.
            if order_by_hint.is_none() {
                if let Some(first) = parts.get_mut(0) {
                    let up0 = first.to_uppercase();
                    if up0.ends_with(" USING ANN") {
                        // strip the suffix
                        let new_len = first.len() - " USING ANN".len();
                        *first = first[..new_len].trim_end().to_string();
                        order_by_hint = Some("ann".to_string());
                    } else if up0.ends_with(" USING EXACT") {
                        let new_len = first.len() - " USING EXACT".len();
                        *first = first[..new_len].trim_end().to_string();
                        order_by_hint = Some("exact".to_string());
                    }
                }
            }
            for raw in parts {
                let mut p = raw.trim().to_string();
                if p.is_empty() { continue; }
                // Strip trailing ASC/DESC at depth 0 (outside parentheses and quotes)
                let mut asc = true;
                {
                    let mut d: i32 = 0;
                    let mut s_in = false;
                    let mut d_in = false;
                    for ch in p.chars() {
                        match ch {
                            '\'' if !d_in => s_in = !s_in,
                            '"' if !s_in => d_in = !d_in,
                            '(' if !s_in && !d_in => d += 1,
                            ')' if !s_in && !d_in => d -= 1,
                            _ => {}
                        }
                    }
                    if d == 0 && !s_in && !d_in {
                        let tail_up = p.to_uppercase();
                        // remove once; only handle simple suffix forms
                        if tail_up.ends_with(" DESC") {
                            let cut = p.len() - 5; // len(" DESC")
                            p = p[..cut].trim_end().to_string();
                            asc = false;
                        } else if tail_up.ends_with(" ASC") {
                            let cut = p.len() - 4; // len(" ASC")
                            p = p[..cut].trim_end().to_string();
                            asc = true;
                        }
                    }
                }
                let expr_txt = p.trim().to_string();
                // Preserve raw expression for advanced planners (e.g., ANN)
                raw_list.push((expr_txt.clone(), asc));
                // Determine if this is a bare identifier (no parens, spaces, or quotes)
                let is_simple_ident = !expr_txt.contains('(')
                    && !expr_txt.contains(')')
                    && !expr_txt.contains(' ')
                    && !expr_txt.contains('\t')
                    && !expr_txt.contains('"')
                    && !expr_txt.contains('\'');
                if is_simple_ident {
                    let normalized_name = crate::ident::normalize_identifier(&expr_txt);
                    list.push((normalized_name, asc));
                } else {
                    // Keep full expression; downstream decides how to handle it
                    list.push((expr_txt, asc));
                }
            }
            if list.is_empty() { anyhow::bail!("Invalid ORDER BY: empty list"); }
            if order_by.is_some() { anyhow::bail!("Duplicate ORDER BY clause"); }
            order_by = Some(list);
            order_by_raw = Some(raw_list);
            t = after[end..].trim_start();
            continue;
        } else if t_up.starts_with("LIMIT ") {
            let after = &t[6..];
            let mut num_txt = String::new();
            let mut chars = after.chars();
            if let Some(first) = chars.next() {
                if first == '-' || first.is_ascii_digit() { num_txt.push(first); }
            }
            for ch in chars {
                if ch.is_ascii_digit() { num_txt.push(ch); } else { break; }
            }
            // num_txt may be just "-" or empty if malformed
            if num_txt.is_empty() || num_txt == "-" { anyhow::bail!("Invalid LIMIT: expected integer"); }
            let n: i64 = num_txt.parse().map_err(|_| anyhow::anyhow!("Invalid LIMIT value"))?;
            if limit.is_some() { anyhow::bail!("Duplicate LIMIT clause"); }
            limit = Some(n);
            // advance t by consumed length
            let consumed = 6 + num_txt.len();
            t = t[consumed..].trim_start();
            continue;
        } else if t_up.starts_with(" BY ") {
            // leading space variant
            t = &t[1..];
            continue;
        } else if t_up.starts_with(" GROUP BY ") {
            t = &t[1..];
            continue;
        } else if t_up.starts_with(" WHERE ") {
            t = &t[1..];
            continue;
        } else if t_up.starts_with(" HAVING ") {
            t = &t[1..];
            continue;
        } else if t_up.starts_with(" INTO ") || t_up.starts_with("INTO ") {
            // Parse: INTO <table> [APPEND|REPLACE]
            // Accept both with/without leading space
            let after = if t_up.starts_with(" INTO ") { &t[6..] } else { &t[5..] };
            let after = after.trim_start();
            // split once on whitespace to separate table and optional mode
            let mut parts = after.splitn(2, char::is_whitespace);
            let tbl = parts.next().unwrap_or("").trim();
            if tbl.is_empty() { anyhow::bail!("Invalid INTO: missing table name"); }
            into_table = Some(tbl.to_string());
            if let Some(rest) = parts.next() {
                let mode_tok = rest.split_whitespace().next().unwrap_or("").to_uppercase();
                if !mode_tok.is_empty() {
                    into_mode = Some(match mode_tok.as_str() { "APPEND" => IntoMode::Append, "REPLACE" => IntoMode::Replace, other => { anyhow::bail!("Invalid INTO mode: {} (expected APPEND or REPLACE)", other); } });
                    // consume the mode token (rest of string is ignored)
                }
            }
            // nothing else should follow INTO; break
            t = "";
            break;
        } else {
            break;
        }
    }

    // Finalize
    let from_clause = database.trim().to_string();
    let mut base_table: Option<TableRef> = None;
    let mut joins: Option<Vec<JoinClause>> = None;
    // Initialize database with from_clause; may strip alias later
    let mut database = from_clause.clone();
    
    // Always use parse_from_with_joins to handle both tables and subqueries
    // This function now supports subqueries starting with '(' as well as regular tables
    let (base, js) = parse_from_with_joins(&from_clause)?;
    base_table = Some(base);
    
    // If there are actual joins, store them and clear database to signal join path
    if !js.is_empty() {
        joins = Some(js);
        database = String::new();
    } else {
        // No joins: set database to the table name for simple FROM (ignore for subqueries)
        if let Some(TableRef::Table { name, .. }) = &base_table {
            database = name.clone();
        } else {
            // Subquery without joins: clear database
            database = String::new();
        }
        joins = None;
    }

    let select = parse_select_list(sel_fields)?;

    // Forbid both BY and GROUP BY
    if (by_window_ms.is_some() || by_slices.is_some()) && group_by_cols.is_some() {
        anyhow::bail!("BY and GROUP BY cannot be used together");
    }

    Ok(Query { select, by_window_ms, by_slices, group_by_cols, group_by_notnull_cols, where_clause, having_clause, rolling_window_ms, order_by, order_by_hint, order_by_raw, limit, into_table, into_mode, base_table, joins, with_ctes, original_sql: s.trim().to_string() })
}