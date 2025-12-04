use crate::server::query::query_common::*;
use crate::server::query::*;


// --- SLICE parser ---
pub fn parse_slice(input: &str) -> Result<SlicePlan> {
    let s = input.trim();
    let up = s.to_uppercase();
    let mut pos = 0usize;
    // Expect leading SLICE
    let upb = up.as_bytes();
    if up.starts_with("SLICE") {
        pos = 5;
        // skip following whitespace
        while pos < upb.len() && upb[pos].is_ascii_whitespace() { pos += 1; }
    }
    // Expect USING or nested SLICE
    let rest = &s[pos..].trim_start();
    let rest_up = rest.to_uppercase();
    let mut cursor = 0usize;
    if rest_up.starts_with("USING ") {
        cursor = 6;
    } else if rest_up.starts_with("USING") {
        cursor = 5;
    } else if rest_up.starts_with("SLICE") {
        // Allow: SLICE SLICE(...) as full plan (nested grouping via SLICE only)
        let kw_len = 5;
        let (inner, consumed) = extract_slice_block(&rest[kw_len..])?;
        let inner_plan = parse_slice(inner)?;
        let mut clauses: Vec<SliceClause> = Vec::new();
        let mut tail = &rest[kw_len + consumed..];
        // parse subsequent clauses
        while !tail.trim().is_empty() {
            let (cl, used) = parse_slice_clause(tail.trim())?;
            clauses.push(cl);
            tail = &tail[used..];
        }
        return Ok(SlicePlan { base: SliceSource::Plan(Box::new(inner_plan)), clauses, labels: None });
    } else {
        anyhow::bail!("SLICE expects USING or SLICE(...)");
    }
    let mut tail = &rest[cursor..];

    // Optional LABELS(...) immediately after USING
    let mut labels: Option<Vec<String>> = None;
    let tail_upcase = tail.to_uppercase();
    let mut consumed_labels = 0usize;
    if tail_upcase.trim_start().starts_with("LABELS") {
        // find opening paren after LABELS
        let after = tail.trim_start();
        let idx = 6; // after LABELS
        let after_up = after.to_uppercase();
        if !after_up[idx..].trim_start().starts_with('(') {
            anyhow::bail!("LABELS expects (name, ...)");
        }
        // reposition to just after LABELS
        let mut p = 6;
        while p < after.len() && after.as_bytes()[p].is_ascii_whitespace() { p += 1; }
        if p >= after.len() || after.as_bytes()[p] != b'(' { anyhow::bail!("LABELS expects ( ... )"); }
        // extract until matching ')'
        let mut depth = 0i32; let mut j = p;
        while j < after.len() {
            let ch = after[j..].chars().next().unwrap();
            if ch == '(' { depth += 1; }
            else if ch == ')' { depth -= 1; if depth == 0 { j += ch.len_utf8(); break; } }
            j += ch.len_utf8();
        }
        let inside = &after[p+1..j-1];
        let mut names: Vec<String> = Vec::new();
        for part in inside.split(',') { let n = part.trim().trim_matches('"').trim_matches('\'').trim().to_string(); if !n.is_empty() { names.push(n); } }
        labels = Some(names);
        consumed_labels = after[..j].len();
        tail = &after[j..];
    }

    let t0 = tail.trim_start();
    let (base_src, used) = parse_slice_source(t0)?;
    let lead_ws0 = tail.len() - t0.len();
    tail = &tail[lead_ws0 + used..];
    let mut clauses: Vec<SliceClause> = Vec::new();
    loop {
        let t = tail.trim_start();
        if t.is_empty() { break; }
        let up = t.to_uppercase();
        if !(up.starts_with("INTERSECT") || up.starts_with("UNION")) {
            break;
        }
        let (cl, used2) = parse_slice_clause(t)?;
        clauses.push(cl);
        // Map used2 (relative to trimmed t) back to original tail by accounting for leading whitespace
        let lead_ws = tail.len() - tail.trim_start().len();
        let adv = lead_ws + used2;
        tail = &tail[adv..];
    }
    Ok(SlicePlan { base: base_src, clauses, labels })
}

pub fn parse_slice_clause(s: &str) -> Result<(SliceClause, usize)> {
    let up = s.to_uppercase();
    let mut op: Option<SliceOp> = None;
    let mut offset = 0usize;
    if up.starts_with("INTERSECT ") { op = Some(SliceOp::Intersect); offset = 10; }
    else if up.starts_with("INTERSECT") { op = Some(SliceOp::Intersect); offset = 9; }
    else if up.starts_with("UNION ") { op = Some(SliceOp::Union); offset = 6; }
    else if up.starts_with("UNION") { op = Some(SliceOp::Union); offset = 5; }
    else { anyhow::bail!("Expected INTERSECT or UNION"); }
    let rest = s[offset..].trim_start();
    // Nested grouped plan? Accept only SLICE(...)
    let rest_up = rest.to_uppercase();
    if rest_up.starts_with("SLICE") {
        let kw_len = 5;
        let (inner, consumed) = extract_slice_block(&rest[kw_len..])?;
        let plan = parse_slice(inner)?;
        let used = offset + (rest.len() - rest[kw_len+consumed..].len());
        return Ok((SliceClause{ op: op.unwrap(), source: SliceSource::Plan(Box::new(plan)) }, used));
    }
    let (src, used2) = parse_slice_source(rest)?;
    let used = offset + (rest.len() - rest[used2..].len());
    Ok((SliceClause{ op: op.unwrap(), source: src }, used))
}

pub fn parse_slice_source(s: &str) -> Result<(SliceSource, usize)> {
    let up = s.to_uppercase();
    let mut i = 0usize;
    let bytes = s.as_bytes();
    // manual inline rows? starts with '(' possibly nested ((row1),(row2)) or single (row)
    let st = s.trim_start();
    if !st.is_empty() && st.as_bytes()[0] == b'(' {
        if let Some((manual, used)) = parse_manual_rows(st)? {
            // Map used back to original s by accounting for trimmed prefix
            let lead_ws = s.len() - st.len();
            return Ok((manual, lead_ws + used));
        }
    }
    // read identifier token (respect quotes)
    if i >= bytes.len() { anyhow::bail!("Expected table identifier"); }
    let start_i = i;
    let mut in_quote = false;
    let mut quote_ch: u8 = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_quote {
            if b == quote_ch { in_quote = false; }
            i += 1; continue;
        }
        if b == b'\'' || b == b'"' { in_quote = true; quote_ch = b; i += 1; continue; }
        if b.is_ascii_whitespace() { break; }
        i += 1;
    }
    let ident = s[start_i..i].trim();
    let mut start_col: Option<String> = None;
    let mut end_col: Option<String> = None;
    let mut where_clause: Option<WhereExpr> = None;
    let mut label_values: Option<Vec<String>> = None;
    // tail after ident
    let mut tail = s[i..].to_string();
    // Parse optional ON
    let tail_up = tail.to_uppercase();
    let mut advanced = 0usize;
    if let Some(idx) = tail_up.find(" ON ") {
        // after ON read two tokens
        let after = &tail[idx+4..];
        let mut it = after.split_whitespace();
        if let Some(a) = it.next() { start_col = Some(a.trim_matches('"').to_string()); }
        if let Some(b) = it.next() { end_col = Some(b.trim_matches('"').to_string()); }
        advanced = idx + 4;
        // advance past the two tokens in original tail
        let mut consumed = 0usize;
        let mut cnt = 0;
        for (j, ch) in after.char_indices() {
            if ch.is_whitespace() { continue; }
            // consume a token
            let k = j + after[j..].find(char::is_whitespace).unwrap_or(after.len()-j);
            consumed = k;
            cnt += 1;
            if cnt >= 2 { break; }
        }
        advanced += consumed;
        tail = after[consumed..].to_string();
    }
    let mut t2 = tail;
    // Optional WHERE/FILTER for this source; capture only if it appears before the next UNION/INTERSECT
    let t2_up = t2.to_uppercase();
    let next_clause_pos = find_next_keyword(&t2, [" INTERSECT ", " UNION "].as_slice());
    let mut found_filter = None;
    if let Some(iw) = t2_up.find("WHERE ") { found_filter = Some((iw, 5)); }
    else if let Some(iflt) = t2_up.find("FILTER ") { found_filter = Some((iflt, 6)); }
    if let Some((pos_kw, kw_len)) = found_filter {
        // Ensure WHERE/FILTER belongs to this source (i.e., occurs before the next INTERSECT/UNION keyword)
        if next_clause_pos.map(|p| pos_kw < p).unwrap_or(true) {
            let after = &t2[pos_kw + kw_len + 1..]; // skip keyword and following space
            // find end marker starting from 'after'
            let end_idx_rel = find_next_keyword(after, [" INTERSECT ", " UNION "].as_slice()).unwrap_or(after.len());
            let expr_txt = after[..end_idx_rel].trim();
            where_clause = Some(parse_where_expr(expr_txt)?);
            // Reconstruct remaining tail of this slice source (everything after the WHERE expression)
            t2 = after[end_idx_rel..].to_string();
            advanced = s.len() - t2.len();
        }
    }
    // Parse optional LABEL(...) clause with positional label expressions to avoid clashes with WHERE/FILTER
    // Only consider content before the next INTERSECT/UNION when looking for LABEL or legacy patterns
    let rem_all = t2.trim_start();
    if !rem_all.is_empty() {
        // Determine boundary to next clause based on the original (untrimmed) tail to avoid missing leading-space keywords
        let next_pos_full = find_next_keyword(&t2, [" INTERSECT ", " UNION "].as_slice());
        let lead_ws = t2.len() - rem_all.len();
        let next_pos = next_pos_full.map(|p| p.saturating_sub(lead_ws)).unwrap_or(rem_all.len());
        let cutoff = next_pos.min(rem_all.len());
        let rem = &rem_all[..cutoff];
        let up = rem.to_uppercase();
        if up.starts_with("LABEL") {
            // Expect '(' ... ')'
            let after = &rem[5..].trim_start();
            if after.is_empty() || (after.as_bytes()[0] != b'(' && after.as_bytes()[0] != b'{') { anyhow::bail!("LABEL expects (expr, ...)"); }
            let open = after.as_bytes()[0] as char;
            let close = if open == '(' { ')' } else { '}' };
            let mut depth = 0i32; let mut j = 0usize; let mut i0 = 0usize; let mut started = false;
            for (idx, ch) in after.char_indices() {
                if ch == open { depth += 1; if !started { started = true; i0 = idx + ch.len_utf8(); } }
                else if ch == close { depth -= 1; if depth == 0 { j = idx; break; } }
            }
            if !started || depth != 0 { anyhow::bail!("Unterminated LABEL(...) block"); }
            let inside = &after[i0..j];
            let mut vals: Vec<String> = Vec::new();
            for part in inside.split(',') {
                let p = part.trim();
                if p.is_empty() { vals.push(String::new()); continue; }
                vals.push(p.to_string());
            }
            label_values = Some(vals);
            // advance t2 to after the LABEL(...) block (keep any following clauses intact)
            let consumed = 5 + (after[..=j].len());
            // Account for leading whitespace trimmed in rem_all
            let lead_ws = t2.len() - rem_all.len();
            t2 = t2[(lead_ws + consumed)..].to_string();
        } else {
            // If legacy name=expr is detected within the immediate suffix (not across next clause), give a helpful error
            if rem.contains('=') {
                anyhow::bail!("Per-source labels now use LABEL(expr, ...) instead of name=expr. Declare names once in USING LABELS(...).");
            }
        }
    }
    let used = if advanced == 0 { s.len() - t2.len() } else { advanced };
    Ok((SliceSource::Table { database: ident.to_string(), start_col, end_col, where_clause, label_values }, used))
}
