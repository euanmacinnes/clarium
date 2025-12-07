use crate::server::query::query_common::*;
use crate::server::query::*;

pub fn parse_arith_expr(tokens: &[String]) -> Result<ArithExpr> {
    // Helper: parse date part keyword
    fn parse_part(s: &str) -> Option<DatePart> {
        match s.to_uppercase().as_str() {
            "YEAR" => Some(DatePart::Year),
            "MONTH" => Some(DatePart::Month),
            "DAY" => Some(DatePart::Day),
            "HOUR" => Some(DatePart::Hour),
            "MINUTE" => Some(DatePart::Minute),
            "SECOND" => Some(DatePart::Second),
            "MILLISECOND" | "MS" => Some(DatePart::Millisecond),
            _ => None,
        }
    }

    // Turn whitespace-split tokens into a single string, then tokenize char-by-char to support parentheses
    let src = tokens.join(" ");

    // Detect scalar subquery of the form: (SELECT ...)
    // This is commonly used as a scalar RHS for functions, e.g., cosine_sim(x,(SELECT v FROM q))
    // We only treat the whole expression as a scalar subquery when it is exactly wrapped once by parentheses
    // and starts with SELECT (case-insensitive) inside.
    {
        let s = src.trim();
        if s.starts_with('(') && s.ends_with(')') {
            // Check matching outer parentheses
            let mut depth: i32 = 0;
            let mut ok_outer = false;
            for (i, ch) in s.chars().enumerate() {
                if ch == '(' { depth += 1; }
                else if ch == ')' { depth -= 1; if depth == 0 { ok_outer = i == s.len()-1; break; } }
            }
            if ok_outer {
                let inner = &s[1..s.len()-1];
                if inner.trim_start().to_uppercase().starts_with("SELECT ") {
                    return Ok(ArithExpr::Call { name: "SCALAR_SUBQUERY".to_string(), args: vec![ArithExpr::Term(ArithTerm::Str(inner.to_string()))] });
                }
            }
        }
    }

    // Detect top-level comparison expressions (including LIKE / NOT LIKE) and wrap as a predicate
    // This enables using boolean comparisons inside SELECT expressions, e.g., `SELECT 'a' LIKE 'a%' AS ok`.
    // We scan respecting parentheses and single-quoted strings.
    {
        fn split_top_level_comparison(s: &str) -> Option<(String, CompOp, String)> {
            let up = s.to_uppercase();
            let mut depth: i32 = 0;
            let mut in_str = false;
            let mut i = 0usize;
            let chars: Vec<char> = s.chars().collect();
            let upchars: Vec<char> = up.chars().collect();
            let n = chars.len();

            // helper to match a keyword at i (case-insensitive) and ensure separations by whitespace/parens
            fn match_kw(upchars: &[char], i: usize, n: usize, kw: &str, depth: i32, in_str: bool, op: CompOp) -> Option<(usize, usize, CompOp)> {
                let kw_len = kw.len();
                if i + kw_len > n { return None; }
                let seg: String = upchars[i..i+kw_len].iter().collect();
                if seg == kw {
                    // ensure not inside string/paren and boundaries are reasonable
                    if depth == 0 && !in_str {
                        return Some((i, i + kw_len, op));
                    }
                }
                None
            }

            while i < n {
                let ch = chars[i];
                if in_str {
                    if ch == '\'' {
                        // handle escaped '' as one quote
                        if i + 1 < n && chars[i + 1] == '\'' { i += 2; continue; }
                        in_str = false; i += 1; continue;
                    }
                    i += 1; continue;
                }
                match ch {
                    '\'' => { in_str = true; i += 1; continue; }
                    '(' => { depth += 1; i += 1; continue; }
                    ')' => { depth -= 1; i += 1; continue; }
                    _ => {}
                }
                // Try NOT LIKE (must check before LIKE)
                if let Some((sidx, eidx, op)) = match_kw(&upchars, i, n, " NOT LIKE ", depth, in_str, CompOp::NotLike) {
                    let left = s[..sidx].trim().to_string();
                    let right = s[eidx..].trim().to_string();
                    return Some((left, op, right));
                }
                if let Some((sidx, eidx, op)) = match_kw(&upchars, i, n, " LIKE ", depth, in_str, CompOp::Like) {
                    let left = s[..sidx].trim().to_string();
                    let right = s[eidx..].trim().to_string();
                    return Some((left, op, right));
                }
                // Symbolic operators (check multi-char before single-char)
                // >=, <=, !=, ==
                if i + 2 <= n {
                    let seg: String = chars[i..i+2].iter().collect();
                    let op = match seg.as_str() { ">=" => Some(CompOp::Ge), "<=" => Some(CompOp::Le), "!=" => Some(CompOp::Ne), "==" => Some(CompOp::Eq), _ => None };
                    if let Some(o) = op { if depth == 0 { let left = s[..i].trim().to_string(); let right = s[i+2..].trim().to_string(); return Some((left, o, right)); } }
                }
                // single-char: =, >, <
                let op = match ch { '=' => Some(CompOp::Eq), '>' => Some(CompOp::Gt), '<' => Some(CompOp::Lt), _ => None };
                if let Some(o) = op { if depth == 0 { let left = s[..i].trim().to_string(); let right = s[i+1..].trim().to_string(); return Some((left, o, right)); } }
                i += 1;
            }
            None
        }
        if let Some((l, op, r)) = split_top_level_comparison(&src) {
            if let (Some(le), Some(re)) = (super_parse_arith(&l), super_parse_arith(&r)) {
                return Ok(ArithExpr::Predicate(Box::new(WhereExpr::Comp { left: le, op, right: re })));
            }
        }
    }

    let bytes = src.as_bytes();
    let mut i = 0usize;

    #[derive(Clone, Debug)]
    enum ATok { LParen, RParen, Op(ArithOp), Val(ArithExpr) }

    let mut toks: Vec<ATok> = Vec::new();

    // util: skip spaces
    let skip_ws = |i: &mut usize| { while *i < bytes.len() && bytes[*i].is_ascii_whitespace() { *i += 1; } };

    // util: peek next non-ws byte
    let peek_nonws = |j: usize| -> Option<u8> {
        let mut k = j; while k < bytes.len() { if !bytes[k].is_ascii_whitespace() { return Some(bytes[k]); } k += 1; } None
    };

    // Parse a possibly nested function DATEPART/DATEADD/DATEDIFF starting at name position (i at start of name)
    fn parse_func(src: &str, start: usize) -> Option<(ArithExpr, usize)> {
        let s = &src[start..];
        let upper = s.to_uppercase();
        let name_end = s.find('(')?;
        let name = upper[..name_end].trim();
        // find matching ')' for the opening at name_end
        let mut depth: i32 = 0;
        let mut j = name_end;
        let sbytes = s.as_bytes();
        while j < s.len() {
            let ch = s[j..].chars().next().unwrap();
            if ch == '(' { depth += 1; }
            else if ch == ')' { depth -= 1; if depth == 0 { j += ch.len_utf8(); break; } }
            j += ch.len_utf8();
        }
        if depth != 0 { return None; }
        let inside = &s[name_end+1..j-1];
        // Helper: split by commas at top level, respecting quotes
        let mut args: Vec<String> = Vec::new();
        let mut buf = String::new();
        let mut d = 0i32;
        let mut in_quote = false;
        let mut quote_char = ' ';
        let mut k = 0usize;
        while k < inside.len() {
            let ch = inside[k..].chars().next().unwrap();
            if (ch == '\'' || ch == '"') && !in_quote {
                in_quote = true;
                quote_char = ch;
                buf.push(ch);
            } else if in_quote && ch == quote_char {
                in_quote = false;
                buf.push(ch);
            } else if !in_quote && ch == '(' {
                d += 1;
                buf.push(ch);
            } else if !in_quote && ch == ')' {
                d -= 1;
                buf.push(ch);
            } else if !in_quote && ch == ',' && d == 0 {
                args.push(buf.trim().to_string());
                buf.clear();
            } else {
                buf.push(ch);
            }
            k += ch.len_utf8();
        }
        if !buf.trim().is_empty() { args.push(buf.trim().to_string()); }
        match name {
            "DATEPART" => {
                if args.len() != 2 { return None; }
                let part = parse_part(args[0].trim())?;
                let a1 = super_parse_arith(&args[1])?;
                Some((ArithExpr::Func(DateFunc::DatePart(part, Box::new(a1))), start + j))
            }
            "DATEADD" => {
                if args.len() != 3 { return None; }
                let part = parse_part(args[0].trim())?;
                let a_n = super_parse_arith(&args[1])?;
                let a_d = super_parse_arith(&args[2])?;
                Some((ArithExpr::Func(DateFunc::DateAdd(part, Box::new(a_n), Box::new(a_d))), start + j))
            }
            "DATEDIFF" => {
                if args.len() != 3 { return None; }
                let part = parse_part(args[0].trim())?;
                let a1 = super_parse_arith(&args[1])?;
                let a2 = super_parse_arith(&args[2])?;
                Some((ArithExpr::Func(DateFunc::DateDiff(part, Box::new(a1), Box::new(a2))), start + j))
            }
            "CONCAT" => {
                if args.is_empty() { return None; }
                let mut parts: Vec<ArithExpr> = Vec::with_capacity(args.len());
                for a in args.iter() {
                    if let Some(e) = super_parse_arith(a) { parts.push(e); } else { return None; }
                }
                Some((ArithExpr::Concat(parts), start + j))
            }
            "EXTRACT" => {
                // EXTRACT(field FROM expr) - parse specially
                // inside should be "field FROM expr"
                let inside_up = inside.to_uppercase();
                if let Some(from_pos) = inside_up.find(" FROM ") {
                    let field = inside[..from_pos].trim();
                    let expr_str = inside[from_pos + 6..].trim(); // +6 for " FROM "
                    // Create Call with field as string and expr as second arg
                    let field_expr = ArithExpr::Term(ArithTerm::Str(field.to_string()));
                    let value_expr = super_parse_arith(expr_str)?;
                    let call_expr = ArithExpr::Call {
                        name: "extract".to_string(),
                        args: vec![field_expr, value_expr],
                    };
                    Some((call_expr, start + j))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    // Recursive parse entry used by parse_func to parse inner arithmetic from string
    fn super_parse_arith(s: &str) -> Option<ArithExpr> {
        let t: Vec<String> = s.split_whitespace().map(|x| x.to_string()).collect();
        parse_arith_expr(&t).ok()
    }

    while i < bytes.len() {
        skip_ws(&mut i);
        if i >= bytes.len() { break; }
        let c = bytes[i] as char;
        match c {
            '(' => {
                // Parenthesized expression: parse until matching ')' and treat as a grouped sub-expression
                let mut depth: i32 = 0; let mut j = i;
                while j < bytes.len() {
                    let ch = src[j..].chars().next().unwrap();
                    if ch == '(' { depth += 1; }
                    else if ch == ')' { depth -= 1; if depth == 0 { j += ch.len_utf8(); break; } }
                    j += ch.len_utf8();
                }
                // Extract inside and parse recursively
                let inside = &src[i+1..j-1];
                let inner = super_parse_arith(inside).ok_or_else(|| anyhow::anyhow!("Invalid parenthesized expression"))?;
                let mut base = inner;
                // Support chained PostgreSQL casts after the closing paren: (expr)::type[::type2...]
                loop {
                    let mut kcast = j; while kcast < bytes.len() && bytes[kcast].is_ascii_whitespace() { kcast += 1; }
                    if kcast + 1 < bytes.len() && bytes[kcast] as char == ':' && bytes[kcast+1] as char == ':' {
                        kcast += 2; while kcast < bytes.len() && bytes[kcast].is_ascii_whitespace() { kcast += 1; }
                        if let Some((ty, consumed)) = parse_type_name(&src[kcast..]) {
                            base = ArithExpr::Cast { expr: Box::new(base), ty };
                            j = kcast + consumed;
                            continue;
                        }
                    }
                    break;
                }
                // Optional slice suffix after parenthesized/casted expression
                let mut ii2 = j; while ii2 < bytes.len() && bytes[ii2].is_ascii_whitespace() { ii2 += 1; }
                if ii2 < bytes.len() && (bytes[ii2] as char) == '[' {
                    let mut k3 = ii2 + 1; let mut inside2 = String::new(); let mut closed2 = false;
                    while k3 < bytes.len() { let ch2 = bytes[k3] as char; k3 += 1; if ch2 == ']' { closed2 = true; break; } inside2.push(ch2); }
                    if !closed2 { anyhow::bail!("Unclosed slice bracket"); }
                    let parts2: Vec<String> = inside2.split(':').map(|p| p.trim().to_string()).collect();
                    if parts2.len() < 2 || parts2.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                    fn parse_bound2(txt: &str) -> Option<StrSliceBound> {
                        if txt.is_empty() { return None; }
                        if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                        let t = txt.trim();
                        let (include, lit_txt) = if t.starts_with("-'") && t.ends_with("'") { (false, &t[1..]) } else { (true, t) };
                        if lit_txt.starts_with('\'') && lit_txt.ends_with('\'') {
                            let inner = &lit_txt[1..lit_txt.len()-1];
                            return Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Str(inner.to_string()))), include });
                        }
                        Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Col { name: t.to_string(), previous: false })), include: true })
                    }
                    let start_b = parse_bound2(&parts2[0]);
                    let stop_b = parse_bound2(&parts2[1]);
                    let step_v = if parts2.len() == 3 { let p = parts2[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) } } else { None };
                    let slice_expr = ArithExpr::Slice { base: Box::new(base), start: start_b, stop: stop_b, step: step_v };
                    toks.push(ATok::Val(slice_expr));
                    i = k3; // after ']'
                } else {
                    toks.push(ATok::Val(base));
                    i = j; // after ')'/casts
                }
            },
            ')' => { toks.push(ATok::RParen); i += 1; },
            '+' => { toks.push(ATok::Op(ArithOp::Add)); i += 1; },
            '-' => {
                // Negative number literal if followed by digit
                if i+1 < bytes.len() {
                    let next = peek_nonws(i+1);
                    if let Some(nc) = next { if (nc as char).is_ascii_digit() { 
                        // parse number starting at i
                        let mut j = i; // include '-' sign
                        j += 1; // consume '-'
                        // skip ws between - and number
                        while j < bytes.len() && bytes[j].is_ascii_whitespace() { j += 1; }
                        let start = j;
                        let mut dot = false;
                        while j < bytes.len() {
                            let ch = bytes[j] as char;
                            if ch.is_ascii_digit() { j += 1; }
                            else if ch == '.' && !dot { dot = true; j += 1; }
                            else { break; }
                        }
                        if let Ok(val) = src[i..j].trim().parse::<f64>() { toks.push(ATok::Val(ArithExpr::Term(ArithTerm::Number(val)))); i = j; continue; }
                    }}
                }
                toks.push(ATok::Op(ArithOp::Sub)); i += 1;
            },
            '*' => { toks.push(ATok::Op(ArithOp::Mul)); i += 1; },
            '/' => { toks.push(ATok::Op(ArithOp::Div)); i += 1; },
            '\'' => {
                // single-quoted literal (string or datetime)
                let mut j = i + 1; let mut s = String::new();
                while j < bytes.len() {
                    let ch = bytes[j] as char; j += 1; if ch == '\'' { break; } s.push(ch);
                }
                // Determine base value
                let mut base_val = if let Some(ms) = parse_iso8601_to_ms(&format!("'{}'", s)) { ArithExpr::Term(ArithTerm::Number(ms as f64)) } else { ArithExpr::Term(ArithTerm::Str(s)) };
                i = j;
                // Optional PostgreSQL ::type cast (possibly chained)
                loop {
                    // skip ws
                    let mut k = i; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                    if k + 1 < bytes.len() && bytes[k] as char == ':' && bytes[k+1] as char == ':' {
                        // parse type name after '::'
                        k += 2; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                        if let Some((ty, consumed)) = parse_type_name(&src[k..]) {
                            base_val = ArithExpr::Cast { expr: Box::new(base_val), ty };
                            i = k + consumed; // advance after typename
                            continue;
                        }
                    }
                    break;
                }
                // Optional slice suffix like [start:stop:step]
                let mut ii = i; while ii < bytes.len() && bytes[ii].is_ascii_whitespace() { ii += 1; }
                if ii < bytes.len() && (bytes[ii] as char) == '[' {
                    // parse until closing ']'
                    let mut k = ii + 1; let mut inside = String::new();
                    let mut closed = false;
                    while k < bytes.len() {
                        let ch = bytes[k] as char; k += 1;
                        if ch == ']' { closed = true; break; }
                        inside.push(ch);
                    }
                    if !closed { anyhow::bail!("Unclosed slice bracket"); }
                    // parse parts by ':' allowing 1 or 2 colons
                    let parts: Vec<String> = inside.split(':').map(|p| p.trim().to_string()).collect();
                    if parts.len() < 2 || parts.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                    // helper to parse bound
                    fn parse_bound(txt: &str) -> Option<StrSliceBound> {
                        if txt.is_empty() { return None; }
                        if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                        let t = txt.trim();
                        let (include, lit_txt) = if t.starts_with("-'") && t.ends_with("'") { (false, &t[1..]) } else { (true, t) };
                        if lit_txt.starts_with('\'') && lit_txt.ends_with('\'') {
                            let inner = &lit_txt[1..lit_txt.len()-1];
                            return Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Str(inner.to_string()))), include });
                        }
                        Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Col { name: t.to_string(), previous: false })), include: true })
                    }
                    let start_b = parse_bound(&parts[0]);
                    let stop_b = parse_bound(&parts[1]);
                    let step_v = if parts.len() == 3 {
                        let p = parts[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) }
                    } else { None };
                    toks.push(ATok::Val(ArithExpr::Slice { base: Box::new(base_val), start: start_b, stop: stop_b, step: step_v }));
                    i = k; // after ']'
                } else {
                    toks.push(ATok::Val(base_val));
                }
            },
            '"' => {
                // double-quoted identifier
                let mut j = i + 1; let mut s = String::new();
                while j < bytes.len() { let ch = bytes[j] as char; j += 1; if ch == '"' { break; } s.push(ch); }
                let mut base = ArithExpr::Term(ArithTerm::Col { name: s, previous: false });
                // Optional PostgreSQL ::type cast (possibly chained)
                loop {
                    let mut k = j; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                    if k + 1 < bytes.len() && bytes[k] as char == ':' && bytes[k+1] as char == ':' {
                        k += 2; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                        if let Some((ty, consumed)) = parse_type_name(&src[k..]) {
                            base = ArithExpr::Cast { expr: Box::new(base), ty };
                            j = k + consumed; // advance
                            continue;
                        }
                    }
                    break;
                }
                // Optional slice suffix
                let mut ii = j; while ii < bytes.len() && bytes[ii].is_ascii_whitespace() { ii += 1; }
                if ii < bytes.len() && (bytes[ii] as char) == '[' {
                    let mut k = ii + 1; let mut inside = String::new(); let mut closed = false;
                    while k < bytes.len() { let ch = bytes[k] as char; k += 1; if ch == ']' { closed = true; break; } inside.push(ch); }
                    if !closed { anyhow::bail!("Unclosed slice bracket"); }
                    let parts: Vec<String> = inside.split(':').map(|p| p.trim().to_string()).collect();
                    if parts.len() < 2 || parts.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                    fn parse_bound(txt: &str) -> Option<StrSliceBound> {
                        if txt.is_empty() { return None; }
                        if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                        None
                    }
                    let start_b = parse_bound(&parts[0]);
                    let stop_b = parse_bound(&parts[1]);
                    let step_v = if parts.len() == 3 { let p = parts[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) } } else { None };
                    base = ArithExpr::Slice { base: Box::new(base), start: start_b, stop: stop_b, step: step_v };
                    j = k;
                }
                toks.push(ATok::Val(base));
                i = j;
            },
            _ => {
                // number, identifier (possibly with dots), or ISO date literal without quotes
                if c.is_ascii_digit() || (c == '.' && i+1 < bytes.len() && (bytes[i+1] as char).is_ascii_digit()) {
                    // First, attempt to read a datetime-like token (allowing - : T Z . +)
                    let mut j = i;
                    while j < bytes.len() {
                        let ch = bytes[j] as char;
                        if ch.is_ascii_alphanumeric() || ch == '-' || ch == ':' || ch == 'T' || ch == 'Z' || ch == '.' || ch == '+' { j += 1; }
                        else { break; }
                    }
                    let candidate = &src[i..j];
                    if let Some(ms) = parse_iso8601_to_ms(candidate) {
                        toks.push(ATok::Val(ArithExpr::Term(ArithTerm::Number(ms as f64))));
                        i = j; continue;
                    }
                    // Fallback to simple number parsing
                    let mut j2 = i; let mut dot = false;
                    while j2 < bytes.len() {
                        let ch = bytes[j2] as char;
                        if ch.is_ascii_digit() { j2 += 1; }
                        else if ch == '.' && !dot { dot = true; j2 += 1; }
                        else { break; }
                    }
                    let token = &src[i..j2];
                    // Start with number literal or identifier
                    let mut base = if let Ok(v) = token.parse::<f64>() { ArithExpr::Term(ArithTerm::Number(v)) } else { ArithExpr::Term(ArithTerm::Col { name: token.to_string(), previous: false }) };
                    let mut j_after = j2;
                    // Optional PostgreSQL ::type cast (possibly chained)
                    loop {
                        let mut k = j_after; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                        if k + 1 < bytes.len() && bytes[k] as char == ':' && bytes[k+1] as char == ':' {
                            k += 2; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                            if let Some((ty, consumed)) = parse_type_name(&src[k..]) {
                                base = ArithExpr::Cast { expr: Box::new(base), ty };
                                j_after = k + consumed; continue;
                            }
                        }
                        break;
                    }
                    toks.push(ATok::Val(base));
                    i = j_after;
                } else if c.is_ascii_alphabetic() || c == '_' {
                    // identifier or function
                    let mut j = i;
                    while j < bytes.len() {
                        let ch = bytes[j] as char;
                        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' { j += 1; } else { break; }
                    }
                    // Handle identifiers and f-strings
                    let name = &src[i..j];
                    // Support alias."identifier.with.dots" and alias.'identifier.with.dots'
                    // If the parsed name ends with a dot, and the next char is a quote, consume the quoted part and combine
                    if name.ends_with('.') {
                        let mut end = j;
                        // Skip whitespace
                        while end < bytes.len() && bytes[end].is_ascii_whitespace() { end += 1; }
                        if end < bytes.len() {
                            let qch = bytes[end] as char;
                            if qch == '"' || qch == '\'' {
                                let quote = qch;
                                let mut k = end + 1;
                                let mut inner = String::new();
                                while k < bytes.len() {
                                    let ch2 = bytes[k] as char; k += 1;
                                    if ch2 == quote {
                                        // handle escaped '' for single quotes inside literals
                                        if quote == '\'' && k < bytes.len() && (bytes[k] as char) == '\'' { inner.push('\''); k += 1; continue; }
                                        break;
                                    }
                                    inner.push(ch2);
                                }
                                // Combine alias. + inner
                                let mut combined = String::from(name);
                                combined.push_str(&inner);
                                // Update name slice by storing combined separately and advancing j
                                // To keep logic simple, push directly as a column token here and advance i/j beyond consumed
                                let mut base = ArithExpr::Term(ArithTerm::Col { name: combined, previous: false });
                                // Optional PostgreSQL ::type cast after identifier
                                let mut j_after = k; // position after closing quote
                                loop {
                                    let mut k2 = j_after; while k2 < bytes.len() && bytes[k2].is_ascii_whitespace() { k2 += 1; }
                                    if k2 + 1 < bytes.len() && bytes[k2] as char == ':' && bytes[k2+1] as char == ':' {
                                        k2 += 2; while k2 < bytes.len() && bytes[k2].is_ascii_whitespace() { k2 += 1; }
                                        if let Some((ty, consumed)) = parse_type_name(&src[k2..]) {
                                            base = ArithExpr::Cast { expr: Box::new(base), ty };
                                            j_after = k2 + consumed; continue;
                                        }
                                    }
                                    break;
                                }
                                toks.push(ATok::Val(base));
                                i = j_after;
                                continue;
                            }
                        }
                    }
                    // NULL literal (case-insensitive)
                    if name.eq_ignore_ascii_case("NULL") {
                        toks.push(ATok::Val(ArithExpr::Term(ArithTerm::Null)));
                        i = j;
                        continue;
                    }
                    // Optional PostgreSQL ::type cast for bare keywords/identifiers before treating as function/column
                    // For simplicity, we only apply to identifiers that are not immediately followed by '(' (function call handled later)
                    let mut base_opt: Option<ArithExpr> = None;
                    // Peek next non-ws
                    let mut kpeek = j; while kpeek < bytes.len() && bytes[kpeek].is_ascii_whitespace() { kpeek += 1; }
                    if kpeek < bytes.len() && (bytes[kpeek] as char) != '(' {
                        // allow casts like mycol::int
                        let mut after = j;
                        loop {
                            let mut k = after; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                            if k + 1 < bytes.len() && bytes[k] as char == ':' && bytes[k+1] as char == ':' {
                                k += 2; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                                if let Some((ty, consumed)) = parse_type_name(&src[k..]) {
                                    let base = base_opt.take().unwrap_or_else(|| ArithExpr::Term(ArithTerm::Col { name: name.to_string(), previous: false }));
                                    let casted = ArithExpr::Cast { expr: Box::new(base), ty };
                                    base_opt = Some(casted);
                                    after = k + consumed; continue;
                                }
                            }
                            break;
                        }
                        if let Some(b) = base_opt.take() {
                            toks.push(ATok::Val(b));
                            i = after;
                            continue;
                        }
                    }
                    // CASE expression: CASE WHEN cond THEN val [WHEN ...] [ELSE val] END
                    if name.eq_ignore_ascii_case("CASE") {
                        // Find matching END keyword
                        let case_start = i;
                        let mut depth = 1; // Track nested CASE expressions
                        let mut end_pos = j;
                        let src_up = src.to_uppercase();
                        while end_pos < src.len() && depth > 0 {
                            // Look for CASE or END keywords
                            if let Some(case_pos) = src_up[end_pos..].find("CASE") {
                                let abs_pos = end_pos + case_pos;
                                // Check word boundary
                                let prev_ok = abs_pos == 0 || !src.as_bytes()[abs_pos-1].is_ascii_alphanumeric();
                                let next_ok = abs_pos + 4 >= src.len() || !src.as_bytes()[abs_pos+4].is_ascii_alphanumeric();
                                if prev_ok && next_ok {
                                    if let Some(end_pos_found) = src_up[end_pos..].find("END") {
                                        let abs_end = end_pos + end_pos_found;
                                        let prev_ok_end = abs_end == 0 || !src.as_bytes()[abs_end-1].is_ascii_alphanumeric();
                                        let next_ok_end = abs_end + 3 >= src.len() || !src.as_bytes()[abs_end+3].is_ascii_alphanumeric();
                                        if prev_ok_end && next_ok_end
                                            && abs_pos < abs_end {
                                                depth += 1;
                                                end_pos = abs_pos + 4;
                                                continue;
                                            }
                                    }
                                }
                            }
                            if let Some(end_offset) = src_up[end_pos..].find("END") {
                                let abs_end = end_pos + end_offset;
                                let prev_ok = abs_end == 0 || !src.as_bytes()[abs_end-1].is_ascii_alphanumeric();
                                let next_ok = abs_end + 3 >= src.len() || !src.as_bytes()[abs_end+3].is_ascii_alphanumeric();
                                if prev_ok && next_ok {
                                    depth -= 1;
                                    if depth == 0 {
                                        end_pos = abs_end + 3;
                                        break;
                                    }
                                    end_pos = abs_end + 3;
                                } else {
                                    end_pos += 1;
                                }
                            } else {
                                anyhow::bail!("CASE without matching END");
                            }
                        }
                        if depth != 0 {
                            anyhow::bail!("CASE without matching END");
                        }
                        // Parse CASE expression content
                        let case_body = &src[j..end_pos-3].trim();
                        let mut when_clauses: Vec<(WhereExpr, ArithExpr)> = Vec::new();
                        let mut else_expr: Option<Box<ArithExpr>> = None;
                        
                        // Split by WHEN keywords
                        let body_up = case_body.to_uppercase();
                        let mut pos = 0;
                        while pos < case_body.len() {
                            // Skip whitespace
                            while pos < case_body.len() && case_body.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
                            if pos >= case_body.len() { break; }
                            
                            // Check for WHEN or ELSE
                            if body_up[pos..].starts_with("WHEN ") {
                                pos += 5; // Skip "WHEN "
                                // Find THEN keyword
                                if let Some(then_offset) = body_up[pos..].find(" THEN ") {
                                    let when_cond = &case_body[pos..pos+then_offset].trim();
                                    pos += then_offset + 6; // Skip " THEN "
                                    
                                    // Find next WHEN, ELSE, or end
                                    let mut next_pos = case_body.len();
                                    if let Some(when_pos) = body_up[pos..].find(" WHEN ") {
                                        next_pos = next_pos.min(pos + when_pos);
                                    }
                                    if let Some(else_pos) = body_up[pos..].find(" ELSE ") {
                                        next_pos = next_pos.min(pos + else_pos);
                                    }
                                    
                                    let then_val = &case_body[pos..next_pos].trim();
                                    let cond = parse_where_expr(when_cond)?;
                                    let val = super_parse_arith(then_val).ok_or_else(|| anyhow::anyhow!("Invalid THEN expression"))?;
                                    when_clauses.push((cond, val));
                                    pos = next_pos;
                                } else {
                                    anyhow::bail!("WHEN without THEN in CASE expression");
                                }
                            } else if body_up[pos..].starts_with("ELSE ") {
                                pos += 5; // Skip "ELSE "
                                let else_val = &case_body[pos..].trim();
                                else_expr = Some(Box::new(super_parse_arith(else_val).ok_or_else(|| anyhow::anyhow!("Invalid ELSE expression"))?));
                                break;
                            } else {
                                anyhow::bail!("Expected WHEN or ELSE in CASE expression");
                            }
                        }
                        
                        if when_clauses.is_empty() {
                            anyhow::bail!("CASE expression must have at least one WHEN clause");
                        }
                        
                        toks.push(ATok::Val(ArithExpr::Case { when_clauses, else_expr }));
                        i = end_pos;
                        continue;
                    }
                    // f-string detection: immediately followed by a single quote
                    if name == "f" && j < bytes.len() && (bytes[j] as char) == '\'' {
                        // parse f-string contents
                        let mut p = j + 1; // position after opening quote
                        let mut parts: Vec<ArithExpr> = Vec::new();
                        let mut lit = String::new();
                        while p < bytes.len() {
                            let ch = bytes[p] as char;
                            p += 1;
                            if ch == '\'' {
                                // end of string
                                if !lit.is_empty() { parts.push(ArithExpr::Term(ArithTerm::Str(lit.clone()))); lit.clear(); }
                                break;
                            } else if ch == '{' {
                                // handle escaped '{{'
                                if p < bytes.len() && (bytes[p] as char) == '{' {
                                    lit.push('{'); p += 1; continue;
                                }
                                // flush current literal
                                if !lit.is_empty() { parts.push(ArithExpr::Term(ArithTerm::Str(lit.clone()))); lit.clear(); }
                                // capture until matching '}' (no nesting)
                                let mut inner = String::new();
                                let mut closed = false;
                                while p < bytes.len() {
                                    let ch2 = bytes[p] as char; p += 1;
                                    if ch2 == '}' {
                                        closed = true; break;
                                    } else if ch2 == '\'' {
                                        // allow quotes inside expression by just including them; parsing will handle
                                        inner.push(ch2);
                                    } else if ch2 == '"' {
                                        inner.push(ch2);
                                    } else if ch2 == '{' {
                                        // simple protection against nesting: treat as plain char
                                        inner.push(ch2);
                                    } else {
                                        inner.push(ch2);
                                    }
                                }
                                if !closed { anyhow::bail!("Unclosed {{ in f-string"); }
                                if let Some(expr) = super_parse_arith(inner.trim()) {
                                    parts.push(expr);
                                } else {
                                    anyhow::bail!("Invalid expression inside f-string: {}", inner);
                                }
                            } else if ch == '}' {
                                // escaped '}}'
                                if p < bytes.len() && (bytes[p] as char) == '}' {
                                    lit.push('}'); p += 1; continue;
                                } else {
                                    anyhow::bail!("Single '}}' in f-string");
                                }
                            } else {
                                lit.push(ch);
                            }
                        }
                        if parts.is_empty() { parts.push(ArithExpr::Term(ArithTerm::Str(String::new()))); }
                        let base = if parts.len() == 1 { parts.remove(0) } else { ArithExpr::Concat(parts) };
                        // Optional slice suffix
                        let mut ii = p; while ii < bytes.len() && bytes[ii].is_ascii_whitespace() { ii += 1; }
                        if ii < bytes.len() && (bytes[ii] as char) == '[' {
                            let mut k2 = ii + 1; let mut inside2 = String::new(); let mut closed2 = false;
                            while k2 < bytes.len() { let ch2 = bytes[k2] as char; k2 += 1; if ch2 == ']' { closed2 = true; break; } inside2.push(ch2); }
                            if !closed2 { anyhow::bail!("Unclosed slice bracket"); }
                            let parts2: Vec<String> = inside2.split(':').map(|p| p.trim().to_string()).collect();
                            if parts2.len() < 2 || parts2.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                            fn parse_bound2(txt: &str) -> Option<StrSliceBound> {
                                if txt.is_empty() { return None; }
                                if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                                let t = txt.trim();
                                let (include, lit_txt) = if t.starts_with("-'") && t.ends_with("'") { (false, &t[1..]) } else { (true, t) };
                                if lit_txt.starts_with('\'') && lit_txt.ends_with('\'') {
                                    let inner = &lit_txt[1..lit_txt.len()-1];
                                    return Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Str(inner.to_string()))), include });
                                }
                                Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Col { name: t.to_string(), previous: false })), include: true })
                            }
                            let start_b = parse_bound2(&parts2[0]);
                            let stop_b = parse_bound2(&parts2[1]);
                            let step_v = if parts2.len() == 3 { let p = parts2[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) } } else { None };
                            let slice_expr = ArithExpr::Slice { base: Box::new(base), start: start_b, stop: stop_b, step: step_v };
                            toks.push(ATok::Val(slice_expr));
                            i = k2; // after ']'
                        } else {
                            toks.push(ATok::Val(base));
                            i = p;
                        }
                        continue;
                    }
                    // If next non-ws is '(', try parse function
                    let mut k = j; while k < bytes.len() && bytes[k].is_ascii_whitespace() { k += 1; }
                    if k < bytes.len() && bytes[k] as char == '(' {
                        if let Some((func_expr, end)) = parse_func(&src, i) {
                            // Optional slice suffix after function call
                            let j2 = i + end; // absolute index end
                            let mut ii2 = j2; while ii2 < bytes.len() && bytes[ii2].is_ascii_whitespace() { ii2 += 1; }
                            if ii2 < bytes.len() && (bytes[ii2] as char) == '[' {
                                let mut k2 = ii2 + 1; let mut inside2 = String::new(); let mut closed2 = false;
                                while k2 < bytes.len() { let ch2 = bytes[k2] as char; k2 += 1; if ch2 == ']' { closed2 = true; break; } inside2.push(ch2); }
                                if !closed2 { anyhow::bail!("Unclosed slice bracket"); }
                                let parts2: Vec<String> = inside2.split(':').map(|p| p.trim().to_string()).collect();
                                if parts2.len() < 2 || parts2.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                                fn parse_bound2(txt: &str) -> Option<StrSliceBound> {
                                    if txt.is_empty() { return None; }
                                    if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                                    let t = txt.trim();
                                    let (include, lit_txt) = if t.starts_with("-'") && t.ends_with("'") { (false, &t[1..]) } else { (true, t) };
                                    if lit_txt.starts_with('\'') && lit_txt.ends_with('\'') {
                                        let inner = &lit_txt[1..lit_txt.len()-1];
                                        return Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Str(inner.to_string()))), include });
                                    }
                                    Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Col { name: t.to_string(), previous: false })), include: true })
                                }
                                let start_b = parse_bound2(&parts2[0]);
                                let stop_b = parse_bound2(&parts2[1]);
                                let step_v = if parts2.len() == 3 { let p = parts2[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) } } else { None };
                                let slice_expr = ArithExpr::Slice { base: Box::new(func_expr), start: start_b, stop: stop_b, step: step_v };
                                toks.push(ATok::Val(slice_expr));
                                i = k2; // after ']'
                            } else {
                                toks.push(ATok::Val(func_expr));
                                i = j2;
                            }
                            continue;
                        } else {
                            // Parse generic function call: name(arg1, arg2, ...)
                            // Find matching ')'
                            let mut depth: i32 = 0; let mut j2 = k; // at '('
                            while j2 < bytes.len() {
                                let ch = src[j2..].chars().next().unwrap();
                                if ch == '(' { depth += 1; }
                                else if ch == ')' { depth -= 1; if depth == 0 { j2 += ch.len_utf8(); break; } }
                                j2 += ch.len_utf8();
                            }
                            // inside arguments between ( and )
                            let inside = &src[k+1..j2-1];
                            // split on commas at top-level depth, respecting parentheses and single-quoted strings
                            let mut args: Vec<String> = Vec::new();
                            let mut buf = String::new();
                            let mut d = 0i32;
                            let mut in_str = false;
                            let mut p = 0usize;
                            let chars: Vec<char> = inside.chars().collect();
                            let n = chars.len();
                            while p < n {
                                let ch = chars[p];
                                if in_str {
                                    if ch == '\'' {
                                        // handle escaped '' inside string
                                        if p + 1 < n && chars[p + 1] == '\'' { buf.push('\''); p += 2; continue; }
                                        in_str = false; buf.push(ch); p += 1; continue;
                                    }
                                    buf.push(ch); p += 1; continue;
                                }
                                match ch {
                                    '\'' => { in_str = true; buf.push(ch); }
                                    '(' => { d += 1; buf.push(ch); }
                                    ')' => { d -= 1; buf.push(ch); }
                                    ',' if d == 0 => { args.push(buf.trim().to_string()); buf.clear(); }
                                    _ => buf.push(ch),
                                }
                                p += 1;
                            }
                            if !buf.trim().is_empty() { args.push(buf.trim().to_string()); }
                            // Parse-time UDF arity enforcement for known scalar functions.
                            // This avoids misinterpreting extra arguments (e.g., 'true') as columns later.
                            fn expected_udf_arity(name: &str) -> Option<(usize, usize)> {
                                let n = name.to_ascii_lowercase();
                                match n.as_str() {
                                    // Standard helpers
                                    "nullif" => Some((2, 2)),
                                    "format_type" | "pg_catalog.format_type" => Some((2, 2)),
                                    // PostgreSQL compatibility UDFs shipped with clarium
                                    "pg_catalog.pg_get_expr" | "pg_get_expr" => Some((2, 3)), // third arg optional (pretty)
                                    "pg_catalog.pg_total_relation_size" | "pg_total_relation_size" => Some((1, 1)),
                                    "pg_catalog.pg_get_partkeydef" | "pg_get_partkeydef" => Some((1, 1)),
                                    _ => None,
                                }
                            }
                            if let Some((_min, max)) = expected_udf_arity(name) {
                                // filter out empty arg slots first for count comparison
                                let provided_count = args.iter().filter(|s| !s.is_empty()).count();
                                if provided_count > max {
                                    // Identify the first extra argument (1-based index)
                                    let extra_index = max + 1;
                                    // Compute textual representation of that argument if present
                                    // Note: args may contain empties from stray commas; skip empties when indexing
                                    let non_empty: Vec<String> = args.iter().filter(|s| !s.is_empty()).cloned().collect();
                                    let passed_text: String = if extra_index >= 1 && extra_index <= non_empty.len() { non_empty[extra_index - 1].clone() } else { "".to_string() };
                                    anyhow::bail!(
                                        "Missing Argument: function '{}' does not define argument {}; received {}",
                                        name,
                                        extra_index,
                                        passed_text
                                    );
                                }
                            }
                            // If the name matches a known aggregate or special function used as a column label, keep it as an identifier token (e.g., COUNT(v))
                            let name_up = name.to_uppercase();
                            let is_agg_label = matches!(name_up.as_str(),
                                "COUNT" | "AVG" | "SUM" | "MIN" | "MAX" | "FIRST" | "LAST" | "STDEV" | "DELTA" | "HEIGHT" | "GRADIENT" | "QUANTILE" | "ARRAY_AGG");
                            if is_agg_label {
                                let full = &src[i..j2];
                                toks.push(ATok::Val(ArithExpr::Term(ArithTerm::Col { name: full.to_string(), previous: false })));
                                i = j2; continue;
                            }
                            let parsed_args: Vec<ArithExpr> = args
                                .into_iter()
                                .filter(|s| !s.is_empty())
                                .map(|s| {
                                    let sl = s.to_ascii_lowercase();
                                    // Treat unquoted true/false as boolean literals in arithmetic contexts
                                    if sl == "true" {
                                        ArithExpr::Term(ArithTerm::Number(1.0))
                                    } else if sl == "false" {
                                        ArithExpr::Term(ArithTerm::Number(0.0))
                                    } else {
                                        super_parse_arith(&s).unwrap()
                                    }
                                })
                                .collect();
                            let mut call = ArithExpr::Call { name: name.to_string(), args: parsed_args };
                            // Optional PostgreSQL ::type cast (possibly chained) after function call: func(...)::type
                            loop {
                                let mut kcast = j2; while kcast < bytes.len() && bytes[kcast].is_ascii_whitespace() { kcast += 1; }
                                if kcast + 1 < bytes.len() && bytes[kcast] as char == ':' && bytes[kcast+1] as char == ':' {
                                    kcast += 2; while kcast < bytes.len() && bytes[kcast].is_ascii_whitespace() { kcast += 1; }
                                    if let Some((ty, consumed)) = parse_type_name(&src[kcast..]) {
                                        call = ArithExpr::Cast { expr: Box::new(call), ty };
                                        j2 = kcast + consumed;
                                        continue;
                                    }
                                }
                                break;
                            }
                            // Optional slice suffix after call
                            let mut ii2 = j2; while ii2 < bytes.len() && bytes[ii2].is_ascii_whitespace() { ii2 += 1; }
                            if ii2 < bytes.len() && (bytes[ii2] as char) == '[' {
                                let mut k3 = ii2 + 1; let mut inside2 = String::new(); let mut closed2 = false;
                                while k3 < bytes.len() { let ch2 = bytes[k3] as char; k3 += 1; if ch2 == ']' { closed2 = true; break; } inside2.push(ch2); }
                                if !closed2 { anyhow::bail!("Unclosed slice bracket"); }
                                let parts2: Vec<String> = inside2.split(':').map(|p| p.trim().to_string()).collect();
                                if parts2.len() < 2 || parts2.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                                fn parse_bound2(txt: &str) -> Option<StrSliceBound> {
                                    if txt.is_empty() { return None; }
                                    if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                                    let t = txt.trim();
                                    let (include, lit_txt) = if t.starts_with("-'") && t.ends_with("'") { (false, &t[1..]) } else { (true, t) };
                                    if lit_txt.starts_with('\'') && lit_txt.ends_with('\'') {
                                        let inner = &lit_txt[1..lit_txt.len()-1];
                                        return Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Str(inner.to_string()))), include });
                                    }
                                    Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Col { name: t.to_string(), previous: false })), include: true })
                                }
                                let start_b = parse_bound2(&parts2[0]);
                                let stop_b = parse_bound2(&parts2[1]);
                                let step_v = if parts2.len() == 3 { let p = parts2[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) } } else { None };
                                let slice_expr = ArithExpr::Slice { base: Box::new(call), start: start_b, stop: stop_b, step: step_v };
                                toks.push(ATok::Val(slice_expr));
                                i = k3; // after ']'
                            } else {
                                toks.push(ATok::Val(call));
                                i = j2; // after ')'
                            }
                            continue;
                        }
                    }
                    // regular identifier
                    let up = name.to_uppercase();
                    if up.starts_with("PREVIOUS.") {
                        let nm = name[9..].to_string();
                        let mut base = ArithExpr::Term(ArithTerm::Col { name: nm, previous: true });
                        // Optional slice suffix
                        let mut ii = j; while ii < bytes.len() && bytes[ii].is_ascii_whitespace() { ii += 1; }
                        if ii < bytes.len() && (bytes[ii] as char) == '[' {
                            let mut k = ii + 1; let mut inside = String::new(); let mut closed = false;
                            while k < bytes.len() { let ch = bytes[k] as char; k += 1; if ch == ']' { closed = true; break; } inside.push(ch); }
                            if !closed { anyhow::bail!("Unclosed slice bracket"); }
                            let parts: Vec<String> = inside.split(':').map(|p| p.trim().to_string()).collect();
                            if parts.len() < 2 || parts.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                            fn parse_bound(txt: &str) -> Option<StrSliceBound> {
                                if txt.is_empty() { return None; }
                                if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                                let t = txt.trim();
                                let (include, lit_txt) = if t.starts_with("-'") && t.ends_with("'") { (false, &t[1..]) } else { (true, t) };
                                if lit_txt.starts_with('\'') && lit_txt.ends_with('\'') {
                                    let inner = &lit_txt[1..lit_txt.len()-1];
                                    return Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Str(inner.to_string()))), include });
                                }
                                Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Col { name: t.to_string(), previous: false })), include: true })
                            }
                            let start_b = parse_bound(&parts[0]);
                            let stop_b = parse_bound(&parts[1]);
                            let step_v = if parts.len() == 3 { let p = parts[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) } } else { None };
                            base = ArithExpr::Slice { base: Box::new(base), start: start_b, stop: stop_b, step: step_v };
                            j = k;
                        }
                        toks.push(ATok::Val(base));
                    } else {
                        let mut base = ArithExpr::Term(ArithTerm::Col { name: name.to_string(), previous: false });
                        // Optional slice suffix
                        let mut ii = j; while ii < bytes.len() && bytes[ii].is_ascii_whitespace() { ii += 1; }
                        if ii < bytes.len() && (bytes[ii] as char) == '[' {
                            let mut k = ii + 1; let mut inside = String::new(); let mut closed = false;
                            while k < bytes.len() { let ch = bytes[k] as char; k += 1; if ch == ']' { closed = true; break; } inside.push(ch); }
                            if !closed { anyhow::bail!("Unclosed slice bracket"); }
                            let parts: Vec<String> = inside.split(':').map(|p| p.trim().to_string()).collect();
                            if parts.len() < 2 || parts.len() > 3 { anyhow::bail!("Slice must have one or two ':' separators"); }
                            fn parse_bound(txt: &str) -> Option<StrSliceBound> {
                                if txt.is_empty() { return None; }
                                if let Ok(v) = txt.parse::<i64>() { return Some(StrSliceBound::Index(v)); }
                                let t = txt.trim();
                                let (include, lit_txt) = if t.starts_with("-'") && t.ends_with("'") { (false, &t[1..]) } else { (true, t) };
                                if lit_txt.starts_with('\'') && lit_txt.ends_with('\'') {
                                    let inner = &lit_txt[1..lit_txt.len()-1];
                                    return Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Str(inner.to_string()))), include });
                                }
                                Some(StrSliceBound::Pattern { expr: Box::new(ArithExpr::Term(ArithTerm::Col { name: t.to_string(), previous: false })), include: true })
                            }
                            let start_b = parse_bound(&parts[0]);
                            let stop_b = parse_bound(&parts[1]);
                            let step_v = if parts.len() == 3 { let p = parts[2].trim(); if p.is_empty() { None } else { Some(p.parse::<i64>().map_err(|_| anyhow::anyhow!("Invalid step"))?) } } else { None };
                            base = ArithExpr::Slice { base: Box::new(base), start: start_b, stop: stop_b, step: step_v };
                            j = k;
                        }
                        toks.push(ATok::Val(base));
                    }
                    i = j;
                } else {
                    // Unrecognized char, skip
                    i += 1;
                }
            }
        }
    }

    // Shunting-yard including parentheses
    let mut out: Vec<ATok> = Vec::new();
    let mut opstack: Vec<ATok> = Vec::new();

    for t in toks.into_iter() {
        match t.clone() {
            ATok::Val(_) => out.push(t),
            ATok::Op(op) => {
                while let Some(top) = opstack.last() {
                    match top {
                        ATok::Op(top_op) => { if prec(top_op) >= prec(&op) { out.push(opstack.pop().unwrap()); } else { break; } }
                        ATok::LParen => break,
                        _ => break,
                    }
                }
                opstack.push(ATok::Op(op));
            }
            ATok::LParen => opstack.push(ATok::LParen),
            ATok::RParen => {
                while let Some(top) = opstack.pop() {
                    match top {
                        ATok::LParen => break,
                        _ => out.push(top),
                    }
                }
            }
        }
    }
    while let Some(top) = opstack.pop() { out.push(top); }

    // Build AST from RPN in 'out'
    let mut stack: Vec<ArithExpr> = Vec::new();
    for t in out.into_iter() {
        match t {
            ATok::Val(v) => { stack.push(v); }
            ATok::Op(op) => {
                let r = Box::new(stack.pop().ok_or_else(|| anyhow::anyhow!("Syntax error: missing right-hand operand for operator '{:?}' in expression: {}", op, src))?);
                let l = Box::new(stack.pop().ok_or_else(|| anyhow::anyhow!("Syntax error: missing left-hand operand for operator '{:?}' in expression: {}", op, src))?);
                stack.push(ArithExpr::BinOp { left: l, op, right: r });
            }
            _ => {}
        }
    }
    stack.pop().ok_or_else(|| anyhow::anyhow!("Syntax error: empty or invalid expression: {}", src))
}
