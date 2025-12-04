use crate::server::query::query_common::*;
use crate::server::query::query_parse_arith_expr::parse_arith_expr;
use crate::server::query::*;

// WHERE parsing (simple, whitespace-delimited tokens)
pub fn parse_where_expr(s: &str) -> Result<WhereExpr> {
    // New precedence-climbing boolean expression parser with proper tokenization and
    // detailed error messages including approximate position and snippet.

    // Compatibility fast-path: if the WHERE text contains PostgreSQL-style casts (::),
    // our boolean lexer (which does not recognize ':') will reject it. In that case,
    // delegate to the legacy whitespace-token parser that reuses the arithmetic parser
    // (which fully supports expr::type and (expr)::type chaining).
    if s.contains("::") {
        let tokens: Vec<String> = s.split_whitespace().map(|x| x.to_string()).collect();
        return parse_where_tokens(&tokens, s);
    }

    // Local helper: parse an arithmetic expression from a raw snippet by tokenizing on whitespace.
    // This mirrors the super_parse_arith used in the arithmetic parser area but is scoped here.
    fn local_super_parse_arith(s: &str) -> Option<ArithExpr> {
        let t: Vec<String> = s.split_whitespace().map(|x| x.to_string()).collect();
        parse_arith_expr(&t).ok()
    }

    #[derive(Clone, Debug, PartialEq)]
    enum TKind {
        Ident(String), Str(String), Num(String), LParen, RParen, Comma,
        Eq, Ne, Lt, Gt, Le, Ge,
        And, Or, Not, Is, Null,
        Like, Between, In, Exists, Any, All,
        True, False,
    }
    #[derive(Clone, Debug)]
    struct Tok { kind: TKind, pos: usize }

    fn is_ident_start(c: char) -> bool { c.is_ascii_alphabetic() || c == '_' || c == '"' }
    // Allow broader identifier parts to support our table naming scheme where
    // fully-qualified names may include path-like separators (e.g., clarium/public/orders).
    // We also allow backslash because some contexts may provide it on Windows paths.
    fn is_ident_part(c: char) -> bool {
        c.is_ascii_alphanumeric()
            || c == '_'
            || c == '.'
            || c == '"'
            || c == '*'
            || c == '/'  // support schema/table separator used by clarium
            || c == '\\' // allow backslash in identifiers to avoid lexer aborts
    }

    fn caret_snippet(src: &str, pos: usize) -> String {
        // produce a one-line snippet with caret under the column (approximate for ASCII)
        let prefix = &src[..pos.min(src.len())];
        let line_start = prefix.rfind('\n').map(|i| i+1).unwrap_or(0);
        let line_end = src[pos..].find('\n').map(|i| pos + i).unwrap_or(src.len());
        let line = &src[line_start..line_end];
        let col = prefix[line_start..].chars().count();
        let mut caret = String::new();
        for _ in 0..col { caret.push(' '); }
        caret.push('^');
        format!("{}\n{}", line, caret)
    }

    fn lex(input: &str) -> Result<Vec<Tok>> {
        let bytes = input.as_bytes();
        let mut i = 0usize;
        let mut toks: Vec<Tok> = Vec::new();
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_whitespace() { i += 1; continue; }
            // strings: single-quoted with '' escape
            if c == '\'' {
                let start = i; i += 1; let mut s = String::new();
                while i < bytes.len() {
                    let ch = bytes[i] as char;
                    if ch == '\'' {
                        if i + 1 < bytes.len() && bytes[i+1] as char == '\'' { s.push('\''); i += 2; continue; }
                        i += 1; break;
                    } else { s.push(ch); i += 1; }
                }
                toks.push(Tok{ kind: TKind::Str(s), pos: start });
                continue;
            }
            // numbers (simple: digits with optional dot)
            if c.is_ascii_digit() {
                let start = i; i += 1; while i < bytes.len() { let ch = bytes[i] as char; if ch.is_ascii_digit() || ch == '.' { i += 1; } else { break; } }
                toks.push(Tok{ kind: TKind::Num(input[start..i].to_string()), pos: start });
                continue;
            }
            // identifiers/keywords (allow dotted and quoted identifiers)
            if is_ident_start(c) {
                let start = i; i += 1; while i < bytes.len() { let ch = bytes[i] as char; if is_ident_part(ch) { i += 1; } else { break; } }
                let raw = input[start..i].to_string();
                let up = raw.to_uppercase();
                let kind = match up.as_str() {
                    "AND" => TKind::And,
                    "OR" => TKind::Or,
                    "NOT" => TKind::Not,
                    "IS" => TKind::Is,
                    "NULL" => TKind::Null,
                    "LIKE" => TKind::Like,
                    "BETWEEN" => TKind::Between,
                    "IN" => TKind::In,
                    "EXISTS" => TKind::Exists,
                    "ANY" => TKind::Any,
                    "ALL" => TKind::All,
                    "TRUE" => TKind::True,
                    "FALSE" => TKind::False,
                    _ => TKind::Ident(raw),
                };
                toks.push(Tok{ kind, pos: start });
                continue;
            }
            // operators and punctuation
            match c {
                '(' => { toks.push(Tok{ kind: TKind::LParen, pos: i }); i += 1; }
                ')' => { toks.push(Tok{ kind: TKind::RParen, pos: i }); i += 1; }
                ',' => { toks.push(Tok{ kind: TKind::Comma, pos: i }); i += 1; }
                '*' => { // allow COUNT(*) and similar forms by treating * as an identifier token
                    toks.push(Tok{ kind: TKind::Ident("*".to_string()), pos: i }); i += 1; }
                '<' => {
                    if i+1 < bytes.len() { let n = bytes[i+1] as char; if n == '=' { toks.push(Tok{kind:TKind::Le,pos:i}); i+=2; continue; } if n == '>' { toks.push(Tok{kind:TKind::Ne,pos:i}); i+=2; continue; } }
                    toks.push(Tok{ kind: TKind::Lt, pos: i }); i += 1; }
                '>' => { if i+1 < bytes.len() && bytes[i+1] as char == '=' { toks.push(Tok{kind:TKind::Ge,pos:i}); i+=2; } else { toks.push(Tok{kind:TKind::Gt,pos:i}); i+=1; } }
                '!' => { if i+1 < bytes.len() && bytes[i+1] as char == '=' { toks.push(Tok{kind:TKind::Ne,pos:i}); i+=2; } else { anyhow::bail!("Syntax error at position {}: unexpected '!'.\n{}", i, caret_snippet(input, i)); } }
                '=' => { toks.push(Tok{ kind: TKind::Eq, pos: i }); i += 1; }
                _ => {
                    anyhow::bail!("Syntax error at position {}: unexpected character '{}'.\n{}", i, c, caret_snippet(input, i));
                }
            }
        }
        Ok(toks)
    }

    #[derive(Clone, Debug)]
    struct Cursor { toks: Vec<Tok>, idx: usize }
    impl Cursor {
        fn peek(&self) -> Option<&Tok> { self.toks.get(self.idx) }
        fn next(&mut self) -> Option<Tok> { let t = self.toks.get(self.idx).cloned(); if t.is_some() { self.idx += 1; } t }
        fn expect<F: FnOnce(&Tok) -> bool>(&self, f: F) -> bool { if let Some(t)=self.peek(){ f(t) } else { false } }
        fn peek_kind(&self) -> Option<TKind> { self.peek().map(|t| t.kind.clone()) }
        fn peek_pos(&self) -> Option<usize> { self.peek().map(|t| t.pos) }
        fn peek_n_kind(&self, n: usize) -> Option<TKind> { self.toks.get(self.idx + n).map(|t| t.kind.clone()) }
        fn peek_n_pos(&self, n: usize) -> Option<usize> { self.toks.get(self.idx + n).map(|t| t.pos) }
    }

    // precedence: OR=1, AND=2, comparisons/IS=3
    fn parse_primary(cur: &mut Cursor, src: &str) -> Result<ArithExpr> {
        if let Some(t) = cur.peek() {
            match &t.kind {
                TKind::LParen => { cur.next(); let expr = parse_bool_expr(cur, src, 1)?; // parse inner as boolean, wrap as predicate=1 for arithmetic context
                    if let Some(t2)=cur.peek(){ if t2.kind == TKind::RParen { cur.next(); } else { anyhow::bail!("Syntax error at position {}: expected ')'.\n{}", t2.pos, caret_snippet(src, t2.pos)); } } else { anyhow::bail!("Syntax error: unexpected end, expected ')'."); }
                    // Represent boolean as predicate expression
                    return Ok(ArithExpr::Predicate(Box::new(expr))); }
                TKind::Str(sv) => {
                    // Clone the string before advancing cursor to avoid borrow conflicts
                    let svv = sv.clone();
                    cur.next();
                    // If string literal looks like ISO-8601 datetime, convert to numeric milliseconds
                    if let Some(ms) = parse_iso8601_to_ms(&format!("'{}'", svv)) {
                        return Ok(ArithExpr::Term(ArithTerm::Number(ms as f64)));
                    }
                    return Ok(ArithExpr::Term(ArithTerm::Str(svv)));
                }
                TKind::Num(nv) => {
                    // Clone the numeric literal before advancing to avoid borrow conflicts
                    let p = t.pos;
                    let nvs = nv.clone();
                    cur.next();
                    if let Ok(n) = nvs.parse::<f64>() {
                        return Ok(ArithExpr::Term(ArithTerm::Number(n)));
                    } else {
                        anyhow::bail!("Invalid number '{}' at position {}.\n{}", nvs, p, caret_snippet(src, p));
                    }
                }
                TKind::True => { cur.next(); return Ok(ArithExpr::Term(ArithTerm::Number(1.0))); }
                TKind::False => { cur.next(); return Ok(ArithExpr::Term(ArithTerm::Number(0.0))); }
                TKind::Ident(name_token) => {
                    // consume contiguous identifiers possibly containing dots, keep original text
                    let start_pos = t.pos;
                    // If the next token is '(', parse as a function call, delegating to arithmetic parser for full fidelity
                    if matches!(cur.peek_n_kind(1), Some(TKind::LParen)) {
                        let lpos = cur.peek_n_pos(1).unwrap_or(start_pos + name_token.len());
                        // Scan source to find the matching ')' starting right after lpos
                        let mut depth = 1usize; let mut k = lpos + 1;
                        while k < src.len() && depth > 0 {
                            let ch = src[k..].chars().next().unwrap();
                            if ch == '(' { depth += 1; }
                            else if ch == ')' { depth -= 1; if depth == 0 { break; } }
                            k += ch.len_utf8();
                        }
                        let end_pos = (k + 1).min(src.len());
                        let call_text = &src[start_pos..end_pos];
                        if let Some(expr) = local_super_parse_arith(call_text) {
                            // consume IDENT and the entire parenthesized arg list tokens from the cursor
                            cur.next(); // IDENT
                            cur.next(); // LParen
                            let mut d = 1i32;
                            while d > 0 {
                                if let Some(knd) = cur.peek_kind() {
                                    match knd {
                                        TKind::LParen => { cur.next(); d += 1; }
                                        TKind::RParen => { cur.next(); d -= 1; }
                                        _ => { cur.next(); }
                                    }
                                } else { break; }
                            }
                            return Ok(expr);
                        } else {
                            anyhow::bail!("Failed to parse function call at position {}: '{}'.", start_pos, call_text);
                        }
                    }
                    // Otherwise, collect identifier text (possibly dotted) as a column reference
                    let mut name = String::new();
                    while let Some(tt) = cur.peek() { match &tt.kind { TKind::Ident(s) => { if !name.is_empty() { name.push(' '); } name.push_str(s); cur.next(); }, _ => break } }
                    if name.is_empty() { let start = start_pos; anyhow::bail!("Syntax error at position {}: expected identifier.\n{}", start, caret_snippet(src, start)); }
                    // Use existing Col variant, mark as not 'previous'
                    return Ok(ArithExpr::Term(ArithTerm::Col { name, previous: false }));
                }
                _ => {}
            }
        }
        anyhow::bail!("Syntax error: unexpected end of input while parsing expression.")
    }

    fn parse_comparison(cur: &mut Cursor, src: &str) -> Result<WhereExpr> {
        // left side arithmetic
        let left = parse_primary(cur, src)?;
        // Handle NOT BETWEEN specially: left NOT BETWEEN a AND b
        if matches!(cur.peek_kind(), Some(TKind::Not)) {
            // lookahead for BETWEEN
            let save_idx = cur.idx;
            cur.next();
            if matches!(cur.peek_kind(), Some(TKind::Between)) {
                cur.next();
                // low expr
                let low = parse_primary(cur, src)?;
                // expect AND
                if matches!(cur.peek_kind(), Some(TKind::And)) { cur.next(); } else {
                    let p = cur.peek_pos().unwrap_or(src.len());
                    anyhow::bail!("Syntax error at position {}: expected AND in BETWEEN.\n{}", p, caret_snippet(src, p));
                }
                let high = parse_primary(cur, src)?;
                // NOT BETWEEN -> negate the between (i.e., < low OR > high)
                let ge = WhereExpr::Comp { left: left.clone(), op: CompOp::Ge, right: low };
                let le = WhereExpr::Comp { left: left.clone(), op: CompOp::Le, right: high };
                let between = WhereExpr::And(Box::new(ge), Box::new(le));
                return Ok(negate_where(between));
            } else { cur.idx = save_idx; }
        }

        // BETWEEN variant
        if matches!(cur.peek_kind(), Some(TKind::Between)) {
                cur.next();
                let low = parse_primary(cur, src)?;
                if matches!(cur.peek_kind(), Some(TKind::And)) { cur.next(); } else { let p = cur.peek_pos().unwrap_or(src.len()); anyhow::bail!("Syntax error at position {}: expected AND in BETWEEN.\n{}", p, caret_snippet(src, p)); }
                let high = parse_primary(cur, src)?;
                let ge = WhereExpr::Comp { left: left.clone(), op: CompOp::Ge, right: low };
                let le = WhereExpr::Comp { left, op: CompOp::Le, right: high };
                return Ok(WhereExpr::And(Box::new(ge), Box::new(le)));
        }

        // IS [NOT] NULL variant
        if matches!(cur.peek_kind(), Some(TKind::Is)) {
                let is_pos = cur.peek_pos().unwrap_or(0); cur.next();
                let mut neg = false;
                if matches!(cur.peek_kind(), Some(TKind::Not)) { cur.next(); neg = true; }
                if matches!(cur.peek_kind(), Some(TKind::Null)) { cur.next(); return Ok(WhereExpr::IsNull { expr: left, negated: neg }); } else {
                    let p = cur.peek_pos().unwrap_or(is_pos);
                    anyhow::bail!("Syntax error at position {}: expected NULL after IS{}.\n{}", is_pos, if neg {" NOT"} else {""}, caret_snippet(src, is_pos));
                }
        }
        // LIKE / NOT LIKE
        if matches!(cur.peek_kind(), Some(TKind::Like)) { cur.next(); let right = parse_primary(cur, src)?; return Ok(WhereExpr::Comp { left, op: CompOp::Like, right }); }
        if matches!(cur.peek_kind(), Some(TKind::Not)) {
                let save = cur.idx; cur.next();
                if matches!(cur.peek_kind(), Some(TKind::Like)) { cur.next(); let right = parse_primary(cur, src)?; return Ok(WhereExpr::Comp { left, op: CompOp::NotLike, right }); }
                cur.idx = save;
        }

        // IN / NOT IN (list or subquery)
        if matches!(cur.peek_kind(), Some(TKind::In)) || matches!(cur.peek_kind(), Some(TKind::Not)) {
                let mut neg = false;
                if matches!(cur.peek_kind(), Some(TKind::Not)) { cur.next(); neg = true; if !matches!(cur.peek_kind(), Some(TKind::In)) { cur.idx -= 1; neg = false; } }
                if matches!(cur.peek_kind(), Some(TKind::In)) {
                    cur.next();
                    // expect '(' then either values or SELECT ... ')'
                    if matches!(cur.peek_kind(), Some(TKind::LParen)) {
                        let lparen_pos = cur.peek_pos().unwrap_or(0); cur.next();
                        // If next token begins with SELECT as identifier, parse as subquery by scanning source for matching ')'
                        // fast path: if next non-ws char at lp.pos+1 starts with 'S' and source contains SELECT
                        let mut j = lparen_pos + 1; let bytes = src.as_bytes(); while j < bytes.len() && bytes[j].is_ascii_whitespace() { j += 1; }
                        let rem = &src[j..];
                        if rem.to_uppercase().starts_with("SELECT ") {
                                // scan for matching ')'
                                let mut depth = 1usize; let mut k = j;
                                while k < src.len() && depth > 0 {
                                    let ch = src[k..].chars().next().unwrap();
                                    if ch == '(' { depth += 1; }
                                    else if ch == ')' { depth -= 1; if depth == 0 { break; } }
                                    k += ch.len_utf8();
                                }
                                let inner = &src[j..k];
                                let subq = parse_select(inner.trim())?;
                                // consume tokens until after matching RParen
                                while !matches!(cur.peek_kind(), Some(TKind::RParen)) { if cur.next().is_none() { break; } }
                                if matches!(cur.peek_kind(), Some(TKind::RParen)) { cur.next(); }
                                let expr = if neg { WhereExpr::Any { left, op: CompOp::Ne, subquery: Box::new(subq), negated: false } } else { WhereExpr::Any { left, op: CompOp::Eq, subquery: Box::new(subq), negated: false } };
                                return Ok(expr);
                        }
                        // Otherwise parse list of values: val (, val)* )
                        let mut values: Vec<ArithExpr> = Vec::new();
                        loop {
                            if matches!(cur.peek_kind(), Some(TKind::RParen)) { cur.next(); break; }
                            let val = parse_primary(cur, src)?; values.push(val);
                            if matches!(cur.peek_kind(), Some(TKind::Comma)) { cur.next(); continue; }
                            else if matches!(cur.peek_kind(), Some(TKind::RParen)) { cur.next(); break; }
                            else { let p = cur.peek_pos().unwrap_or(src.len()); anyhow::bail!("Syntax error at position {}: expected ',' or ')'.\n{}", p, caret_snippet(src, p)); }
                        }
                        if values.is_empty() { anyhow::bail!("IN clause requires at least one value"); }
                        // Build OR/AND chain
                        let mut result = WhereExpr::Comp { left: left.clone(), op: if neg { CompOp::Ne } else { CompOp::Eq }, right: values[0].clone() };
                        for v in values.iter().skip(1) {
                            let cmp = WhereExpr::Comp { left: left.clone(), op: if neg { CompOp::Ne } else { CompOp::Eq }, right: v.clone() };
                            result = if neg { WhereExpr::And(Box::new(result), Box::new(cmp)) } else { WhereExpr::Or(Box::new(result), Box::new(cmp)) };
                        }
                        return Ok(result);
                    } else { let p = cur.peek_pos().unwrap_or(src.len()); anyhow::bail!("Syntax error at position {}: expected '(' after IN.\n{}", p, caret_snippet(src, p)); }
                }
        }

        // EXISTS (subquery) and NOT EXISTS handled via NOT at higher level; here handle plain EXISTS
        if matches!(cur.peek_kind(), Some(TKind::Exists)) {
                cur.next();
                if matches!(cur.peek_kind(), Some(TKind::LParen)) { let lpos = cur.peek_pos().unwrap_or(0); cur.next();
                    // find matching ')' starting from lpos+1
                    let mut depth = 1usize; let mut k = lpos + 1;
                    while k < src.len() && depth > 0 {
                        let ch = src[k..].chars().next().unwrap();
                        if ch == '(' { depth += 1; }
                        else if ch == ')' { depth -= 1; if depth == 0 { break; } }
                        k += ch.len_utf8();
                    }
                    let inner = &src[lpos+1..k];
                    let subq = parse_select(inner.trim())?;
                    // advance tokens until after the matching RParen, tracking nested depth
                    let mut depth_toks: i32 = 1; // we already consumed the opening '('
                    loop {
                        match cur.peek_kind() {
                            Some(TKind::LParen) => { cur.next(); depth_toks += 1; }
                            Some(TKind::RParen) => { cur.next(); depth_toks -= 1; if depth_toks == 0 { break; } }
                            Some(_) => { cur.next(); }
                            None => break,
                        }
                    }
                    return Ok(WhereExpr::Exists { negated: false, subquery: Box::new(subq) });
                } else { let p = cur.peek_pos().unwrap_or(src.len()); anyhow::bail!("Syntax error at position {}: expected '(' after EXISTS.\n{}", p, caret_snippet(src, p)); }
        }

        // ANY/ALL with comparator: left op ANY|ALL (subquery)
        {
            let op = match cur.peek_kind() { Some(TKind::Eq)=>Some(CompOp::Eq), Some(TKind::Ne)=>Some(CompOp::Ne), Some(TKind::Lt)=>Some(CompOp::Lt), Some(TKind::Le)=>Some(CompOp::Le), Some(TKind::Gt)=>Some(CompOp::Gt), Some(TKind::Ge)=>Some(CompOp::Ge), _=>None };
            if let Some(o) = op {
                cur.next();
                {
                    let is_any = matches!(cur.peek_kind(), Some(TKind::Any)); let is_all = matches!(cur.peek_kind(), Some(TKind::All));
                    if is_any || is_all {
                        cur.next();
                        if matches!(cur.peek_kind(), Some(TKind::LParen)) { let lpos = cur.peek_pos().unwrap_or(0); cur.next();
                            let mut depth = 1usize; let mut k = lpos + 1; while k < src.len() && depth > 0 { let ch = src[k..].chars().next().unwrap(); if ch == '(' { depth += 1; } else if ch == ')' { depth -= 1; if depth == 0 { break; } } k += ch.len_utf8(); }
                            let inner = &src[lpos+1..k]; let subq = parse_select(inner.trim())?;
                            // consume tokens up to the matching ')', tracking nesting
                            let mut depth_toks: i32 = 1;
                            loop {
                                match cur.peek_kind() {
                                    Some(TKind::LParen) => { cur.next(); depth_toks += 1; }
                                    Some(TKind::RParen) => { cur.next(); depth_toks -= 1; if depth_toks == 0 { break; } }
                                    Some(_) => { cur.next(); }
                                    None => break,
                                }
                            }
                            return Ok(if is_any { WhereExpr::Any { left, op: o, subquery: Box::new(subq), negated: false } } else { WhereExpr::All { left, op: o, subquery: Box::new(subq), negated: false } });
                        } else { let p = cur.peek_pos().unwrap_or(src.len()); anyhow::bail!("Syntax error at position {}: expected '(' after {}.\n{}", p, if is_any {"ANY"} else {"ALL"}, caret_snippet(src, p)); }
                    } else {
                        // Fall back to simple comparison with right expression
                        let right = parse_primary(cur, src)?; return Ok(WhereExpr::Comp { left, op: o, right });
                    }
                }
            }
        }

        // standard comparisons
        if let Some(op) = match cur.peek_kind() { Some(TKind::Eq)=>Some(CompOp::Eq), Some(TKind::Ne)=>Some(CompOp::Ne), Some(TKind::Lt)=>Some(CompOp::Lt), Some(TKind::Le)=>Some(CompOp::Le), Some(TKind::Gt)=>Some(CompOp::Gt), Some(TKind::Ge)=>Some(CompOp::Ge), _=>None } {
            cur.next(); let right = parse_primary(cur, src)?; return Ok(WhereExpr::Comp { left, op, right });
        }
        // If no comparator, treat non-null/identifier truthiness as = 1 (compatibility)
        Ok(WhereExpr::Comp { left, op: CompOp::Eq, right: ArithExpr::Term(ArithTerm::Number(1.0)) })
    }

    fn precedence(tok: &Tok) -> i32 { match tok.kind { TKind::Or => 1, TKind::And => 2, _ => 0 } }

    // Helper to negate a boolean WhereExpr without requiring a dedicated Not variant.
    // Applies De Morgan for And/Or and flips comparison operators when possible.
    fn negate_where(e: WhereExpr) -> WhereExpr {
        // Local helper to flip a comparison operator
        fn flip_op(op: CompOp) -> CompOp {
            match op {
                CompOp::Eq => CompOp::Ne,
                CompOp::Ne => CompOp::Eq,
                CompOp::Lt => CompOp::Ge,
                CompOp::Le => CompOp::Gt,
                CompOp::Gt => CompOp::Le,
                CompOp::Ge => CompOp::Lt,
                // Pattern operators
                CompOp::Like => CompOp::NotLike,
                CompOp::NotLike => CompOp::Like,
            }
        }
        match e {
            WhereExpr::And(a, b) => WhereExpr::Or(Box::new(negate_where(*a)), Box::new(negate_where(*b))),
            WhereExpr::Or(a, b) => WhereExpr::And(Box::new(negate_where(*a)), Box::new(negate_where(*b))),
            WhereExpr::Comp { left, op, right } => WhereExpr::Comp { left, op: flip_op(op), right },
            WhereExpr::IsNull { expr, negated } => WhereExpr::IsNull { expr, negated: !negated },
            WhereExpr::Exists { negated, subquery } => WhereExpr::Exists { negated: !negated, subquery },
            WhereExpr::All { left, op, subquery, negated } => WhereExpr::All { left, op, subquery, negated: !negated },
            WhereExpr::Any { left, op, subquery, negated } => WhereExpr::Any { left, op, subquery, negated: !negated },
            // Fallback: treat boolean as predicate and compare to 0 (i.e., NOT e  => e = 0)
            other => WhereExpr::Comp { left: ArithExpr::Predicate(Box::new(other)), op: CompOp::Eq, right: ArithExpr::Term(ArithTerm::Number(0.0)) },
        }
    }

    fn parse_bool_expr(cur: &mut Cursor, src: &str, min_prec: i32) -> Result<WhereExpr> {
        // handle unary NOT
        let mut left = if let Some(t) = cur.peek() {
            if t.kind == TKind::Not {
                cur.next();
                let inner = parse_bool_expr(cur, src, 3)?;
                negate_where(inner)
            } else if t.kind == TKind::LParen {
                cur.next();
                let e = parse_bool_expr(cur, src, 1)?;
                if let Some(t2) = cur.peek() {
                    if t2.kind == TKind::RParen { cur.next(); } else { anyhow::bail!("Syntax error at position {}: expected ')'.\n{}", t2.pos, caret_snippet(src, t2.pos)); }
                } else { anyhow::bail!("Syntax error: unexpected end, expected ')'."); }
                e
            } else if t.kind == TKind::Exists {
                // EXISTS (subquery)
                cur.next();
                if matches!(cur.peek_kind(), Some(TKind::LParen)) {
                    let lpos = cur.peek_pos().unwrap_or(0); cur.next();
                    // find matching ')'
                    let mut depth = 1usize; let mut k = lpos + 1;
                    while k < src.len() && depth > 0 {
                        let ch = src[k..].chars().next().unwrap();
                        if ch == '(' { depth += 1; }
                        else if ch == ')' { depth -= 1; if depth == 0 { break; } }
                        k += ch.len_utf8();
                    }
                    let inner = &src[lpos+1..k];
                    let subq = parse_select(inner.trim())?;
                    // consume tokens until the matching ')', tracking nested pairs
                    let mut depth_toks: i32 = 1;
                    loop {
                        match cur.peek_kind() {
                            Some(TKind::LParen) => { cur.next(); depth_toks += 1; }
                            Some(TKind::RParen) => { cur.next(); depth_toks -= 1; if depth_toks == 0 { break; } }
                            Some(_) => { cur.next(); }
                            None => break,
                        }
                    }
                    WhereExpr::Exists { negated: false, subquery: Box::new(subq) }
                } else {
                    let p = cur.peek_pos().unwrap_or(src.len()); anyhow::bail!("Syntax error at position {}: expected '(' after EXISTS.\n{}", p, caret_snippet(src, p));
                }
            } else { parse_comparison(cur, src)? }
        } else { anyhow::bail!("Syntax error: empty boolean expression") };

        loop {
            let op_tok = match cur.peek() { Some(t) if t.kind==TKind::And || t.kind==TKind::Or => t.clone(), _ => break };
            let prec = precedence(&op_tok);
            if prec < min_prec { break; }
            cur.next();
            let rhs = parse_bool_expr(cur, src, prec + 1)?;
            left = match op_tok.kind { TKind::And => WhereExpr::And(Box::new(left), Box::new(rhs)), TKind::Or => WhereExpr::Or(Box::new(left), Box::new(rhs)), _ => left };
        }
        Ok(left)
    }

    let toks = lex(s)?;
    let mut cur = Cursor{ toks, idx: 0 };
    let expr = parse_bool_expr(&mut cur, s, 1)?;
    if let Some(t) = cur.peek() {
        anyhow::bail!("Syntax error at position {}: unexpected token remaining.\n{}", t.pos, caret_snippet(s, t.pos));
    }
    Ok(expr)
}
