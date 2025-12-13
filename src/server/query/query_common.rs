use anyhow::Result;

/// Build an uppercase "shadow" string used only for keyword scanning.
/// - Converts ASCII letters to uppercase
/// - Replaces newlines (\n, \r) with a single space to keep clause cuts stable across lines
/// - Preserves overall length to keep indices aligned with the original input
pub fn upper_shadow(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\n' | '\r' => out.push(' '),
            _ => out.push(ch.to_ascii_uppercase()),
        }
    }
    out
}

/// Strip SQL comments from the input while preserving content inside string literals.
/// Supported comment styles:
/// - Line comments starting with `--` until end of line
/// - Block comments delimited by `/* ... */` (nesting is supported defensively)
/// Newlines inside comments are preserved to keep line numbers stable; other
/// commented characters are removed. Quotes inside strings are respected and
/// not treated as comment delimiters.
pub fn strip_sql_comments(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;
    let mut in_squote = false;
    let mut in_dquote = false;
    let mut block_depth: i32 = 0;
    let mut line_comment = false;

    while i < bytes.len() {
        let ch = bytes[i] as char;

        // Handle inside line comment
        if line_comment {
            if ch == '\n' {
                out.push('\n');
                line_comment = false;
            } else if ch == '\r' {
                out.push('\r');
                // Do not reset here; Windows CRLF will reset on next \n
            }
            i += 1;
            continue;
        }

        // Handle inside block comment
        if block_depth > 0 {
            // Preserve newlines, skip other chars
            if ch == '\n' || ch == '\r' {
                out.push(ch);
                i += 1;
                continue;
            }
            // Detect nested /* */
            if ch == '/' && i + 1 < bytes.len() && bytes[i + 1] as char == '*' {
                block_depth += 1;
                i += 2;
                continue;
            }
            if ch == '*' && i + 1 < bytes.len() && bytes[i + 1] as char == '/' {
                block_depth -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        // Not inside any comment
        if !in_dquote && ch == '\'' {
            in_squote = !in_squote;
            out.push(ch);
            i += 1;
            continue;
        }
        if !in_squote && ch == '"' {
            in_dquote = !in_dquote;
            out.push(ch);
            i += 1;
            continue;
        }

        if !in_squote && !in_dquote {
            // Start of line comment?
            if ch == '-' && i + 1 < bytes.len() && bytes[i + 1] as char == '-' {
                line_comment = true;
                i += 2;
                continue;
            }
            // Start of block comment?
            if ch == '/' && i + 1 < bytes.len() && bytes[i + 1] as char == '*' {
                block_depth = 1;
                i += 2;
                continue;
            }
        }

        // Default: copy through
        out.push(ch);
        i += 1;
    }

    out
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompOp { Gt, Ge, Lt, Le, Eq, Ne, Like, NotLike }

#[derive(Debug, Clone, PartialEq)]
pub enum ArithOp { Add, Sub, Mul, Div }

#[derive(Debug, Clone, PartialEq)]
pub enum ArithTerm {
    Col { name: String, previous: bool },
    Number(f64),
    Str(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: Vec<SelectItem>,
    pub by_window_ms: Option<i64>,
    pub by_slices: Option<SlicePlan>,
    pub group_by_cols: Option<Vec<String>>,
    // Columns within group_by that use NOTNULL run-based grouping semantics
    pub group_by_notnull_cols: Option<Vec<String>>,
    pub where_clause: Option<WhereExpr>,
    pub having_clause: Option<WhereExpr>,
    pub rolling_window_ms: Option<i64>,
    pub order_by: Option<Vec<(String, bool)>>, // (column/alias, asc=true/desc=false)
    // Optional ANN/EXACT hint attached to ORDER BY clause: "ANN" | "EXACT"
    pub order_by_hint: Option<String>,
    // Raw ORDER BY items as written (per item text), preserved for advanced planners (e.g., ANN)
    pub order_by_raw: Option<Vec<(String, bool)>>,
    pub limit: Option<i64>,
    // Optional INTO destination for persisting SELECT results
    pub into_table: Option<String>,
    pub into_mode: Option<IntoMode>,
    // JOIN support (optional). When present, JOINs take precedence over `base_table`.
    pub base_table: Option<TableRef>,
    pub joins: Option<Vec<JoinClause>>,
    // CTEs (Common Table Expressions) defined by WITH clause
    pub with_ctes: Option<Vec<CTE>>,
    // Full original SQL text for this query, preserved for diagnostics/debugging/reference
    pub original_sql: String,
}


#[derive(Debug, Clone, PartialEq)]
pub enum DatePart { Year, Month, Day, Hour, Minute, Second, Millisecond }

#[derive(Debug, Clone, PartialEq)]
pub enum DateFunc {
    DatePart(DatePart, Box<ArithExpr>),
    DateAdd(DatePart, Box<ArithExpr>, Box<ArithExpr>),
    DateDiff(DatePart, Box<ArithExpr>, Box<ArithExpr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum StrSliceBound {
    Index(i64),
    // pattern expression to search; include controls whether to include the pattern itself in the slice
    Pattern { expr: Box<ArithExpr>, include: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArithExpr {
    Term(ArithTerm),
    BinOp { left: Box<ArithExpr>, op: ArithOp, right: Box<ArithExpr> },
    Func(DateFunc),
    // String slice expression: base[start:stop:step]
    Slice { base: Box<ArithExpr>, start: Option<StrSliceBound>, stop: Option<StrSliceBound>, step: Option<i64> },
    // String concatenation (used by f-strings)
    Concat(Vec<ArithExpr>),
    // Generic function call (potentially Lua UDF)
    Call { name: String, args: Vec<ArithExpr> },
    // Boolean predicate expression (supports comparisons, LIKE/NOT LIKE, AND/OR)
    Predicate(Box<WhereExpr>),
    // CASE expression: CASE WHEN cond1 THEN val1 [WHEN cond2 THEN val2 ...] [ELSE else_val] END
    Case { when_clauses: Vec<(WhereExpr, ArithExpr)>, else_expr: Option<Box<ArithExpr>> },
    // PostgreSQL-style type cast: expr::typename (with optional parameters)
    Cast { expr: Box<ArithExpr>, ty: SqlType },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Text,
    Varchar(Option<i32>),
    // Single-width character types
    Char(Option<i32>),
    // Binary / JSON-like and identifiers
    Bytea,
    Uuid,
    Json,
    Jsonb,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    TimeTz,
    Interval,
    Numeric(Option<(i32, i32)>),
    Regclass,
    // PostgreSQL regtype pseudo-type used in casts like oid::regtype::text
    Regtype,
    // One-dimensional array type with an inner SqlType
    Array(Box<SqlType>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterOp {
    // ADD COLUMN <name> <type> [NULL|NOT NULL] [DEFAULT <expr>]
    AddColumn { name: String, type_key: String, nullable: bool, default_expr: Option<String> },
    // RENAME COLUMN <old> TO <new>
    RenameColumn { from: String, to: String },
    // ALTER COLUMN <name> TYPE <type>
    AlterColumnType { name: String, type_key: String },
    // ADD PRIMARY KEY (col[, ...])
    AddPrimaryKey { columns: Vec<String> },
    // DROP PRIMARY KEY
    DropPrimaryKey,
    // ADD CONSTRAINT <name> USING <udf_name>
    AddConstraint { name: String, udf: String },
    // DROP CONSTRAINT <name>
    DropConstraint { name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    Comp { left: ArithExpr, op: CompOp, right: ArithExpr },
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    // Unary null checks: expr IS NULL / expr IS NOT NULL
    IsNull { expr: ArithExpr, negated: bool },
    // EXISTS (subquery): [NOT] EXISTS (SELECT ...)
    Exists { negated: bool, subquery: Box<Query> },
    // value op ALL (subquery): value = ALL (...), value > ALL (...), etc.
    All { left: ArithExpr, op: CompOp, subquery: Box<Query>, negated: bool },
    // value op ANY (subquery): value = ANY (...), value > ANY (...), etc.
    Any { left: ArithExpr, op: CompOp, subquery: Box<Query>, negated: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc { Avg, Max, Min, Sum, Count, First, Last, Stdev, Delta, Height, Gradient, Quantile(i64), ArrayAgg }

#[derive(Debug, Clone, PartialEq)]
pub enum StrFunc { Upper, Lower }

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunc { RowNumber }

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Option<Vec<String>>,
    pub order_by: Option<Vec<(ArithExpr, bool)>>, // (expression, asc)
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub func: Option<AggFunc>,
    pub str_func: Option<StrFunc>,
    pub window_func: Option<WindowFunc>,
    pub window_spec: Option<WindowSpec>,
    pub column: String,
    pub expr: Option<ArithExpr>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntoMode { Append, Replace }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType { Inner, Left, Right, Full }

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// A table name reference with optional alias
    Table { name: String, alias: Option<String> },
    /// A subquery in FROM clause with required alias
    Subquery { query: Box<Query>, alias: String },
    /// A table-valued function call in FROM/JOIN with optional alias. The `call` is the full text
    /// "func(arg1, arg2, ...)" preserved for evaluation, but naming rules will not use the call text.
    Tvf { call: String, alias: Option<String> },
}

impl TableRef {
    /// Get the table name if this is a Table variant, None for Subquery
    pub fn table_name(&self) -> Option<&str> {
        match self {
            TableRef::Table { name, .. } => Some(name.as_str()),
            TableRef::Subquery { .. } => None,
            TableRef::Tvf { .. } => None,
        }
    }
    
    /// Get the alias for this table reference
    pub fn alias(&self) -> Option<&str> {
        match self {
            TableRef::Table { alias, .. } => alias.as_deref(),
            TableRef::Subquery { alias, .. } => Some(alias.as_str()),
            TableRef::Tvf { alias, .. } => alias.as_deref(),
        }
    }
    
    /// Get the effective name (alias if present, otherwise table name for Table variant, or alias for Subquery)
    pub fn effective_name(&self) -> &str {
        match self {
            TableRef::Table { name, alias } => alias.as_deref().unwrap_or(name.as_str()),
            TableRef::Subquery { alias, .. } => alias.as_str(),
            // For TVFs, only alias is an effective name; otherwise they have no qualifier
            TableRef::Tvf { alias, .. } => alias.as_deref().unwrap_or(""),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause { pub join_type: JoinType, pub right: TableRef, pub on: WhereExpr }

#[derive(Debug, Clone, PartialEq)]
pub struct CTE {
    pub name: String,
    pub query: Box<Query>,
}


#[derive(Debug, Clone, PartialEq)]
pub struct ManualLabel { pub name: Option<String>, pub value: Option<String> }

#[derive(Debug, Clone, PartialEq)]
pub struct ManualRow { pub start: i64, pub end: i64, pub labels: Vec<ManualLabel> }

#[derive(Debug, Clone, PartialEq)]
pub struct SlicePlan {
    pub base: SliceSource, // from USING
    pub clauses: Vec<SliceClause>,
    pub labels: Option<Vec<String>>, // optional LABELS declared after USING
}

#[derive(Debug, Clone, PartialEq)]
pub enum SliceSource {
    Table { database: String, start_col: Option<String>, end_col: Option<String>, where_clause: Option<WhereExpr>, label_values: Option<Vec<String>> },
    Manual { rows: Vec<ManualRow> },
    Plan(Box<SlicePlan>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SliceOp { Intersect, Union }

#[derive(Debug, Clone, PartialEq)]
pub struct SliceClause { pub op: SliceOp, pub source: SliceSource }

// Module-level helper: skip ASCII whitespace from index and return the next position
#[inline]
pub fn skip_ws(s: &str, mut idx: usize) -> usize {
    let b = s.as_bytes();
    while idx < b.len() && b[idx].is_ascii_whitespace() { idx += 1; }
    idx
}

// Module-level helper: read a non-whitespace token starting at `start`.
#[inline]
pub fn read_word(s: &str, start: usize) -> (String, usize) {
    let b = s.as_bytes();
    let mut j = start;
    while j < b.len() && !b[j].is_ascii_whitespace() { j += 1; }
    (s[start..j].to_string(), j)
}


pub fn split_once_any<'a>(s: &'a str, seps: &[&str]) -> (&'a str, Option<&'a str>) {
    for sep in seps {
        if let Some(i) = s.to_uppercase().find(&sep.to_uppercase()) {
            let (a, b) = s.split_at(i);
            return (a, Some(&b[sep.len()..]));
        }
    }
    (s, None)
}


pub fn find_token_ci(tokens: &[String], needle: &str) -> Option<usize> {
    let n = needle.to_uppercase();
    let mut depth = 0;
    for (i, t) in tokens.iter().enumerate() {
        // Track parenthesis depth
        for ch in t.chars() {
            if ch == '(' { depth += 1; }
            else if ch == ')' { depth -= 1; }
        }
        // Only match at depth 0 (outside parentheses)
        if depth == 0 && t.to_uppercase() == n {
            return Some(i);
        }
    }
    None
}

pub fn parse_iso8601_to_ms(tok: &str) -> Option<i64> {
    // Accept bare or single-quoted ISO 8601/RFC3339 timestamps and common variants without timezone (assume UTC)
    let mut s = tok.trim();
    if s.len() >= 2 {
        let first = s.as_bytes()[0] as char;
        let last = s.as_bytes()[s.len()-1] as char;
        if first == '\'' && last == '\'' {
            s = &s[1..s.len()-1];
        }
    }
    // Try RFC3339 (e.g., 2025-01-01T00:00:00Z or with offset)
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    // Try NaiveDateTime with T separator, assume UTC
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
        return Some(dt.timestamp_millis());
    }
    // Try NaiveDateTime with space separator
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
        return Some(dt.timestamp_millis());
    }
    // Try date-only (YYYY-MM-DD) at midnight UTC
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(ndt) = nd.and_hms_opt(0, 0, 0) {
            let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
            return Some(dt.timestamp_millis());
        }
    }
    None
}

pub fn prec(op: &ArithOp) -> i32 { match op { ArithOp::Add|ArithOp::Sub => 1, ArithOp::Mul|ArithOp::Div => 2 } }



pub fn find_next_keyword(s: &str, kws: &[&str]) -> Option<usize> {
    let up = s.to_uppercase();
    let mut best: Option<usize> = None;
    for kw in kws {
        if let Some(i) = up.find(&kw.to_uppercase()) {
            best = Some(best.map(|b| b.min(i)).unwrap_or(i));
        }
    }
    best
}


pub fn extract_slice_block(s: &str) -> Result<(&str, usize)> {
    // s starts after SLICE, allow forms: SLICE ( ... ) or SLICE{ ... }
    // Return inner text and total bytes consumed from the ORIGINAL input `s` (including any leading whitespace)
    let t = s.trim_start();
    if t.is_empty() || (t.as_bytes()[0] != b'(' && t.as_bytes()[0] != b'{') {
        anyhow::bail!("SLICE expects ( ... )");
    }
    let lead_ws = s.len() - t.len();
    let open = t.as_bytes()[0] as char;
    let close = if open == '(' { ')' } else { '}' };
    let mut depth = 0i32;
    let mut start_inner = 0usize;
    for (idx, ch) in t.char_indices() {
        if ch == open {
            depth += 1;
            if depth == 1 {
                start_inner = idx + ch.len_utf8();
            }
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                let inner = &t[start_inner..idx];
                let consumed_in_t = idx + ch.len_utf8();
                let consumed_total = lead_ws + consumed_in_t;
                return Ok((inner, consumed_total));
            }
        }
    }
    anyhow::bail!("Unterminated SLICE block")
}

pub fn extract_paren_block(s: &str) -> Option<(&str, usize)> {
    let t = s;
    if t.is_empty() || t.as_bytes()[0] != b'(' { return None; }
    let mut depth = 0i32; let mut i0 = 0usize; let mut started = false;
    for (idx, ch) in t.char_indices() {
        if ch == '(' { depth += 1; if !started { started = true; i0 = idx + ch.len_utf8(); } }
        else if ch == ')' { depth -= 1; if depth == 0 { let inner = &t[i0..idx]; return Some((inner, idx + 1)); } }
    }
    None
}

pub fn split_csv_ignoring_quotes(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_s = false; let mut in_d = false;
    for ch in s.chars() {
        match ch {
            '\'' if !in_d => { in_s = !in_s; cur.push(ch); }
            '"' if !in_s => { in_d = !in_d; cur.push(ch); }
            ',' if !in_s && !in_d => { out.push(cur.trim().to_string()); cur.clear(); }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() || s.ends_with(',') { out.push(cur.trim().to_string()); }
    out
}