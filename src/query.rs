use anyhow::{Result, bail};
use regex::Regex;
use tracing::{debug};

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
}

impl TableRef {
    /// Get the table name if this is a Table variant, None for Subquery
    pub fn table_name(&self) -> Option<&str> {
        match self {
            TableRef::Table { name, .. } => Some(name.as_str()),
            TableRef::Subquery { .. } => None,
        }
    }
    
    /// Get the alias for this table reference
    pub fn alias(&self) -> Option<&str> {
        match self {
            TableRef::Table { alias, .. } => alias.as_deref(),
            TableRef::Subquery { alias, .. } => Some(alias.as_str()),
        }
    }
    
    /// Get the effective name (alias if present, otherwise table name for Table variant, or alias for Subquery)
    pub fn effective_name(&self) -> &str {
        match self {
            TableRef::Table { name, alias } => alias.as_deref().unwrap_or(name.as_str()),
            TableRef::Subquery { alias, .. } => alias.as_str(),
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

#[derive(Debug, Clone)]
pub enum Command {
    Select(Query),
    // UNION or UNION ALL of multiple SELECT queries
    SelectUnion { queries: Vec<Query>, all: bool },
    // VIEW DDL
    // CREATE [OR ALTER] VIEW <name> AS <SELECT...>
    CreateView { name: String, or_alter: bool, definition_sql: String },
    // DROP VIEW [IF EXISTS] <name>
    DropView { name: String, if_exists: bool },
    // SHOW VIEW <name>
    ShowView { name: String },
    Calculate { target_sensor: String, query: Query },
    // UPDATE <table> SET col = value[, ...] [WHERE ...]
    Update { table: String, assignments: Vec<(String, ArithTerm)>, where_clause: Option<WhereExpr> },
    DeleteRows { database: String, where_clause: Option<WhereExpr> },
    DeleteColumns { database: String, columns: Vec<String>, where_clause: Option<WhereExpr> },
    SchemaShow { database: String },
    // Allow optional PRIMARY KEY and PARTITION BY on schema additions for regular tables
    SchemaAdd { database: String, entries: Vec<(String, String)>, primary_key: Option<Vec<String>>, partitions: Option<Vec<String>> },
    // Legacy/compat
    DatabaseAdd { database: String },
    DatabaseDelete { database: String },
    // New DDL
    CreateDatabase { name: String },
    DropDatabase { name: String },
    RenameDatabase { from: String, to: String },
    CreateSchema { path: String },
    DropSchema { path: String },
    RenameSchema { from: String, to: String },
    CreateTimeTable { table: String },
    DropTimeTable { table: String },
    RenameTimeTable { from: String, to: String },
    // Regular parquet table DDL
    // Optional PRIMARY KEY / PARTITION BY metadata on create
    CreateTable { table: String, primary_key: Option<Vec<String>>, partitions: Option<Vec<String>> },
    DropTable { table: String, if_exists: bool },
    RenameTable { from: String, to: String },
    // KV store/keys DDL/DML
    CreateStore { database: String, store: String },
    DropStore { database: String, store: String },
    RenameStore { database: String, from: String, to: String },
    // Discovery/listing
    ListStores { database: String },
    ListKeys { database: String, store: String },
    DescribeKey { database: String, store: String, key: String },
    // Key operations
    WriteKey { database: String, store: String, key: String, value: String, ttl_ms: Option<i64>, reset_on_access: Option<bool> },
    ReadKey { database: String, store: String, key: String },
    DropKey { database: String, store: String, key: String },
    RenameKey { database: String, store: String, from: String, to: String },
    UserAdd { username: String, password: String, is_admin: bool, perms: Vec<String>, scope_db: Option<String> },
    UserDelete { username: String, scope_db: Option<String> },
    UserAlter { username: String, new_password: Option<String>, is_admin: Option<bool>, perms: Option<Vec<String>>, scope_db: Option<String> },
    // Scripts
    CreateScript { path: String, code: String },
    DropScript { path: String },
    RenameScript { from: String, to: String },
    LoadScript { path: Option<String> },
    // Global session-affecting commands
    UseDatabase { name: String },
    UseSchema { name: String },
    Set { variable: String, value: String },
    // SHOW commands
    ShowTransactionIsolation,
    ShowStandardConformingStrings,
    ShowServerVersion,
    ShowClientEncoding,
    ShowServerEncoding,
    ShowDateStyle,
    ShowIntegerDateTimes,
    ShowTimeZone,
    ShowSearchPath,
    ShowDefaultTransactionIsolation,
    ShowTransactionReadOnly,
    ShowApplicationName,
    ShowExtraFloatDigits,
    ShowAll,
    ShowSchemas,
    ShowTables,
    ShowObjects,
    ShowScripts,
    // Vector index catalog
    CreateVectorIndex { name: String, table: String, column: String, algo: String, options: Vec<(String, String)> },
    DropVectorIndex { name: String },
    ShowVectorIndex { name: String },
    ShowVectorIndexes,
    // Graph catalog
    CreateGraph { name: String, nodes: Vec<(String, String)>, edges: Vec<(String, String, String)>, nodes_table: Option<String>, edges_table: Option<String> },
    DropGraph { name: String },
    ShowGraph { name: String },
    ShowGraphs,
    // DESCRIBE <object> (table/view) and DESCRIBE KEY ... (existing)
    // For backward compatibility, DESCRIBE KEY is parsed specially; otherwise
    // we treat DESCRIBE <object> as DescribeObject with a possibly unqualified name.
    DescribeObject { name: String },
    Slice(SlicePlan),
    Insert { table: String, columns: Vec<String>, values: Vec<Vec<ArithTerm>> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SlicePlan {
    pub base: SliceSource, // from USING
    pub clauses: Vec<SliceClause>,
    pub labels: Option<Vec<String>>, // optional LABELS declared after USING
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManualLabel { pub name: Option<String>, pub value: Option<String> }

#[derive(Debug, Clone, PartialEq)]
pub struct ManualRow { pub start: i64, pub end: i64, pub labels: Vec<ManualLabel> }

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

fn parse_window(s: &str) -> Result<i64> {
    // e.g. 1s, 5m, 1h
    let re = Regex::new(r"^(?i)(\d+)(ms|s|m|h|d)$")?;
    let caps = re.captures(s.trim()).ok_or_else(|| anyhow::anyhow!("Invalid window: {}", s))?;
    let n: i64 = caps.get(1).unwrap().as_str().parse()?;
    let unit = caps.get(2).unwrap().as_str().to_lowercase();
    let ms = match unit.as_str() {
        "ms" => n,
        "s" => n * 1000,
        "m" => n * 60_000,
        "h" => n * 3_600_000,
        "d" => n * 86_400_000,
        _ => n,
    };
    Ok(ms)
}

fn split_union_queries(input: &str) -> Result<(Vec<&str>, bool)> {
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

pub fn parse(input: &str) -> Result<Command> {
    let s = input.trim();
    let sup = s.to_uppercase();
    if sup.starts_with("SLICE ") || sup == "SLICE" {
        let plan = parse_slice(s)?;
        return Ok(Command::Slice(plan));
    }
    if sup.starts_with("CALCULATE ") {
        // CALCULATE sensor_1, _time as SELECT ...
        let rest = s[10..].trim();
        let parts: Vec<&str> = rest.splitn(2, " as ").collect();
        if parts.len() != 2 {
            bail!("Invalid CALCULATE syntax");
        }
        let left = parts[0].trim();
        let target_sensor = left.split(',').next().unwrap().trim().to_string();
        let select_part = parts[1].trim();
        let q = parse_select(select_part)?;
        return Ok(Command::Calculate { target_sensor, query: q });
    }
    if sup.starts_with("WITH ") || sup.starts_with("SELECT") {
        // Detect UNION / UNION ALL at top-level using a parser that respects nesting
        let (parts, all) = split_union_queries(s)?;
        if parts.len() > 1 {
            let mut queries: Vec<Query> = Vec::new();
            for part in parts { queries.push(parse_select(part)?); }
            return Ok(Command::SelectUnion { queries, all });
        } else {
            let q = parse_select(s)?;
            return Ok(Command::Select(q));
        }
    }
    if sup.starts_with("SHOW ") || sup == "SHOW" {
        return parse_show(s);
    }
    if sup.starts_with("USE ") {
        return parse_use(s);
    }
    if sup.starts_with("LOAD ") {
        return parse_load(s);
    }
    if sup.starts_with("DELETE ") {
        return parse_delete(s);
    }
    if sup.starts_with("UPDATE ") {
        return parse_update(s);
    }
    if sup.starts_with("SCHEMA ") {
        return parse_schema(s);
    }
    if sup.starts_with("DATABASE ") {
        return parse_database(s);
    }
    if sup.starts_with("CREATE ") {
        return parse_create(s);
    }
    if sup.starts_with("DROP ") {
        return parse_drop(s);
    }
    if sup.starts_with("RENAME ") {
        return parse_rename(s);
    }
    if sup.starts_with("WRITE ") {
        return parse_write(s);
    }
    if sup.starts_with("READ ") {
        return parse_read(s);
    }
    if sup.starts_with("LIST ") {
        return parse_list(s);
    }
    if sup.starts_with("DESCRIBE ") {
        return parse_describe(s);
    }
    if sup.starts_with("USER ") {
        return parse_user(s);
    }
    if sup.starts_with("SET ") {
        return parse_set(s);
    }
    if sup.starts_with("INSERT ") {
        return parse_insert(s);
    }
    bail!("Unsupported DDL-SQL command: {} ", sup)
}

fn parse_create(s: &str) -> Result<Command> {
    // CREATE DATABASE <db>
    // CREATE SCHEMA <db>/<schema> | <schema>
    // CREATE TIME TABLE <db>/<schema>/<table>.time
    // CREATE TABLE <db>/<schema>/<table> | <table>
    let rest = s[6..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("DATABASE ") {
        let name = rest[9..].trim();
        if name.is_empty() { anyhow::bail!("Invalid CREATE DATABASE: missing database name"); }
        return Ok(Command::CreateDatabase { name: name.to_string() });
    }
    if up.starts_with("VIEW ") || up.starts_with("OR ALTER VIEW ") {
        // CREATE [OR ALTER] VIEW <name> AS <SELECT...>
        // Capture the definition SQL verbatim after AS (can be SELECT or SELECT UNION)
        let mut or_alter = false;
        let after = if up.starts_with("OR ALTER VIEW ") {
            or_alter = true;
            &rest["OR ALTER VIEW ".len()..]
        } else {
            &rest["VIEW ".len()..]
        };
        let after = after.trim();
        // Split on AS (case-insensitive)
        let up_after = after.to_uppercase();
        let as_pos = up_after.find(" AS ").ok_or_else(|| anyhow::anyhow!("Invalid CREATE VIEW: expected AS"))?;
        let name = after[..as_pos].trim();
        let def_sql = after[as_pos + 4..].trim();
        if name.is_empty() { anyhow::bail!("Invalid CREATE VIEW: missing view name"); }
        if def_sql.is_empty() { anyhow::bail!("Invalid CREATE VIEW: missing SELECT definition after AS"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::CreateView { name: normalized_name, or_alter, definition_sql: def_sql.to_string() });
    }
    if up.starts_with("VECTOR INDEX ") {
        // CREATE VECTOR INDEX <name> ON <table>(<column>) USING hnsw [WITH (k=v, ...)]
        let after = &rest["VECTOR INDEX ".len()..];
        let after = after.trim();
        // name
        let (name_tok, mut i) = read_word(after, 0);
        if name_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing index name"); }
        let name_norm = crate::ident::normalize_identifier(name_tok);
        i = skip_ws(after, i);
        let rem = &after[i..];
        let rem_up = rem.to_uppercase();
        if !rem_up.starts_with("ON ") { anyhow::bail!("Invalid CREATE VECTOR INDEX: expected ON <table>(<column>)"); }
        let mut j = 3; // after ON 
        // table name until '(' or whitespace
        let (table_tok, k1) = read_word(rem, j);
        if table_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing table after ON"); }
        j = k1; j = skip_ws(rem, j);
        if j >= rem.len() || rem.as_bytes()[j] as char != '(' { anyhow::bail!("Invalid CREATE VECTOR INDEX: expected (column) after table name"); }
        j += 1; // past '('
        // read column until ')'
        let mut col_end = j;
        while col_end < rem.len() && rem.as_bytes()[col_end] as char != ')' { col_end += 1; }
        if col_end >= rem.len() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing ')' after column"); }
        let column_tok = rem[j..col_end].trim();
        if column_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing column name"); }
        j = col_end + 1;
        j = skip_ws(rem, j);
        let rem2 = &rem[j..];
        let rem2_up = rem2.to_uppercase();
        if !rem2_up.starts_with("USING ") { anyhow::bail!("Invalid CREATE VECTOR INDEX: expected USING <algo>"); }
        let mut k = 6; // after USING 
        let (algo_tok, k2) = read_word(rem2, k);
        if algo_tok.is_empty() { anyhow::bail!("Invalid CREATE VECTOR INDEX: missing algorithm after USING"); }
        k = k2; k = skip_ws(rem2, k);
        let mut options: Vec<(String, String)> = Vec::new();
        if k < rem2.len() {
            let rem3 = &rem2[k..];
            let rem3_up = rem3.to_uppercase();
            if rem3_up.starts_with("WITH ") {
                let mut x = 5; // after WITH 
                x = skip_ws(rem3, x);
                if x >= rem3.len() || rem3.as_bytes()[x] as char != '(' { anyhow::bail!("Invalid CREATE VECTOR INDEX: expected WITH (k=v,...)"); }
                x += 1;
                // parse until closing ')'
                let mut buf = String::new();
                let mut depth = 1i32;
                let mut y = x;
                while y < rem3.len() {
                    let ch = rem3.as_bytes()[y] as char;
                    if ch == '(' { depth += 1; }
                    else if ch == ')' { depth -= 1; if depth == 0 { break; } }
                    buf.push(ch);
                    y += 1;
                }
                if depth != 0 { anyhow::bail!("Invalid CREATE VECTOR INDEX: unterminated WITH (...)"); }
                // split buf on commas into k=v pairs
                for part in buf.split(',') {
                    let p = part.trim(); if p.is_empty() { continue; }
                    if let Some(eq) = p.find('=') {
                        let k = p[..eq].trim().to_string();
                        let v = p[eq+1..].trim().trim_matches('\'').to_string();
                        options.push((k, v));
                    } else {
                        anyhow::bail!("Invalid option in WITH: expected k=v, got '{}'", p);
                    }
                }
            }
        }
        return Ok(Command::CreateVectorIndex { name: name_norm, table: crate::ident::normalize_identifier(table_tok), column: column_tok.to_string(), algo: algo_tok.to_lowercase(), options });
    }
    if up.starts_with("GRAPH ") {
        // CREATE GRAPH <name> NODES (...) EDGES (...) [USING TABLES (nodes=..., edges=...)]
        let after = &rest["GRAPH ".len()..];
        let after = after.trim();
        let (name_tok, mut i) = read_word(after, 0);
        if name_tok.is_empty() { anyhow::bail!("Invalid CREATE GRAPH: missing name"); }
        i = skip_ws(after, i);
        let rem = &after[i..]; let rem_up = rem.to_uppercase();
        if !rem_up.starts_with("NODES ") { anyhow::bail!("Invalid CREATE GRAPH: expected NODES (...)"); }
        let mut j = 6; // after NODES 
        j = skip_ws(rem, j);
        if j >= rem.len() || rem.as_bytes()[j] as char != '(' { anyhow::bail!("Invalid CREATE GRAPH: expected '(' after NODES"); }
        j += 1; let start_nodes = j;
        let mut depth = 1i32;
        while j < rem.len() && depth > 0 {
            let ch = rem.as_bytes()[j] as char; if ch == '(' { depth += 1; } else if ch == ')' { depth -= 1; }
            j += 1;
        }
        if depth != 0 { anyhow::bail!("Invalid CREATE GRAPH: unterminated NODES(...)"); }
        let nodes_block = &rem[start_nodes..j-1];
        // parse nodes of form Label KEY(col)
        let mut nodes: Vec<(String, String)> = Vec::new();
        for part in nodes_block.split(',') { let p = part.trim(); if p.is_empty() { continue; }
            let up = p.to_uppercase();
            if let Some(kpos) = up.find(" KEY(") {
                let label = p[..kpos].trim();
                if let Some(rp) = p[kpos+5..].find(')') { let key = p[kpos+5..kpos+5+rp].trim(); nodes.push((label.to_string(), key.to_string())); } else { anyhow::bail!("Invalid NODES entry: expected KEY(...)"); }
            } else { anyhow::bail!("Invalid NODES entry: expected Label KEY(col)"); }
        }
        // After nodes, expect EDGES
        let rem2 = &rem[j..]; let rem2 = rem2.trim_start(); let rem2_up = rem2.to_uppercase();
        if !rem2_up.starts_with("EDGES ") { anyhow::bail!("Invalid CREATE GRAPH: expected EDGES (...)"); }
        let mut k = 6; k = skip_ws(rem2, k);
        if k >= rem2.len() || rem2.as_bytes()[k] as char != '(' { anyhow::bail!("Invalid CREATE GRAPH: expected '(' after EDGES"); }
        k += 1; let start_edges = k; let mut d2 = 1i32; while k < rem2.len() && d2 > 0 { let ch = rem2.as_bytes()[k] as char; if ch == '(' { d2 += 1; } else if ch == ')' { d2 -= 1; } k += 1; }
        if d2 != 0 { anyhow::bail!("Invalid CREATE GRAPH: unterminated EDGES(...)"); }
        let edges_block = &rem2[start_edges..k-1];
        // parse edges of form Type FROM A TO B
        let mut edges: Vec<(String, String, String)> = Vec::new();
        for part in edges_block.split(',') { let p = part.trim(); if p.is_empty() { continue; }
            let up = p.to_uppercase();
            if let Some(fp) = up.find(" FROM ") { if let Some(tp) = up[fp+6..].find(" TO ") {
                let et = p[..fp].trim();
                let from = p[fp+6..fp+6+tp].trim();
                let to = p[fp+6+tp+4..].trim();
                edges.push((et.to_string(), from.to_string(), to.to_string()));
            } else { anyhow::bail!("Invalid EDGES entry: expected FROM ... TO ..."); } } else { anyhow::bail!("Invalid EDGES entry: expected Type FROM A TO B"); }
        }
        // Optional USING TABLES (nodes=..., edges=...)
        let rem3 = &rem2[k..]; let rem3 = rem3.trim_start(); let rem3_up = rem3.to_uppercase();
        let mut nodes_table: Option<String> = None; let mut edges_table: Option<String> = None;
        if rem3_up.starts_with("USING TABLES ") {
            let mut x = 13; x = skip_ws(rem3, x);
            if x >= rem3.len() || rem3.as_bytes()[x] as char != '(' { anyhow::bail!("Invalid USING TABLES: expected (nodes=..., edges=...)"); }
            x += 1; let mut buf = String::new(); let mut depth3 = 1i32; let mut y = x; while y < rem3.len() { let ch = rem3.as_bytes()[y] as char; if ch == '(' { depth3 += 1; } else if ch == ')' { depth3 -= 1; if depth3 == 0 { break; } } buf.push(ch); y += 1; }
            if depth3 != 0 { anyhow::bail!("Invalid USING TABLES: unterminated (...) block"); }
            for part in buf.split(',') { let p = part.trim(); if p.is_empty() { continue; }
                if let Some(eq) = p.find('=') { let k = p[..eq].trim().to_lowercase(); let v = p[eq+1..].trim(); if k == "nodes" { nodes_table = Some(v.to_string()); } else if k == "edges" { edges_table = Some(v.to_string()); } }
            }
        }
        return Ok(Command::CreateGraph { name: crate::ident::normalize_identifier(name_tok), nodes, edges, nodes_table, edges_table });
    }
    if up.starts_with("SCRIPT ") {
        // CREATE SCRIPT name AS 'code'
        let after = &rest[7..];
        let parts: Vec<&str> = after.splitn(2, " AS ").collect();
        if parts.len() != 2 { anyhow::bail!("Invalid CREATE SCRIPT syntax. Use: CREATE SCRIPT <path> AS '<code>'"); }
        let name = parts[0].trim();
        let code = parts[1].trim();
        // strip single quotes around code if present
        let code_s = if code.starts_with('\'') && code.ends_with('\'') && code.len() >= 2 { &code[1..code.len()-1] } else { code };
        if name.is_empty() { anyhow::bail!("Invalid CREATE SCRIPT: missing name"); }
        return Ok(Command::CreateScript { path: name.to_string(), code: code_s.to_string() });
    }
    if up.starts_with("SCHEMA ") {
        let path = rest[7..].trim();
        if path.is_empty() { anyhow::bail!("Invalid CREATE SCHEMA: missing schema name"); }
        let normalized_path = crate::ident::normalize_identifier(path);
        return Ok(Command::CreateSchema { path: normalized_path });
    }
    if up.starts_with("STORE ") {
        // CREATE STORE <db>.store.<store>
        let addr = rest[6..].trim();
        let (db, st) = parse_store_addr(addr)?;
        return Ok(Command::CreateStore { database: db, store: st });
    }
    if up.starts_with("TIME TABLE ") || up == "TIME TABLE" {
        let db = if up == "TIME TABLE" { "" } else { &rest[11..] };
        let table = db.trim();
        if table.is_empty() { anyhow::bail!("Invalid CREATE TIME TABLE: missing time table name"); }
        if !table.ends_with(".time") { anyhow::bail!("CREATE TIME TABLE target must end with .time"); }
        // Prefer new variant while keeping legacy Command::DatabaseAdd path available elsewhere
        return Ok(Command::CreateTimeTable { table: table.to_string() });
    }
    if up.starts_with("TABLE ") || up == "TABLE" {
        let arg = if up == "TABLE" { "" } else { &rest[6..] };
        let t = arg.trim();
        if t.is_empty() { anyhow::bail!("Invalid CREATE TABLE: missing table name"); }
        // Split table name and optional clauses
        let mut parts = t.splitn(2, char::is_whitespace);
        let table_name = parts.next().unwrap().trim();
        if table_name.ends_with(".time") { anyhow::bail!("CREATE TABLE cannot target a .time table; use CREATE TIME TABLE"); }
        let mut primary_key: Option<Vec<String>> = None;
        let mut partitions: Option<Vec<String>> = None;
        if let Some(tail) = parts.next() {
            let tail_up = tail.to_uppercase();
            let parse_list = |s: &str| -> Vec<String> { s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect() };
            if let Some(i) = tail_up.find("PRIMARY KEY") {
                if let Some(p1) = tail[i..].find('(') { if let Some(p2) = tail[i+p1+1..].find(')') {
                    let start = i + p1 + 1; let end = i + p1 + 1 + p2; let cols = parse_list(&tail[start..end]); if !cols.is_empty() { primary_key = Some(cols); }
                }}
            }
            if let Some(i) = tail_up.find("PARTITION BY") {
                if let Some(p1) = tail[i..].find('(') { if let Some(p2) = tail[i+p1+1..].find(')') {
                    let start = i + p1 + 1; let end = i + p1 + 1 + p2; let cols = parse_list(&tail[start..end]); if !cols.is_empty() { partitions = Some(cols); }
                }}
            }
        }
        return Ok(Command::CreateTable { table: table_name.to_string(), primary_key, partitions });
    }
    anyhow::bail!("Invalid CREATE syntax")
}

fn parse_load(s: &str) -> Result<Command> {
    // LOAD SCRIPT <path> | LOAD SCRIPT ALL
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("SCRIPT ") {
        let arg = rest[7..].trim();
        if arg.eq_ignore_ascii_case("ALL") { return Ok(Command::LoadScript { path: None }); }
        if arg.is_empty() { anyhow::bail!("Invalid LOAD SCRIPT: missing name"); }
        return Ok(Command::LoadScript { path: Some(arg.to_string()) });
    }
    anyhow::bail!("Invalid LOAD syntax")
}

fn parse_drop(s: &str) -> Result<Command> {
    // DROP DATABASE <db>
    // DROP SCHEMA <db>/<schema>
    // DROP TIME TABLE <db>/<schema>/<table>.time
    // DROP TABLE <db>/<schema>/<table> | <table>
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("VIEW ") || up.starts_with("VIEW") {
        // DROP VIEW [IF EXISTS] <name>
        let mut tail = if up == "VIEW" { "" } else { &rest["VIEW ".len()..] };
        tail = tail.trim();
        let tail_up = tail.to_uppercase();
        let mut if_exists = false;
        if tail_up.starts_with("IF EXISTS ") {
            if_exists = true;
            tail = &tail["IF EXISTS ".len()..].trim();
        }
        if tail.is_empty() { anyhow::bail!("Invalid DROP VIEW: missing view name"); }
        let normalized_name = crate::ident::normalize_identifier(tail);
        return Ok(Command::DropView { name: normalized_name, if_exists });
    }
    if up.starts_with("VECTOR INDEX ") {
        // DROP VECTOR INDEX <name>
        let name = rest["VECTOR INDEX ".len()..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP VECTOR INDEX: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::DropVectorIndex { name: normalized_name });
    }
    if up.starts_with("GRAPH ") {
        let name = rest["GRAPH ".len()..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP GRAPH: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::DropGraph { name: normalized_name });
    }
    if up.starts_with("DATABASE ") {
        let name = rest[9..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP DATABASE: missing database name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::DropDatabase { name: normalized_name });
    }
    if up.starts_with("SCRIPT ") {
        let name = rest[7..].trim();
        if name.is_empty() { anyhow::bail!("Invalid DROP SCRIPT: missing name"); }
        // Script names already normalized in scripts.rs, pass as-is for now
        return Ok(Command::DropScript { path: name.to_string() });
    }
    if up.starts_with("SCHEMA ") {
        let path = rest[7..].trim();
        if path.is_empty() { anyhow::bail!("Invalid DROP SCHEMA: missing schema name"); }
        let normalized_path = crate::ident::normalize_identifier(path);
        return Ok(Command::DropSchema { path: normalized_path });
    }
    if up.starts_with("TIME TABLE ") {
        let table = rest[11..].trim();
        if table.is_empty() { anyhow::bail!("Invalid DROP TIME TABLE: missing time table name"); }
        if !table.ends_with(".time") { anyhow::bail!("DROP TIME TABLE target must end with .time"); }
        return Ok(Command::DropTimeTable { table: table.to_string() });
    }
    if up.starts_with("STORE ") {
        // DROP STORE <db>.store.<store>
        let addr = rest[6..].trim();
        let (db, st) = parse_store_addr(addr)?;
        return Ok(Command::DropStore { database: db, store: st });
    }
    if up.starts_with("TABLE ") {
        let mut table = rest[6..].trim();
        let mut if_exists = false;
        // Check for optional IF EXISTS
        let table_up = table.to_uppercase();
        if table_up.starts_with("IF EXISTS ") {
            if_exists = true;
            table = &table[10..].trim();
        }
        if table.is_empty() { anyhow::bail!("Invalid DROP TABLE: missing table name"); }
        if table.ends_with(".time") { anyhow::bail!("DROP TABLE cannot target a .time table; use DROP TIME TABLE"); }
        return Ok(Command::DropTable { table: table.to_string(), if_exists });
    }
    anyhow::bail!("Invalid DROP syntax")
}

fn parse_rename(s: &str) -> Result<Command> {
    // RENAME SCRIPT old TO new
    let rest = s[6..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("SCRIPT ") {
        let after = &rest[7..];
        let parts: Vec<&str> = after.splitn(2, " TO ").collect();
        if parts.len() != 2 { anyhow::bail!("Invalid RENAME SCRIPT syntax. Use: RENAME SCRIPT <old> TO <new>"); }
        let old = parts[0].trim();
        let newn = parts[1].trim();
        if old.is_empty() || newn.is_empty() { anyhow::bail!("Invalid RENAME SCRIPT: missing names"); }
        // Script names already normalized in scripts.rs, pass as-is
        return Ok(Command::RenameScript { from: old.to_string(), to: newn.to_string() });
    }
    // Otherwise existing rename handlers below
    // RENAME DATABASE <from> TO <to>
    // RENAME SCHEMA <db>/<from> TO <to>
    // RENAME TIME TABLE <db>/<schema>/<from>.time TO <db>/<schema>/<to>.time OR unqualified TO name (we will rebuild path at exec if needed)
    let rest = s[6..].trim();
    let up = rest.to_uppercase();
    let to_kw = " TO ";
    if up.starts_with("DATABASE ") {
        let arg = &rest[9..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME DATABASE syntax: expected RENAME DATABASE <from> TO <to>"); }
            let normalized_from = crate::ident::normalize_identifier(from);
            let normalized_to = crate::ident::normalize_identifier(to);
            return Ok(Command::RenameDatabase { from: normalized_from, to: normalized_to });
        } else { anyhow::bail!("Invalid RENAME DATABASE: missing TO <new_name>"); }
    }
    if up.starts_with("SCHEMA ") {
        let arg = &rest[7..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME SCHEMA syntax: expected RENAME SCHEMA <from> TO <to>"); }
            let normalized_from = crate::ident::normalize_identifier(from);
            let normalized_to = crate::ident::normalize_identifier(to);
            return Ok(Command::RenameSchema { from: normalized_from, to: normalized_to });
        } else { anyhow::bail!("Invalid RENAME SCHEMA: missing TO <new_name>"); }
    }
    if up.starts_with("TIME TABLE ") {
        let arg = &rest[11..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME TIME TABLE syntax: expected RENAME TIME TABLE <from> TO <to>"); }
            if !from.ends_with(".time") || !to.ends_with(".time") { anyhow::bail!("RENAME TIME TABLE requires .time suffix on both names"); }
            return Ok(Command::RenameTimeTable { from: from.to_string(), to: to.to_string() });
        } else { anyhow::bail!("Invalid RENAME TIME TABLE: missing TO <new_name>"); }
    }
    if up.starts_with("STORE ") {
        // RENAME STORE <db>.store.<from> TO <to>
        let arg = &rest[6..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let left = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if to.is_empty() { anyhow::bail!("Invalid RENAME STORE: missing destination name"); }
            let (db, from_store) = parse_store_addr(left)?;
            return Ok(Command::RenameStore { database: db, from: from_store, to: to.to_string() });
        } else { anyhow::bail!("Invalid RENAME STORE: missing TO <new_name>"); }
    }
    if up.starts_with("TABLE ") {
        let arg = &rest[6..];
        let up2 = arg.to_uppercase();
        if let Some(i) = up2.find(to_kw) {
            let from = arg[..i].trim();
            let to = arg[i+to_kw.len()..].trim();
            if from.is_empty() || to.is_empty() { anyhow::bail!("Invalid RENAME TABLE syntax: expected RENAME TABLE <from> TO <to>"); }
            if from.ends_with(".time") || to.ends_with(".time") { anyhow::bail!("RENAME TABLE is for regular tables only; use RENAME TIME TABLE for .time tables"); }
            return Ok(Command::RenameTable { from: from.to_string(), to: to.to_string() });
        } else { anyhow::bail!("Invalid RENAME TABLE: missing TO <new_name>"); }
    }
    anyhow::bail!("Invalid RENAME syntax")
}

fn parse_database(s: &str) -> Result<Command> {
    // DATABASE ADD <db> | DATABASE DELETE <db> | DATABASE DROP <db>
    let rest = s[9..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("ADD ") {
        let db = rest[4..].trim();
        if db.is_empty() { anyhow::bail!("Invalid DATABASE ADD: missing database"); }
        return Ok(Command::DatabaseAdd { database: db.to_string() });
    }
    if up.starts_with("DELETE ") || up.starts_with("DROP ") {
        let db = if up.starts_with("DELETE ") { &rest[7..] } else { &rest[5..] };
        let db = db.trim();
        if db.is_empty() { anyhow::bail!("Invalid DATABASE DELETE: missing database"); }
        return Ok(Command::DatabaseDelete { database: db.to_string() });
    }
    anyhow::bail!("Invalid DATABASE syntax")
}

fn parse_user(s: &str) -> Result<Command> {
    // USER ADD <username> PASSWORD '<pw>' [ADMIN] [PERMISSIONS (<list>)] [GLOBAL | (IN|FROM|TO) <db>]
    // USER DELETE <username> [GLOBAL | (IN|FROM|TO) <db>]
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("ADD ") {
        let mut tail = &rest[4..];
        // username up to space
        let mut parts = tail.trim().splitn(2, ' ');
        let username = parts.next().unwrap_or("").trim();
        if username.is_empty() { anyhow::bail!("USER ADD: missing username"); }
        tail = parts.next().unwrap_or("").trim_start();
        let tail_up = tail.to_uppercase();
        if !tail_up.starts_with("PASSWORD ") { anyhow::bail!("USER ADD: expected PASSWORD"); }
        let after_pw = &tail[9..].trim();
        // password token: quoted string until next space or end; accept single quotes
        let pw = if after_pw.starts_with('\'') {
            if let Some(idx) = after_pw[1..].find('\'') { &after_pw[1..1+idx] } else { anyhow::bail!("USER ADD: unterminated password"); }
        } else { // allow unquoted for convenience
            let mut it = after_pw.split_whitespace(); it.next().unwrap_or("")
        };
        let mut is_admin = false;
        let mut perms: Vec<String> = Vec::new();
        let mut scope_db: Option<String> = None;
        // Remaining tail after password
        let after_pw_tail = if after_pw.starts_with('\'') { &after_pw[pw.len()+2..] } else { &after_pw[pw.len()..] };
        let mut t = after_pw_tail.trim();
        loop {
            if t.is_empty() { break; }
            let t_up = t.to_uppercase();
            if t_up.starts_with("ADMIN") {
                is_admin = true; t = t[5..].trim_start(); continue;
            }
            if t_up.starts_with("PERMISSIONS ") {
                let inner = &t[12..].trim();
                if inner.starts_with('(') {
                    if let Some(end) = inner.find(')') {
                        let list = &inner[1..end];
                        perms = list.split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect();
                        t = inner[end+1..].trim_start();
                        continue;
                    } else { anyhow::bail!("USER ADD: PERMISSIONS missing )"); }
                } else { anyhow::bail!("USER ADD: PERMISSIONS expects (..)"); }
            }
            if t_up.starts_with("GLOBAL") { scope_db = None; t = t[6..].trim_start(); continue; }
            if t_up.starts_with("IN ") || t_up.starts_with("FROM ") || t_up.starts_with("TO ") {
                let db = t[3..].trim(); scope_db = Some(db.to_string()); t = ""; continue;
            }
            break;
        }
        return Ok(Command::UserAdd { username: username.to_string(), password: pw.to_string(), is_admin, perms, scope_db });
    } else if up.starts_with("ALTER ") {
        // USER ALTER <username> [PASSWORD '<pw>'] [ADMIN true|false] [PERMISSIONS (<list>)] [GLOBAL | (IN|FROM|TO) <db>]
        let mut tail = &rest[6..];
        // username up to space or end
        let mut parts = tail.trim().splitn(2, ' ');
        let username = parts.next().unwrap_or("").trim();
        if username.is_empty() { anyhow::bail!("USER ALTER: missing username"); }
        tail = parts.next().unwrap_or("").trim_start();
        let mut new_password: Option<String> = None;
        let mut is_admin: Option<bool> = None;
        let mut perms: Option<Vec<String>> = None;
        let mut scope_db: Option<String> = None;
        let mut t = tail;
        loop {
            if t.is_empty() { break; }
            let t_up = t.to_uppercase();
            if t_up.starts_with("PASSWORD ") {
                let after_pw = &t[9..].trim();
                let pw = if after_pw.starts_with('\'') {
                    if let Some(idx) = after_pw[1..].find('\'') { &after_pw[1..1+idx] } else { anyhow::bail!("USER ALTER: unterminated password"); }
                } else {
                    let mut it = after_pw.split_whitespace(); it.next().unwrap_or("")
                };
                new_password = Some(pw.to_string());
                t = if after_pw.starts_with('\'') { &after_pw[pw.len()+2..] } else { &after_pw[pw.len()..] };
                t = t.trim_start();
                continue;
            }
            if t_up.starts_with("ADMIN ") {
                let val = t[6..].trim();
                let (word, rest): (&str, &str) = if let Some(i) = val.find(' ') { (&val[..i], val[i+1..].trim()) } else { (val, "") };
                let b = match word.to_uppercase().as_str() { "TRUE" | "T" | "YES" | "Y" | "1" => true, "FALSE" | "F" | "NO" | "N" | "0" => false, _ => anyhow::bail!("USER ALTER: ADMIN expects true|false") };
                is_admin = Some(b);
                t = rest.trim_start();
                continue;
            }
            if t_up.starts_with("PERMISSIONS ") {
                let inner = &t[12..].trim();
                if inner.starts_with('(') {
                    if let Some(end) = inner.find(')') {
                        let list = &inner[1..end];
                        let p: Vec<String> = list.split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect();
                        perms = Some(p);
                        t = inner[end+1..].trim_start();
                        continue;
                    } else { anyhow::bail!("USER ALTER: PERMISSIONS missing )"); }
                } else { anyhow::bail!("USER ALTER: PERMISSIONS expects (..)"); }
            }
            if t_up.starts_with("GLOBAL") { scope_db = None; t = t[6..].trim_start(); continue; }
            if t_up.starts_with("IN ") || t_up.starts_with("FROM ") || t_up.starts_with("TO ") {
                let db = t[3..].trim(); scope_db = Some(db.to_string()); t = ""; continue;
            }
            break;
        }
        return Ok(Command::UserAlter { username: username.to_string(), new_password, is_admin, perms, scope_db });
    } else if up.starts_with("DELETE ") {
        let tail = &rest[7..].trim();
        let mut scope_db: Option<String> = None;
        let up_tail = tail.to_uppercase();
        let username_str: String;
        if let Some(i) = up_tail.find(" IN ") {
            username_str = tail[..i].trim().to_string();
            scope_db = Some(tail[i+4..].trim().to_string());
        } else if let Some(i) = up_tail.find(" FROM ") {
            username_str = tail[..i].trim().to_string();
            scope_db = Some(tail[i+6..].trim().to_string());
        } else if let Some(i) = up_tail.find(" TO ") {
            username_str = tail[..i].trim().to_string();
            scope_db = Some(tail[i+4..].trim().to_string());
        } else if up_tail.ends_with(" GLOBAL") {
            username_str = tail[..tail.len()-7].trim().to_string();
            scope_db = None;
        } else {
            username_str = tail.trim().to_string();
        }
        if username_str.is_empty() { anyhow::bail!("USER DELETE: missing username"); }
        return Ok(Command::UserDelete { username: username_str, scope_db });
    }
    anyhow::bail!("Invalid USER syntax")
}

fn parse_schema(s: &str) -> Result<Command> {
    // SCHEMA SHOW <db> | SCHEMA SHOW FROM <db>
    // SCHEMA ADD <name Type>[, <name Type> ...] (FROM|IN|TO) <db>
    let rest = s[6..].trim();
    let rest_up = rest.to_uppercase();
    if rest_up.starts_with("SHOW") {
        let after = rest[4..].trim();
        let mut db = after;
        if after.to_uppercase().starts_with("FROM ") || after.to_uppercase().starts_with("IN ") || after.to_uppercase().starts_with("TO ") {
            db = &after[5..];
        }
        let database = db.trim().to_string();
        if database.is_empty() { anyhow::bail!("Invalid SCHEMA SHOW: missing schema name"); }
        return Ok(Command::SchemaShow { database });
    } else if rest_up.starts_with("ADD ") {
        let after = &rest[4..];
        // Find position of FROM/IN/TO to split entries and database
        let up = after.to_uppercase();
        let mut split_pos: Option<(usize, usize)> = None; // (index, len)
        for kw in [" FROM ", " IN ", " TO "] {
            if let Some(i) = up.find(kw) { split_pos = Some((i, kw.len())); break; }
        }
        let (mut entries_part, db_part) = if let Some((i, l)) = split_pos { (&after[..i], &after[i+l..]) } else { anyhow::bail!("Invalid SCHEMA ADD: missing database (use FROM/IN/TO <db>)"); };
        let database = db_part.trim().to_string();
        if database.is_empty() { anyhow::bail!("Invalid SCHEMA ADD: missing database"); }
        // Extract optional PRIMARY KEY and PARTITION BY clauses from entries_part
        let mut primary_key: Option<Vec<String>> = None;
        let mut partitions: Option<Vec<String>> = None;
        let up_entries = entries_part.to_uppercase();
        // helper to parse list inside parentheses
        let parse_list = |s: &str| -> Vec<String> { s.split(',').map(|t| t.trim().to_string()).filter(|x| !x.is_empty()).collect() };
        // Find positions
        let mut cut_indices: Vec<(usize, usize)> = Vec::new();
        if let Some(i) = up_entries.find("PRIMARY KEY") {
            // find following '(' and ')'
            if let Some(p1) = entries_part[i..].find('(') { if let Some(p2) = entries_part[i+p1+1..].find(')') {
                let start = i + p1 + 1; let end = i + p1 + 1 + p2; let list = &entries_part[start..end];
                let cols = parse_list(list);
                if !cols.is_empty() { primary_key = Some(cols); }
                cut_indices.push((i, end+1));
            }}
        }
        let up_entries2 = entries_part.to_uppercase();
        if let Some(i) = up_entries2.find("PARTITION BY") {
            if let Some(p1) = entries_part[i..].find('(') { if let Some(p2) = entries_part[i+p1+1..].find(')') {
                let start = i + p1 + 1; let end = i + p1 + 1 + p2; let list = &entries_part[start..end];
                let cols = parse_list(list);
                if !cols.is_empty() { partitions = Some(cols); }
                cut_indices.push((i, end+1));
            }}
        }
        // Remove clauses from entries_part by slicing before the earliest clause
        if !cut_indices.is_empty() {
            cut_indices.sort_by_key(|x| x.0);
            let (start, _) = cut_indices[0];
            entries_part = &entries_part[..start];
        }
        // Parse entries: comma-separated pairs of name and type word
        let mut entries: Vec<(String, String)> = Vec::new();
        for chunk in entries_part.split(',') {
            let t = chunk.trim(); if t.is_empty() { continue; }
            let mut parts = t.split_whitespace();
            let name = parts.next().ok_or_else(|| anyhow::anyhow!("Invalid entry: missing name"))?.to_string();
            let ty = parts.next().ok_or_else(|| anyhow::anyhow!("Invalid entry: missing type for {}", name))?.to_string();
            entries.push((name, ty));
        }
        if entries.is_empty() && primary_key.is_none() && partitions.is_none() { anyhow::bail!("SCHEMA ADD: no entries or metadata provided"); }
        return Ok(Command::SchemaAdd { database, entries, primary_key, partitions });
    }
    anyhow::bail!("Invalid SCHEMA syntax")
}

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

fn parse_update(s: &str) -> Result<Command> {
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

fn parse_select(s: &str) -> Result<Query> {
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
    // SELECT ... [FROM db [BY window | GROUP BY <cols>] [WHERE ...] [ROLLING ...] [ORDER BY ...] [HAVING ...]]
    // Robustly detect the FROM keyword even across newlines/tabs and with varied casing.
    fn find_keyword_ci(s: &str, kw: &str) -> Option<usize> {
        let kw_up = kw.to_uppercase();
        let klen = kw_up.len();
        let su = s.to_uppercase();
        let bytes = su.as_bytes();
        let sbytes = s.as_bytes();
        let b_kw = kw_up.as_bytes();
        let mut i = 0usize;
        while i + klen <= bytes.len() {
            if &bytes[i..i+klen] == b_kw {
                // Word boundary check: prev is start or non-alphanumeric/_; next is end or non-alphanumeric/_
                let prev_ok = if i == 0 { true } else {
                    let pc = sbytes[i-1] as char; !(pc.is_alphanumeric() || pc == '_')
                };
                let next_ok = if i + klen >= bytes.len() { true } else {
                    let nc = sbytes[i+klen] as char; !(nc.is_alphanumeric() || nc == '_')
                };
                if prev_ok && next_ok { return Some(i); }
            }
            // advance by one byte (safe for ASCII keywords)
            i += 1;
        }
        None
    }

    let from_pos = find_keyword_ci(query_sql, "FROM");
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
            for raw in inside.split(',') {
                let p = raw.trim();
                if p.is_empty() { continue; }
                let mut toks = p.split_whitespace();
                if let Some(name) = toks.next() {
                    let mut asc = true;
                    if let Some(dir) = toks.next() {
                        if dir.eq_ignore_ascii_case("DESC") { asc = false; }
                        else if dir.eq_ignore_ascii_case("ASC") { asc = true; }
                        else { /* unknown token ignored */ }
                    }
                    let normalized_name = crate::ident::normalize_identifier(name.trim());
                    list.push((normalized_name, asc));
                }
            }
            if list.is_empty() { anyhow::bail!("Invalid ORDER BY: empty list"); }
            if order_by.is_some() { anyhow::bail!("Duplicate ORDER BY clause"); }
            order_by = Some(list);
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

    Ok(Query { select, by_window_ms, by_slices, group_by_cols, group_by_notnull_cols, where_clause, having_clause, rolling_window_ms, order_by, order_by_hint, limit, into_table, into_mode, base_table, joins, with_ctes, original_sql: s.trim().to_string() })
}

fn split_once_any<'a>(s: &'a str, seps: &[&str]) -> (&'a str, Option<&'a str>) {
    for sep in seps {
        if let Some(i) = s.to_uppercase().find(&sep.to_uppercase()) {
            let (a, b) = s.split_at(i);
            return (a, Some(&b[sep.len()..]));
        }
    }
    (s, None)
}

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

fn parse_where_tokens(tokens: &[String], original: &str) -> Result<WhereExpr> {
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

fn find_token_ci(tokens: &[String], needle: &str) -> Option<usize> {
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

fn parse_iso8601_to_ms(tok: &str) -> Option<i64> {
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

// Parse a PostgreSQL type name from the given source substring, returning the parsed SqlType and
// the number of bytes consumed. Supports multi-word names (e.g., "double precision"), schema-qualified
// names (e.g., pg_catalog.regclass), and optional parameters: varchar(10), numeric(10,2).
fn parse_pg_type_keyword(s: &str) -> Option<(&str, usize)> {
    // Consume identifier tokens and spaces, stopping before '(' or other delimiter
    let bytes = s.as_bytes();
    let mut i = 0usize;
    // Collect up to two words for multi-word types like "double precision" or "character varying"
    let mut words: Vec<String> = Vec::new();
    while i < bytes.len() {
        // skip spaces
        while i < bytes.len() && bytes[i].is_ascii_whitespace() { i += 1; }
        if i >= bytes.len() { break; }
        // if next is '(', stop - parameters handled separately
        let ch = bytes[i] as char;
        if ch == '(' { break; }
        // read an identifier token (letters, digits, underscore, dot)
        let start = i;
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' { i += 1; } else { break; }
        }
        if start == i { break; }
        words.push(s[start..i].to_string());
        // lookahead for another word (for multi-word type names)
        let mut j = i; while j < bytes.len() && bytes[j].is_ascii_whitespace() { j += 1; }
        if j < bytes.len() {
            let c2 = bytes[j] as char;
            if c2.is_ascii_alphabetic() { i = j; continue; }
        }
        // otherwise stop
        break;
    }
    if words.is_empty() { return None; }
    let consumed = i;
    let name = words.join(" ");
    Some((Box::leak(name.into_boxed_str()), consumed))
}

fn parse_type_name(s: &str) -> Option<(SqlType, usize)> {
    let s_trim = s;
    let (kw, mut consumed_kw) = parse_pg_type_keyword(s_trim)?;
    let kw_lc = kw.to_ascii_lowercase();
    // Default: no params
    let mut rest = &s_trim[consumed_kw..];
    // Parse optional ( ... ) parameters
    let mut params: Option<Vec<i32>> = None;
    {
        let mut k = 0usize; while k < rest.len() && rest.as_bytes()[k].is_ascii_whitespace() { k += 1; }
        if k < rest.len() && rest.as_bytes()[k] as char == '(' {
            // find closing ')'
            let mut j = k + 1; let bytes = rest.as_bytes(); let mut buf = String::new(); let mut parts = Vec::new();
            while j < rest.len() {
                let ch = bytes[j] as char; j += 1;
                if ch == ')' { break; }
                buf.push(ch);
            }
            if !buf.is_empty() {
                for p in buf.split(',') { if let Ok(v) = p.trim().parse::<i32>() { parts.push(v); } }
                if !parts.is_empty() { params = Some(parts); }
            }
            consumed_kw += j; // include ')'
        }
    }

    let ty = match kw_lc.as_str() {
        "bool" | "boolean" => SqlType::Boolean,
        "smallint" | "int2" => SqlType::SmallInt,
        "int" | "integer" | "int4" => SqlType::Integer,
        "bigint" | "int8" => SqlType::BigInt,
        "real" | "float4" => SqlType::Real,
        "double precision" | "float8" => SqlType::Double,
        "text" => SqlType::Text,
        // character types
        "varchar" | "character varying" => SqlType::Varchar(params.as_ref().and_then(|v| v.get(0).cloned())),
        "character" | "char" | "bpchar" => SqlType::Char(params.as_ref().and_then(|v| v.get(0).cloned())),
        // binary and JSON-like
        "bytea" => SqlType::Bytea,
        "uuid" => SqlType::Uuid,
        "json" => SqlType::Json,
        "jsonb" => SqlType::Jsonb,
        "varchar" | "character varying" => SqlType::Varchar(params.as_ref().and_then(|v| v.get(0).cloned())),
        "date" => SqlType::Date,
        "timestamp" | "timestamp without time zone" => SqlType::Timestamp,
        "timestamptz" | "timestamp with time zone" => SqlType::TimestampTz,
        // time of day (without/with time zone)
        "time" | "time without time zone" => SqlType::Time,
        "timetz" | "time with time zone" => SqlType::TimeTz,
        // interval duration
        "interval" => SqlType::Interval,
        "numeric" | "decimal" => {
            let ps = params.as_ref().and_then(|v| {
                if v.len() == 2 { Some((v[0], v[1])) } else if v.len() == 1 { Some((v[0], 0)) } else { None }
            });
            SqlType::Numeric(ps)
        },
        // Schema-qualified regclass (e.g., pg_catalog.regclass) or bare regclass
        x if x.ends_with(".regclass") || x == "regclass" => SqlType::Regclass,
        // Schema-qualified regtype (e.g., pg_catalog.regtype) or bare regtype
        x if x.ends_with(".regtype") || x == "regtype" => SqlType::Regtype,
        _ => return None,
    };
    Some((ty, consumed_kw))
}

fn parse_arith_expr(tokens: &[String]) -> Result<ArithExpr> {
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
        // Helper: split by commas at top level
        let mut args: Vec<String> = Vec::new();
        let mut buf = String::new();
        let mut d = 0i32;
        let mut k = 0usize;
        while k < inside.len() {
            let ch = inside[k..].chars().next().unwrap();
            if ch == '(' { d += 1; buf.push(ch); }
            else if ch == ')' { d -= 1; buf.push(ch); }
            else if ch == ',' && d == 0 { args.push(buf.trim().to_string()); buf.clear(); }
            else { buf.push(ch); }
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
                    let mut name = &src[i..j];
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
                            // split on commas at top-level depth
                            let mut args: Vec<String> = Vec::new();
                            let mut buf = String::new();
                            let mut d = 0i32;
                            let mut p = 0usize;
                            while p < inside.len() {
                                let ch = inside[p..].chars().next().unwrap();
                                if ch == '(' { d += 1; buf.push(ch); }
                                else if ch == ')' { d -= 1; buf.push(ch); }
                                else if ch == ',' && d == 0 { args.push(buf.trim().to_string()); buf.clear(); }
                                else { buf.push(ch); }
                                p += ch.len_utf8();
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

fn prec(op: &ArithOp) -> i32 { match op { ArithOp::Add|ArithOp::Sub => 1, ArithOp::Mul|ArithOp::Div => 2 } }


fn parse_select_list(s: &str) -> Result<Vec<SelectItem>> {
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


// --- SLICE parser ---
fn parse_slice(input: &str) -> Result<SlicePlan> {
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

fn parse_slice_clause(s: &str) -> Result<(SliceClause, usize)> {
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

fn parse_slice_source(s: &str) -> Result<(SliceSource, usize)> {
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

fn find_next_keyword(s: &str, kws: &[&str]) -> Option<usize> {
    let up = s.to_uppercase();
    let mut best: Option<usize> = None;
    for kw in kws {
        if let Some(i) = up.find(&kw.to_uppercase()) {
            best = Some(best.map(|b| b.min(i)).unwrap_or(i));
        }
    }
    best
}


fn extract_slice_block(s: &str) -> Result<(&str, usize)> {
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

#[cfg(test)]
mod tests;

fn parse_show(s: &str) -> Result<Command> {
    let up = s.trim().to_uppercase();
    if up == "SHOW TRANSACTION ISOLATION LEVEL" { return Ok(Command::ShowTransactionIsolation); }
    if up == "SHOW STANDARD_CONFORMING_STRINGS" { return Ok(Command::ShowStandardConformingStrings); }
    if up.starts_with("SHOW SERVER_VERSION") { return Ok(Command::ShowServerVersion); }
    if up == "SHOW CLIENT_ENCODING" { return Ok(Command::ShowClientEncoding); }
    if up == "SHOW SERVER_ENCODING" { return Ok(Command::ShowServerEncoding); }
    if up == "SHOW DATESTYLE" { return Ok(Command::ShowDateStyle); }
    if up == "SHOW INTEGER_DATETIMES" { return Ok(Command::ShowIntegerDateTimes); }
    if up == "SHOW TIME ZONE" || up == "SHOW TIMEZONE" { return Ok(Command::ShowTimeZone); }
    if up == "SHOW SEARCH_PATH" { return Ok(Command::ShowSearchPath); }
    if up == "SHOW DEFAULT_TRANSACTION_ISOLATION" { return Ok(Command::ShowDefaultTransactionIsolation); }
    if up == "SHOW TRANSACTION_READ_ONLY" { return Ok(Command::ShowTransactionReadOnly); }
    if up == "SHOW APPLICATION_NAME" { return Ok(Command::ShowApplicationName); }
    if up == "SHOW EXTRA_FLOAT_DIGITS" { return Ok(Command::ShowExtraFloatDigits); }
    if up == "SHOW ALL" { return Ok(Command::ShowAll); }
    if up.starts_with("SHOW SCHEMAS") || up.starts_with("SHOW SCHEMA") { return Ok(Command::ShowSchemas); }
    if up == "SHOW TABLES" { return Ok(Command::ShowTables); }
    if up == "SHOW OBJECTS" { return Ok(Command::ShowObjects); }
    if up == "SHOW SCRIPTS" { return Ok(Command::ShowScripts); }
    if up.starts_with("SHOW VECTOR INDEXES") { return Ok(Command::ShowVectorIndexes); }
    if up.starts_with("SHOW VECTOR INDEX ") {
        let name = s.trim()["SHOW VECTOR INDEX ".len()..].trim();
        if name.is_empty() { anyhow::bail!("SHOW VECTOR INDEX: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::ShowVectorIndex { name: normalized_name });
    }
    if up.starts_with("SHOW GRAPH ") {
        let tail = s.trim()["SHOW GRAPH ".len()..].trim();
        if tail.eq_ignore_ascii_case("S") || tail.eq_ignore_ascii_case("S;") || tail.eq_ignore_ascii_case("S; ") { /* unlikely */ }
        // Accept SHOW GRAPHS and SHOW GRAPH <name>
        if tail.eq_ignore_ascii_case("S") || tail.eq_ignore_ascii_case("GRAPHS") { return Ok(Command::ShowGraphs); }
        if tail.eq_ignore_ascii_case("RAPHS") { return Ok(Command::ShowGraphs); }
        if tail.eq_ignore_ascii_case("GRAPHS;") { return Ok(Command::ShowGraphs); }
        let normalized_name = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowGraph { name: normalized_name });
    }
    if up.starts_with("SHOW GRAPHS") { return Ok(Command::ShowGraphs); }
    if up.starts_with("SHOW VIEW ") {
        let name = s.trim()["SHOW VIEW ".len()..].trim();
        if name.is_empty() { anyhow::bail!("SHOW VIEW: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::ShowView { name: normalized_name });
    }
    anyhow::bail!("Unsupported SHOW command")
}

fn parse_use(s: &str) -> Result<Command> {
    let rest = s[3..].trim(); // after USE
    let up = rest.to_uppercase();
    if up.starts_with("DATABASE ") {
        let name = rest[9..].trim();
        if name.is_empty() { anyhow::bail!("USE DATABASE: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::UseDatabase { name: normalized_name });
    }
    if up.starts_with("SCHEMA ") {
        let name = rest[7..].trim();
        if name.is_empty() { anyhow::bail!("USE SCHEMA: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::UseSchema { name: normalized_name });
    }
    anyhow::bail!("Unsupported USE command")
}

fn parse_set(s: &str) -> Result<Command> {
    // SET variable TO value | SET variable = value
    let rest = s[3..].trim(); // after SET
    // Split by TO or = (case-insensitive for TO)
    let up = rest.to_uppercase();
    let (variable, value) = if let Some(pos) = up.find(" TO ") {
        let var = rest[..pos].trim();
        let val = rest[pos + 4..].trim();
        (var, val)
    } else if let Some(pos) = rest.find('=') {
        let var = rest[..pos].trim();
        let val = rest[pos + 1..].trim();
        (var, val)
    } else {
        anyhow::bail!("Invalid SET syntax. Use: SET variable TO value or SET variable = value");
    };
    
    if variable.is_empty() { anyhow::bail!("SET: missing variable name"); }
    if value.is_empty() { anyhow::bail!("SET: missing value"); }
    
    // Strip quotes from value if present
    let value_clean = if (value.starts_with('\'') && value.ends_with('\'')) || (value.starts_with('"') && value.ends_with('"')) {
        if value.len() >= 2 { &value[1..value.len()-1] } else { value }
    } else {
        value
    };
    
    Ok(Command::Set { 
        variable: variable.to_string(), 
        value: value_clean.to_string() 
    })
}

fn parse_insert(s: &str) -> Result<Command> {
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

fn extract_paren_block(s: &str) -> Option<(&str, usize)> {
    let t = s;
    if t.is_empty() || t.as_bytes()[0] != b'(' { return None; }
    let mut depth = 0i32; let mut i0 = 0usize; let mut started = false;
    for (idx, ch) in t.char_indices() {
        if ch == '(' { depth += 1; if !started { started = true; i0 = idx + ch.len_utf8(); } }
        else if ch == ')' { depth -= 1; if depth == 0 { let inner = &t[i0..idx]; return Some((inner, idx + 1)); } }
    }
    None
}

fn split_csv_ignoring_quotes(s: &str) -> Vec<String> {
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

fn parse_manual_cell(tok: &str) -> ManualLabel {
    let t = tok.trim();
    if t.is_empty() { return ManualLabel{ name: None, value: None }; }
    if let Some(pos) = t.find(":=") {
        let name = t[..pos].trim().trim_matches(['"','\'']).to_string();
        let val_raw = t[pos+2..].trim();
        let v = if val_raw.eq_ignore_ascii_case("NULL") { None } else if (val_raw.starts_with('\'') && val_raw.ends_with('\'')) || (val_raw.starts_with('"') && val_raw.ends_with('"')) { Some(val_raw[1..val_raw.len()-1].to_string()) } else { Some(val_raw.to_string()) };
        ManualLabel{ name: if name.is_empty() { None } else { Some(name) }, value: v }
    } else {
        let v = if t.eq_ignore_ascii_case("NULL") { None } else if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) { Some(t[1..t.len()-1].to_string()) } else { Some(t.to_string()) };
        ManualLabel{ name: None, value: v }
    }
}

fn parse_date_token_to_ms(tok: &str) -> Result<i64> {
    let t = tok.trim();
    if let Some(ms) = parse_iso8601_to_ms(t) { return Ok(ms); }
    // numeric milliseconds
    if let Ok(n) = t.parse::<i64>() { return Ok(n); }
    anyhow::bail!("Invalid date token in manual SLICE row: {}", tok)
}

fn parse_manual_row(s: &str) -> Result<ManualRow> {
    let parts = split_csv_ignoring_quotes(s);
    if parts.len() < 2 { anyhow::bail!("Manual SLICE row must start with start and end dates"); }
    let start = parse_date_token_to_ms(&parts[0])?;
    let end = parse_date_token_to_ms(&parts[1])?;
    let mut labels: Vec<ManualLabel> = Vec::new();
    for p in parts.into_iter().skip(2) { if p.is_empty() { continue; } labels.push(parse_manual_cell(&p)); }
    Ok(ManualRow{ start, end, labels })
}

fn parse_manual_rows(s: &str) -> Result<Option<(SliceSource, usize)>> {
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


// --- KV STORE/KEY parsing helpers ---
fn parse_store_addr(addr: &str) -> Result<(String, String)> {
    // Expect <database>.store.<store>
    let parts: Vec<&str> = addr.split('.').collect();
    if parts.len() != 3 { anyhow::bail!(format!("Invalid store address '{}'. Expected <database>.store.<store>", addr)); }
    if parts[1].to_lowercase() != "store" { anyhow::bail!("Invalid store address: missing literal 'store' segment"); }
    let db = parts[0].trim();
    let store = parts[2].trim();
    if db.is_empty() || store.is_empty() { anyhow::bail!("Invalid store address: empty database or store name"); }
    Ok((db.to_string(), store.to_string()))
}

fn parse_key_in_clause(rest: &str) -> Result<(String, String, String)> {
    // Expect: KEY <key> IN <database>.store.<store>
    let up = rest.to_uppercase();
    if !up.starts_with("KEY ") { anyhow::bail!("Invalid syntax: expected KEY <name> IN <database>.store.<store>"); }
    let after_key = &rest[4..];
    let parts: Vec<&str> = after_key.splitn(2, " IN ").collect();
    if parts.len() != 2 { anyhow::bail!("Invalid KEY syntax: expected 'IN <database>.store.<store>'"); }
    let key = parts[0].trim();
    let store_addr = parts[1].trim();
    if key.is_empty() { anyhow::bail!("Invalid KEY syntax: missing key name"); }
    let (db, store) = parse_store_addr(store_addr)?;
    Ok((db, store, key.to_string()))
}

fn parse_read(s: &str) -> Result<Command> {
    // READ KEY <key> IN <database>.store.<store>
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("KEY ") {
        let (db, store, key) = parse_key_in_clause(rest)?;
        return Ok(Command::ReadKey { database: db, store, key });
    }
    anyhow::bail!("Invalid READ syntax")
}

fn parse_list(s: &str) -> Result<Command> {
    // LIST STORES <db>
    // LIST KEYS IN <database>.store.<store>
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("STORES ") {
        let db = rest[7..].trim();
        if db.is_empty() { anyhow::bail!("Invalid LIST STORES: missing database name"); }
        return Ok(Command::ListStores { database: db.to_string() });
    }
    if up == "STORES" {
        anyhow::bail!("Invalid LIST STORES: missing database name");
    }
    if up.starts_with("KEYS ") {
        let after = &rest[5..];
        let up2 = after.to_uppercase();
        if let Some(i) = up2.find(" IN ") {
            let addr = after[i+4..].trim();
            let (db, store) = parse_store_addr(addr)?;
            return Ok(Command::ListKeys { database: db, store });
        } else {
            anyhow::bail!("Invalid LIST KEYS syntax: expected 'LIST KEYS IN <database>.store.<store>'");
        }
    }
    anyhow::bail!("Invalid LIST syntax")
}

fn parse_describe(s: &str) -> Result<Command> {
    // DESCRIBE KEY <key> IN <database>.store.<store>
    // or: DESCRIBE <object>
    let rest = s[9..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("KEY ") {
        let (db, store, key) = parse_key_in_clause(rest)?;
        return Ok(Command::DescribeKey { database: db, store, key });
    }
    // Fallback: treat the remainder as an object identifier (table or view)
    if rest.is_empty() { anyhow::bail!("Invalid DESCRIBE syntax: missing object name"); }
    // Keep the name as provided; qualification is applied at execution time
    Ok(Command::DescribeObject { name: rest.to_string() })
}

fn parse_write(s: &str) -> Result<Command> {
    // WRITE KEY <key> IN <database>.store.<store> = <value_or_address> [TTL <duration>] [RESET ON ACCESS|NO RESET]
    let rest = s[5..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("KEY ") {
        // split around '=' first
        let eq_pos = rest.find('=');
        if eq_pos.is_none() { anyhow::bail!("Invalid WRITE KEY: missing '=' assignment"); }
        let eq_pos = eq_pos.unwrap();
        let left = rest[..eq_pos].trim();
        let right_all = rest[eq_pos+1..].trim();
        let (db, store, key) = parse_key_in_clause(left)?;
        // Extract optional TTL/RESET flags from right-hand side tail
        // We'll split value and options by looking for ' TTL ' or ' RESET '
        let mut ttl_ms: Option<i64> = None;
        let mut reset_on_access: Option<bool> = None;
        let mut value_str = right_all.to_string();
        // Normalize spaces for matching
        let up_right = right_all.to_uppercase();
        let mut opt_start = up_right.len();
        if let Some(i) = up_right.find(" TTL ") { opt_start = opt_start.min(i); }
        if let Some(i) = up_right.find(" RESET ") { opt_start = opt_start.min(i); }
        if opt_start < up_right.len() {
            value_str = right_all[..opt_start].trim().to_string();
            let opts = right_all[opt_start..].trim();
            let up_opts = opts.to_uppercase();
            // TTL
            if let Some(i) = up_opts.find("TTL ") {
                let after = &opts[i+4..];
                let token = after.split_whitespace().next().unwrap_or("");
                if token.is_empty() { anyhow::bail!("Invalid TTL: missing duration (e.g., 10s, 5m)"); }
                let ms = parse_window(token)?;
                ttl_ms = Some(ms);
            }
            // RESET flags
            if up_opts.contains("RESET ON ACCESS") { reset_on_access = Some(true); }
            else if up_opts.contains("NO RESET") { reset_on_access = Some(false); }
        } else {
            value_str = right_all.to_string();
        }
        return Ok(Command::WriteKey { database: db, store, key, value: value_str, ttl_ms, reset_on_access });
    }
    anyhow::bail!("Invalid WRITE syntax")
}
