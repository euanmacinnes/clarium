use polars::prelude::*;
// Needed for Expr::map closure signature and output typing
use polars::prelude::Column;
use regex::Regex;


use crate::server::query::query_common::{ArithExpr, ArithOp, ArithTerm, CompOp, WhereExpr, SqlType, DateFunc, DatePart, StrSliceBound};


#[inline]
fn fnv1a32(data: &[u8]) -> u32 {
    // 32-bit FNV-1a hash, commonly used for lightweight hashing
    const FNV_OFFSET: u32 = 0x811C9DC5;
    const FNV_PRIME: u32 = 0x01000193;
    let mut hash = FNV_OFFSET;
    for b in data {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // Avoid zero OID (Postgres reserves 0 as invalid), map 0 to 1
    if hash == 0 { 1 } else { hash }
}

#[inline]
pub fn regclass_oid_with_defaults(name: &str, db: Option<&str>, schema: Option<&str>) -> i32 {
    // Known system catalogs per PostgreSQL
    let mut ident = name.trim().to_string();
    if ident.starts_with("pg_catalog.") { ident = ident[11..].to_string(); }
    let ident_lc = ident.to_ascii_lowercase();
    match ident_lc.as_str() {
        "pg_class" => return 1259,
        "pg_type" => return 1247,
        "pg_namespace" => return 2615,
        _ => {}
    }
    // Qualify using provided defaults if needed
    let effective = if ident_lc.contains('/') || ident_lc.contains(".store.") {
        ident_lc
    } else {
        let parts: Vec<&str> = ident_lc.split('.').collect();
        match parts.len() {
            0 => String::new(),
            1 => {
                use crate::ident::{DEFAULT_DB, DEFAULT_SCHEMA};
                let d = db.unwrap_or(DEFAULT_DB);
                let s = schema.unwrap_or(DEFAULT_SCHEMA);
                format!("{}/{}/{}", d, s, parts[0])
            }
            2 => {
                use crate::ident::DEFAULT_DB;
                let d = db.unwrap_or(DEFAULT_DB);
                format!("{}/{}/{}", d, parts[0], parts[1])
            }
            _ => format!("{}/{}/{}", parts[0], parts[1], parts[2]),
        }
    };
    let eff = if effective.ends_with(".time") { &effective[..effective.len()-5] } else { effective.as_str() };
    let h = fnv1a32(eff.as_bytes());
    (h as i32).abs().max(1)
}

pub fn build_where_expr(w: &WhereExpr, ctx: &crate::server::data_context::DataContext) -> Expr {
    match w {
        WhereExpr::Comp { left, op, right } => {            
            // Helper: detect boolean-returning UDF call using registry metadata from context
            let is_boolean_udf_call = |a: &ArithExpr| -> bool {
                if let ArithExpr::Call { name, .. } = a {
                    if let Some(reg) = ctx.script_registry.as_ref() {
                        if let Some(meta) = reg.get_meta(name) {
                            return meta.returns.first() == Some(&DataType::Boolean);
                        }
                    }
                }
                false
            };
            // Helper: convert an arithmetic expression that is a bare boolean predicate into a mask
            let to_bool_expr = |a: &ArithExpr| -> Option<Expr> {
                match a {
                    ArithExpr::Predicate(inner) => Some(build_where_expr(inner, ctx)),
                    _ if is_boolean_udf_call(a) => Some(build_arith_expr(a, ctx).eq(lit(true))),
                    _ => None,
                }
            };
            // Helper: constant number extractor (without cloning)
            fn const_number(a: &ArithExpr) -> Option<f64> {
                if let ArithExpr::Term(ArithTerm::Number(n)) = a { Some(*n) } else { None }
            }
            // Special-case: support bare boolean predicates parsed as `<expr> = 1`
            // Trigger when LEFT is an explicit predicate OR a boolean-returning UDF call.
            // Handle LEFT = 1/0 pattern
            if let (CompOp::Eq, Some(n)) = (op, const_number(right)) {
                if let Some(be) = to_bool_expr(left) {
                    return if (n - 1.0).abs() < f64::EPSILON { be } else { be.not() };
                }
            }
            // Handle 1/0 = RIGHT pattern
            if let (CompOp::Eq, Some(n)) = (op, const_number(left)) {
                if let Some(be) = to_bool_expr(right) {
                    return if (n - 1.0).abs() < f64::EPSILON { be } else { be.not() };
                }
            }
            // Handle ordered comparisons between boolean predicate and 0/1
            // Map to equivalent boolean masks to avoid numeric-vs-string coercions.
            if let Some(n) = const_number(right) {
                if let Some(be) = to_bool_expr(left) {                    
                    match op {
                        CompOp::Gt | CompOp::Ge => {
                            // b > 0  or b >= 1  => b == true, else for n<=0 treat as b == true for Ge(1) only
                            if (n - 0.0).abs() < f64::EPSILON { return be.clone(); }
                            if (n - 1.0).abs() < f64::EPSILON { return be; }
                        }
                        CompOp::Lt | CompOp::Le => {
                            // b < 1 or b <= 0 => not b
                            if (n - 1.0).abs() < f64::EPSILON { return be.not(); }
                            if (n - 0.0).abs() < f64::EPSILON { return be.not(); }
                        }
                        _ => {}
                    }
                }
            }
            if let Some(n) = const_number(left) {
                if let Some(be) = to_bool_expr(right) {                    
                    match op {
                        CompOp::Lt | CompOp::Le => {
                            // 0 < b or 0 <= b  (with b in {0,1}) => b == true
                            if (n - 0.0).abs() < f64::EPSILON { return be; }
                        }
                        CompOp::Gt | CompOp::Ge => {
                            // 1 > b or 1 >= b => not b
                            if (n - 1.0).abs() < f64::EPSILON { return be.not(); }
                        }
                        _ => {}
                    }
                }
            }

            let l = build_arith_expr(left, ctx);
            let r = build_arith_expr(right, ctx);
            match op {
                // For ordered comparisons, coerce both sides to Float64 to tolerate numeric strings vs numbers
                CompOp::Gt => l.clone().cast(DataType::Float64).gt(r.clone().cast(DataType::Float64)),
                CompOp::Ge => l.clone().cast(DataType::Float64).gt_eq(r.clone().cast(DataType::Float64)),
                CompOp::Lt => l.clone().cast(DataType::Float64).lt(r.clone().cast(DataType::Float64)),
                CompOp::Le => l.clone().cast(DataType::Float64).lt_eq(r.clone().cast(DataType::Float64)),
                // Equality comparisons: if either side is a numeric literal, coerce both to Float64
                // to tolerate Int64-vs-Float64 comparisons (e.g., from correlated subqueries).
                CompOp::Eq => {
                    let numeric_literal = matches!(right, ArithExpr::Term(ArithTerm::Number(_))) || matches!(left, ArithExpr::Term(ArithTerm::Number(_)));
                    if numeric_literal { 
                        l.clone().cast(DataType::Float64).eq(r.clone().cast(DataType::Float64)) 
                    } else { 
                        // Type coercion: try string-to-int first, then int-to-string
                        // Cast both sides to string to handle mixed string/int comparisons
                        l.clone().cast(DataType::String).eq(r.clone().cast(DataType::String))
                    }
                }
                CompOp::Ne => {
                    let numeric_literal = matches!(right, ArithExpr::Term(ArithTerm::Number(_))) || matches!(left, ArithExpr::Term(ArithTerm::Number(_)));
                    if numeric_literal { 
                        l.clone().cast(DataType::Float64).neq(r.clone().cast(DataType::Float64)) 
                    } else { 
                        // Type coercion: try string-to-int first, then int-to-string
                        // Cast both sides to string to handle mixed string/int comparisons
                        l.clone().cast(DataType::String).neq(r.clone().cast(DataType::String))
                    }
                }
                CompOp::Like | CompOp::NotLike => {
                    // Support LIKE when RHS is a literal string pattern by converting to a regex and applying via map
                    if let ArithExpr::Term(ArithTerm::Str(pat)) = right {
                        let regex_text = sql_like_to_regex(pat);
                        // Compile regex safely; if invalid, produce a false mask rather than panic
                        let re = match Regex::new(&regex_text) {
                            Ok(r) => r,
                            Err(_) => { return lit(false); }
                        };
                        let pred = l.clone().cast(DataType::String).map(
                            move |col: Column| {
                                let s = col.as_materialized_series();
                                let ca = s.str()?;
                                let vals: Vec<Option<bool>> = ca
                                    .into_iter()
                                    .map(|opt_val| opt_val.map(|v| re.is_match(v)))
                                    .collect();
                                let s = Series::new("_like_pred".into(), vals);
                                Ok(s.into_column())
                            },
                            |_schema, _field| {
                                Ok(Field::new("_like_pred".into(), DataType::Boolean))
                            }
                        );
                        if matches!(op, CompOp::NotLike) { pred.not() } else { pred }
                    } else {
                        // Fallback for non-literal RHS: unsupported in this engine path currently; return false mask
                        // This case should be rare because pgwire parameter substitution will turn RHS into a literal pattern.
                        lit(false)
                    }
                }
            }
        }
        WhereExpr::And(a, b) => build_where_expr(a, ctx).and(build_where_expr(b, ctx)),
        WhereExpr::Or(a, b) => build_where_expr(a, ctx).or(build_where_expr(b, ctx)),
        WhereExpr::IsNull { expr, negated } => {
            let e = build_arith_expr(expr, ctx);
            if *negated { e.is_not_null() } else { e.is_null() }
        },
        WhereExpr::Exists { negated, subquery } => {
            // EXISTS (subquery): evaluate subquery and return true if any rows exist
            // For now, return a placeholder that evaluates to true (EXISTS support requires query execution context)
            // This will need full implementation with access to storage to execute subquery
            let _ = (negated, subquery);
            lit(true)
        },
        WhereExpr::All { left, op, subquery, negated } => {
            // value op ALL (subquery): true if value op holds for all rows in subquery
            // Placeholder implementation - requires subquery execution
            let _ = (left, op, subquery, negated);
            lit(true)
        },
        WhereExpr::Any { left, op, subquery, negated } => {
            // value op ANY (subquery): true if value op holds for any row in subquery
            // Placeholder implementation - requires subquery execution
            let _ = (left, op, subquery, negated);
            lit(true)
        },
    }
}

// Helper to pattern-match right side number without cloning left
fn left_maybe_number(right: &ArithExpr) -> &ArithExpr {
    right
}

pub fn build_arith_expr(a: &ArithExpr, ctx: &crate::server::data_context::DataContext) -> Expr {
    match a {
        ArithExpr::Term(ArithTerm::Number(n)) => lit(*n),
        ArithExpr::Term(ArithTerm::Str(s)) => lit(s.clone()),
        ArithExpr::Term(ArithTerm::Null) => lit(polars::prelude::Null {}),
        ArithExpr::Term(ArithTerm::Col { name, previous }) => {
            if *previous { col(name).shift(lit(1)) } else { col(name) }
        }
        ArithExpr::Cast { expr, ty } => {
            let inner = build_arith_expr(expr, ctx);
            match ty {
                SqlType::Boolean => inner.cast(DataType::Boolean),
                SqlType::SmallInt | SqlType::Integer | SqlType::BigInt => inner.cast(DataType::Int64),
                SqlType::Real | SqlType::Double | SqlType::Numeric(_) => inner.cast(DataType::Float64),
                SqlType::Text | SqlType::Varchar(_) | SqlType::Char(_) | SqlType::Uuid | SqlType::Json | SqlType::Jsonb | SqlType::TimeTz => {
                    // Cast-to-text semantics: format numbers without trailing .0 when integral,
                    // otherwise preserve normal stringification. This mirrors CONCAT formatting.
                    let ef = inner.clone().cast(DataType::Float64);
                    let is_num = ef.clone().is_not_null();
                    let int_part = ef.clone().cast(DataType::Int64).cast(DataType::Float64);
                    let frac = ef.clone() - int_part.clone();
                    let is_int = frac.eq(lit(0.0));
                    let num_str = when(is_int)
                        .then(int_part.cast(DataType::Int64).cast(DataType::String))
                        .otherwise(ef.cast(DataType::String));
                    when(is_num).then(num_str).otherwise(inner.cast(DataType::String))
                },
                SqlType::Interval => {
                    // Convert to Duration (microseconds). Accept ISO8601-like strings such as
                    // "P1DT2.000000S" and simple numeric seconds.
                    // Strategy: map input to i64 micros per row, then cast to Duration(us).
                    let e_in = inner.clone().alias("__in");
                    let as_struct = polars::lazy::dsl::as_struct(vec![e_in]);
                    let micros_expr = as_struct.map(
                        move |col: polars::prelude::Column| {
                            use polars::prelude::*;
                            // Parser: supports leading '-' sign, 'P{d}D' day part (optional),
                            // 'T{sec}[.frac]S' seconds part (optional). Ignores months/years.
                            fn parse_iso_micros(txt: &str) -> Option<i64> {
                                let s = txt.trim();
                                if s.is_empty() { return None; }
                                let mut neg = false;
                                let mut p = s;
                                if let Some(rest) = p.strip_prefix('-') { neg = true; p = rest; }
                                if !p.starts_with('P') { // try plain seconds
                                    if let Ok(sec) = p.parse::<f64>() { return Some(((if neg { -sec } else { sec }) * 1_000_000f64).round() as i64); }
                                    return None;
                                }
                                p = &p[1..]; // skip 'P'
                                let mut days: i64 = 0;
                                let mut secs: f64 = 0.0;
                                // parse up to 'T'
                                let mut before_t = p;
                                let mut after_t = "";
                                if let Some(tpos) = p.find('T') {
                                    before_t = &p[..tpos];
                                    after_t = &p[tpos+1..];
                                }
                                // days: number before 'D'
                                if let Some(dpos) = before_t.find('D') {
                                    let dnum = &before_t[..dpos];
                                    if !dnum.is_empty() {
                                        days = dnum.parse::<i64>().ok()?;
                                    }
                                }
                                // seconds: number before 'S' in after_t
                                if !after_t.is_empty() {
                                    if let Some(spos) = after_t.find('S') {
                                        let snum = &after_t[..spos];
                                        if !snum.is_empty() {
                                            secs = snum.parse::<f64>().ok()?;
                                        }
                                    }
                                }
                                let total = (days as f64) * 86_400f64 + secs;
                                let micros = (total * 1_000_000f64).round() as i64;
                                Some(if neg { -micros } else { micros })
                            }
                            let ca = col.struct_()?.field_by_name("__in")?;
                            let len = ca.len();
                            let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
                            for i in 0..len {
                                let av = ca.get(i).unwrap_or(AnyValue::Null);
                                let s_owned = match av {
                                    AnyValue::Null => None,
                                    AnyValue::Int64(v) => Some(v.to_string()),
                                    AnyValue::Float64(v) => Some(v.to_string()),
                                    AnyValue::String(s) => Some(s.to_string()),
                                    AnyValue::StringOwned(s) => Some(s.to_string()),
                                    other => Some(format!("{}", other)),
                                };
                                if let Some(st) = s_owned {
                                    out.push(parse_iso_micros(&st));
                                } else { out.push(None); }
                            }
                            let s = Series::new("__interval_micros", out);
                            Ok(s)
                        },
                        GetOutput::from_type(DataType::Int64),
                    );
                    micros_expr.cast(DataType::Duration(TimeUnit::Microseconds))
                }
                SqlType::Bytea => inner.cast(DataType::Binary),
                SqlType::Time => {
                    // Cast to Polars Time (time since midnight) when available
                    inner.cast(DataType::Time)
                }
                SqlType::Date | SqlType::Timestamp | SqlType::TimestampTz => {
                    // Treat all temporal casts as millisecond Datetime for now
                    inner.cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                }
                SqlType::Regclass => {
                    // Convert input string (table name) to a stable 32-bit OID using FNV-1a hash.
                    // If input is a literal string, resolve at plan time; otherwise map at runtime.
                    if let ArithExpr::Term(ArithTerm::Str(s)) = expr.as_ref() {
                        let oid = ctx.resolve_regclass_oid(s);
                        lit(oid)
                    } else {
                        // For now, only literal strings are supported for ::regclass in expression context.
                        // Non-literal inputs fall back to NULL (can be extended to per-row mapping if needed).
                        lit(polars::prelude::Null {})
                    }
                }
                SqlType::Regtype => {
                    // Similar to Regclass, convert type name to OID
                    // For now, treat it similarly - cast to integer representation
                    inner.cast(DataType::Int32)
                }
                SqlType::Array(inner_ty) => {
                    // Target inner dtype for the array elements
                    let target_inner_dt: DataType = match inner_ty.as_ref() {
                        SqlType::Boolean => DataType::Boolean,
                        SqlType::SmallInt | SqlType::Integer | SqlType::BigInt => DataType::Int64,
                        SqlType::Real | SqlType::Double | SqlType::Numeric(_) => DataType::Float64,
                        SqlType::Text | SqlType::Varchar(_) | SqlType::Char(_) | SqlType::Uuid | SqlType::Json | SqlType::Jsonb | SqlType::TimeTz => DataType::String,
                        SqlType::Interval => DataType::Duration(TimeUnit::Microseconds),
                        SqlType::Bytea => DataType::Binary,
                        SqlType::Time => DataType::Time,
                        SqlType::Date | SqlType::Timestamp | SqlType::TimestampTz => DataType::Datetime(TimeUnit::Milliseconds, None),
                        _ => DataType::String,
                    };
                    // If the input is already a List, stringify elements for safety, then cast to the desired List(inner) at the end.
                    // If the input is a String (e.g., Postgres brace literal '{...}' or 'a,b'), parse into a list of strings row-wise.
                    // Otherwise, fall back to simple cast to List(String) first, then to List(target_inner_dt).
                    let in_e = inner.clone().alias("__in");
                    let struct_expr = polars::lazy::dsl::as_struct(vec![in_e]);
                    let list_strings = struct_expr.map(
                        move |col: Column| {
                            use polars::prelude::AnyValue;
                            // Utility: parse a brace array literal into Vec<String>
                            fn parse_brace_array_literal(txt: &str) -> Vec<String> {
                                let s = txt.trim();
                                if !(s.starts_with('{') && s.ends_with('}')) {
                                    // simple CSV fallback
                                    return if s.is_empty() { Vec::new() } else { s.split(',').map(|p| p.trim().to_string()).collect() };
                                }
                                let inner = &s[1..s.len()-1];
                                let mut out: Vec<String> = Vec::new();
                                let mut cur = String::new();
                                let mut in_q = false; let mut esc = false;
                                for ch in inner.chars() {
                                    if in_q {
                                        if esc { cur.push(ch); esc = false; continue; }
                                        if ch == '\\' { esc = true; continue; }
                                        if ch == '"' { in_q = false; continue; }
                                        cur.push(ch);
                                    } else {
                                        match ch {
                                            '"' => { in_q = true; }
                                            ',' => { out.push(cur.trim().to_string()); cur.clear(); }
                                            _ => cur.push(ch),
                                        }
                                    }
                                }
                                if !cur.is_empty() { out.push(cur.trim().to_string()); }
                                // Convert bare NULL to empty string (represents NULL cell); keep others as-is (without quotes)
                                out.into_iter().map(|t| if t.eq_ignore_ascii_case("NULL") { String::new() } else { t }).collect()
                            }
                            let s = col.as_materialized_series();
                            let sc = s.struct_()?;
                            let fields = sc.fields_as_series();
                            let sin = &fields[0];
                            let n = sin.len();
                            let mut out_rows: Vec<Series> = Vec::with_capacity(n);
                            for i in 0..n {
                                let av = sin.get(i).unwrap_or(AnyValue::Null);
                                match av {
                                    AnyValue::List(inner) => {
                                        // Stringify existing list elements
                                        let mut vals: Vec<String> = Vec::with_capacity(inner.len());
                                        for j in 0..inner.len() {
                                            let v = inner.get(j).unwrap_or(AnyValue::Null);
                                            match v { AnyValue::Null => vals.push(String::new()), _ => vals.push(v.to_string()) }
                                        }
                                        out_rows.push(Series::new("__e".into(), vals));
                                    }
                                    AnyValue::String(sv) => {
                                        let parts = parse_brace_array_literal(sv);
                                        out_rows.push(Series::new("__e".into(), parts));
                                    }
                                    AnyValue::StringOwned(ref sv) => {
                                        let parts = parse_brace_array_literal(sv);
                                        out_rows.push(Series::new("__e".into(), parts));
                                    }
                                    AnyValue::Null => {
                                        out_rows.push(Series::new("__e".into(), Vec::<String>::new()));
                                    }
                                    _ => {
                                        // Fallback: single scalar → one-element array of its string form
                                        out_rows.push(Series::new("__e".into(), vec![av.to_string()]));
                                    }
                                }
                            }
                            Ok(Series::new("__arr".into(), out_rows).into_column())
                        },
                        move |_schema, _field| {
                            Ok(Field::new("__arr".into(), DataType::List(Box::new(DataType::String))))
                        }
                    );
                    list_strings.cast(DataType::List(Box::new(target_inner_dt)))
                }
            }
        }
        ArithExpr::BinOp { left, op, right } => {
            let l = build_arith_expr(left, ctx);
            let r = build_arith_expr(right, ctx);
            // Coerce numeric arithmetic operands to Float64 to tolerate mixed numeric
            // input types and stringified numerics from UDFs when metadata is missing.
            let lf = l.clone().cast(DataType::Float64);
            let rf = r.clone().cast(DataType::Float64);
            match op {
                ArithOp::Add => lf + rf,
                ArithOp::Sub => lf - rf,
                ArithOp::Mul => lf * rf,
                ArithOp::Div => lf / rf,
            }
        }
        ArithExpr::Concat(parts) => {
            // Concatenate parts as strings with Python-like formatting for numbers (drop trailing .0 when integer)
            let part_exprs: Vec<Expr> = parts.iter().map(|p| {
                let e = build_arith_expr(p, ctx);
                let ef = e.clone().cast(DataType::Float64);
                let is_num = ef.clone().is_not_null();
                let int_part = ef.clone().cast(DataType::Int64).cast(DataType::Float64);
                let frac = ef.clone() - int_part.clone();
                let is_int = frac.eq(lit(0.0));
                let num_str = when(is_int)
                    .then(int_part.cast(DataType::Int64).cast(DataType::String))
                    .otherwise(ef.cast(DataType::String));
                when(is_num).then(num_str).otherwise(e.cast(DataType::String))
            }).collect();
            #[allow(unused_mut)]
            let mut it = part_exprs.into_iter();
            if let Some(first) = it.next() {
                let mut acc = first;
                for e in it { acc = acc + e; }
                acc
            } else {
                lit("")
            }
        }
        ArithExpr::Call { name, args } => {
            // Compile scalar UDFs and certain built-ins into expressions usable anywhere.
            // Aggregate UDFs are handled in grouped aggregation paths, not here.
            
            // Strip pg_catalog. prefix if present for UDF lookup
            let mut name_lc = name.to_ascii_lowercase();
            if name_lc.starts_with("pg_catalog.") {
                name_lc = name_lc[11..].to_string();
            }

            // Special: SCALAR_SUBQUERY — execute inner SELECT once and substitute a literal value.
            // The parser encodes `(SELECT ...)` as Call { name: "SCALAR_SUBQUERY", args: [Term::Str(inner_sql)] }.
            if name_lc == "scalar_subquery" && args.len() == 1 {
                if let ArithExpr::Term(ArithTerm::Str(inner_sql)) = &args[0] {
                    if let Some(store) = &ctx.store {
                        // Parse inner SQL
                        if let Ok(cmd) = crate::server::query::parse(inner_sql) {
                            if let crate::server::query::Command::Select(q) = cmd {
                                if let Ok(df) = crate::server::exec::exec_select::run_select_with_context(store, &q, Some(ctx)) {
                                    // Extract first row, first column as a scalar
                                    if df.height() > 0 && df.width() > 0 {
                                        let name0 = &df.get_column_names()[0];
                                        if let Ok(s) = df.column(name0) {
                                            use polars::prelude::AnyValue;
                                            match s.get(0) {
                                                Ok(AnyValue::Int64(v)) => return lit(v),
                                                Ok(AnyValue::Int32(v)) => return lit(v as i64),
                                                Ok(AnyValue::UInt64(v)) => return lit(v as i64),
                                                Ok(AnyValue::UInt32(v)) => return lit(v as i64),
                                                Ok(AnyValue::Float64(v)) => return lit(v),
                                                Ok(AnyValue::Boolean(b)) => return lit(b),
                                                Ok(AnyValue::String(v)) => return lit(v.to_string()),
                                                Ok(AnyValue::StringOwned(v)) => return lit(v.to_string()),
                                                Ok(AnyValue::Null) | Err(_) => return lit(polars::prelude::Null {}),
                                                Ok(_) => return lit(polars::prelude::Null {}),
                                            }
                                        }
                                    }
                                    return lit(polars::prelude::Null {});
                                }
                            }
                        }
                    }
                    // If we cannot execute (no store or parse error), treat as NULL
                    return lit(polars::prelude::Null {});
                }
            }

            // Handle built-in: COALESCE(expr1, expr2, ...)
            if name_lc == "coalesce" && !args.is_empty() {
                let mut result = build_arith_expr(&args[0], ctx);
                for arg in &args[1..] {
                    let next_expr = build_arith_expr(arg, ctx);
                    result = result.fill_null(next_expr);
                }
                return result;
            }

            // Handle built-in: EXTRACT(EPOCH FROM expr)
            // For now, we expect EXTRACT to be called as EXTRACT with 2 args: field name and expression
            // The parser will need to transform "EXTRACT(EPOCH FROM expr)" into Call { name: "extract", args: [field, expr] }
            if name_lc == "extract" && args.len() == 2 {
                // First arg should be the field (EPOCH, YEAR, etc.)
                // Second arg is the expression to extract from
                if let ArithExpr::Term(ArithTerm::Str(field)) = &args[0] {
                    let field_lc = field.to_ascii_lowercase();
                    let expr = build_arith_expr(&args[1], ctx);
                    
                    match field_lc.as_str() {
                        "epoch" => {
                            // Convert timestamp to epoch seconds
                            // Polars timestamps are in milliseconds, so divide by 1000.0
                            return expr.cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                                .cast(DataType::Int64) / lit(1000i64);
                        }
                        "year" => return expr.dt().year(),
                        "month" => return expr.dt().month(),
                        "day" => return expr.dt().day(),
                        "hour" => return expr.dt().hour(),
                        "minute" => return expr.dt().minute(),
                        "second" => return expr.dt().second(),
                        _ => {
                            // Unsupported field, fall through to UDF path
                        }
                    }
                }
            }

            // Built-in: ARRAY[...] constructor encoded as Call { name: "array", args: [e1, e2, ...] }
            // For now, build a List(String) per row by stringifying elements safely.
            // Users can cast the resulting array to a specific typed array via ::typename[] if needed.
            if name_lc == "array" {
                // Build each argument expression; then use as_struct to get per-row values in a single closure
                let mut arg_exprs: Vec<Expr> = Vec::with_capacity(args.len());
                for (i, a) in args.iter().enumerate() {
                    let fname = format!("__arg{}", i);
                    arg_exprs.push(build_arith_expr(a, ctx).alias(&fname));
                }
                let struct_expr = polars::lazy::dsl::as_struct(arg_exprs);
                use polars::prelude::AnyValue;
                return struct_expr.map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let sc = s.struct_()?;
                        let fields = sc.fields_as_series();
                        let nrows = sc.len();
                        // Build a Series of type List(String) where each row is a list of the argument strings
                        let mut out_elems: Vec<Series> = Vec::with_capacity(nrows);
                        for i in 0..nrows {
                            let mut row_vals: Vec<String> = Vec::with_capacity(fields.len());
                            for f in &fields {
                                let av = f.get(i).unwrap_or(AnyValue::Null);
                                let s = match av {
                                    AnyValue::Null => String::new(),
                                    _ => av.to_string(),
                                };
                                row_vals.push(s);
                            }
                            let row_series = Series::new("__e".into(), row_vals);
                            out_elems.push(row_series);
                        }
                        Ok(Series::new("array".into(), out_elems).into_column())
                    },
                    |_schema, _field| Ok(Field::new("array".into(), DataType::List(Box::new(DataType::String))))
                );
            }

            // Built-in: array_concat(a, b)
            // Concatenate arrays or append/prepend scalars. Output is List(String) for dtype-agnostic safety.
            if name_lc == "array_concat" && args.len() == 2 {
                let l = build_arith_expr(&args[0], ctx).alias("__l");
                let r = build_arith_expr(&args[1], ctx).alias("__r");
                let struct_expr = polars::lazy::dsl::as_struct(vec![l, r]);
                use polars::prelude::AnyValue;
                return struct_expr.map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let sc = s.struct_()?;
                        let fields = sc.fields_as_series();
                        let nrows = sc.len();
                        if fields.len() != 2 { return Ok(Series::new("array_concat".into(), Vec::<Series>::new()).into_column()); }
                        let left = &fields[0];
                        let right = &fields[1];
                        let mut out_rows: Vec<Series> = Vec::with_capacity(nrows);
                        for i in 0..nrows {
                            // Helper to collect a value (list or scalar) into strings
                            let mut row_vals: Vec<String> = Vec::new();
                            let lav = left.get(i).unwrap_or(AnyValue::Null);
                            match lav {
                                AnyValue::List(inner) => {
                                    let m = inner.len();
                                    for j in 0..m {
                                        let v = inner.get(j).unwrap_or(AnyValue::Null);
                                        let s = match v { AnyValue::Null => String::new(), _ => v.to_string() };
                                        row_vals.push(s);
                                    }
                                }
                                AnyValue::Null => { /* nothing */ }
                                _ => {
                                    // treat as scalar prepend
                                    let s = lav.to_string();
                                    row_vals.push(s);
                                }
                            }
                            let rav = right.get(i).unwrap_or(AnyValue::Null);
                            match rav {
                                AnyValue::List(inner) => {
                                    let m = inner.len();
                                    for j in 0..m {
                                        let v = inner.get(j).unwrap_or(AnyValue::Null);
                                        let s = match v { AnyValue::Null => String::new(), _ => v.to_string() };
                                        row_vals.push(s);
                                    }
                                }
                                AnyValue::Null => { /* nothing */ }
                                _ => {
                                    // treat as scalar append
                                    let s = rav.to_string();
                                    row_vals.push(s);
                                }
                            }
                            out_rows.push(Series::new("__e".into(), row_vals));
                        }
                        Ok(Series::new("array_concat".into(), out_rows).into_column())
                    },
                    |_schema, _field| Ok(Field::new("array_concat".into(), DataType::List(Box::new(DataType::String))))
                );
            }

            // Built-in: array_at(base, idx)
            // Pythonic 0-based (negatives allowed) single element access on arrays (List).
            // Returns element as String (or NULL) for dtype-agnostic safety.
            if name_lc == "array_at" && args.len() == 2 {
                let base_e = build_arith_expr(&args[0], ctx).alias("__base");
                let idx_e = build_arith_expr(&args[1], ctx).alias("__idx");
                let struct_expr = polars::lazy::dsl::as_struct(vec![base_e, idx_e]);
                use polars::prelude::AnyValue;
                return struct_expr.map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let sc = s.struct_()?;
                        let fields = sc.fields_as_series();
                        if fields.len() != 2 { return Ok(Series::new("array_at".into(), Vec::<Option<String>>::new()).into_column()); }
                        let nrows = sc.len();
                        let mut out: Vec<Option<String>> = Vec::with_capacity(nrows);
                        for i in 0..nrows {
                            let bav = fields[0].get(i).unwrap_or(AnyValue::Null);
                            let iav = fields[1].get(i).unwrap_or(AnyValue::Null);
                            // determine index as i64 (fallback NULL if not numeric)
                            let mut idx_opt: Option<i64> = None;
                            match iav {
                                AnyValue::Int64(v) => idx_opt = Some(v),
                                AnyValue::Int32(v) => idx_opt = Some(v as i64),
                                AnyValue::UInt64(v) => idx_opt = Some(v as i64),
                                AnyValue::UInt32(v) => idx_opt = Some(v as i64),
                                AnyValue::Float64(v) => idx_opt = Some(v as i64),
                                AnyValue::Float32(v) => idx_opt = Some(v as i64),
                                AnyValue::String(s) => if let Ok(v) = s.parse::<i64>() { idx_opt = Some(v); },
                                AnyValue::StringOwned(ref s) => if let Ok(v) = s.parse::<i64>() { idx_opt = Some(v); },
                                _ => {}
                            }
                            let val_opt = match (bav, idx_opt) {
                                (AnyValue::List(inner), Some(mut idx)) => {
                                    let n = inner.len() as i64;
                                    if idx < 0 { idx += n; }
                                    if idx < 0 || idx >= n {
                                        None
                                    } else {
                                        let av = inner.get(idx as usize).unwrap_or(AnyValue::Null);
                                        match av {
                                            AnyValue::Null => None,
                                            _ => Some(av.to_string()),
                                        }
                                    }
                                }
                                _ => None,
                            };
                            out.push(val_opt);
                        }
                        Ok(Series::new("array_at".into(), out).into_column())
                    },
                    |_schema, _field| Ok(Field::new("array_at".into(), DataType::String))
                );
            }

            // Built-in: array_length(arr, dim)
            if name_lc == "array_length" && (args.len() == 1 || args.len() == 2) {
                let arr_expr = build_arith_expr(&args[0], ctx);
                // Only dimension 1 is supported; other dims return NULL (graceful) for now
                let dim_ok = if args.len() == 2 {
                    // Evaluate second argument as Int64 constant if possible; else accept 1 as default
                    match &args[1] {
                        ArithExpr::Term(ArithTerm::Number(n)) => (*n as i64) == 1,
                        _ => true,
                    }
                } else { true };
                if dim_ok {
                    // Compute length via map over column AnyValue
                    return arr_expr.map(
                        move |col: Column| {
                            let s = col.as_materialized_series();
                            let len = s.len();
                            let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
                            for i in 0..len {
                                match s.get(i) {
                                    Ok(polars::prelude::AnyValue::List(inner)) => {
                                        out.push(Some(inner.len() as i64));
                                    }
                                    Ok(polars::prelude::AnyValue::Null) | Err(_) => out.push(None),
                                    _ => out.push(None),
                                }
                            }
                            Ok(Series::new("array_length".into(), out).into_column())
                        },
                        |_schema, _field| Ok(Field::new("array_length".into(), DataType::Int64))
                    );
                } else {
                    return lit(polars::prelude::Null {});
                }
            }

            // Built-in: pg_get_viewdef(oid)
            // Return the stored view definition for the given pg_class OID, or NULL when not found.
            if name_lc == "pg_get_viewdef" && args.len() == 1 {
                // Build the argument expression as Int64
                let arg_expr = build_arith_expr(&args[0], ctx).cast(DataType::Int64);
                let store_opt = ctx.store.clone();
                return arg_expr.map(
                    move |col: Column| {
                        
                        let s = col.as_materialized_series();
                        let ca = s.i64()?;
                        let len = ca.len();
                        let mut out: Vec<Option<String>> = Vec::with_capacity(len);
                        for i in 0..len {
                            let oid_opt = ca.get(i);
                            if let Some(oid) = oid_opt {
                                if let Some(ref st) = store_opt {
                                    let def = crate::system_catalog::shared::lookup_view_definition_by_oid(st, oid as i32);
                                    out.push(def);
                                } else {
                                    out.push(None);
                                }
                            } else {
                                out.push(None);
                            }
                        }
                        Ok(Series::new("pg_get_viewdef".into(), out).into_column())
                    },
                    |_schema, _field| Ok(Field::new("pg_get_viewdef".into(), DataType::String))
                );
            }

            // Use the query-scoped registry from DataContext.
            // Clone the registry Arc so the closure can own it (avoids lifetime issues).
            // This ensures stable UDF resolution throughout query execution,
            // isolated from concurrent registry modifications.
            let reg = ctx.script_registry.clone();
            let meta = reg.as_ref().and_then(|r| r.get_meta(&name_lc));
            // If the UDF is declared as aggregate, return a plan-time error expression
            if let Some(m) = &meta {
                if matches!(m.kind, crate::scripts::ScriptKind::Aggregate) {
                    let msg = format!("Aggregate UDF '{}' cannot be used in a scalar context", name);
                    return lit(0).map(
                        move |_col: Column| { Err(polars::error::PolarsError::ComputeError(msg.clone().into())) },
                        |_schema, _field| Ok(Field::new("_udf_agg_in_scalar".into(), DataType::Null))
                    );
                }
                if m.returns.len() > 1 {
                    let msg = format!("UDF '{}' returns multiple columns; this is only allowed in SELECT projections", name);
                    return lit(0).map(
                        move |_col: Column| { Err(polars::error::PolarsError::ComputeError(msg.clone().into())) },
                        |_schema, _field| Ok(Field::new("_udf_multi_return".into(), DataType::Null))
                    );
                }
            }

            // Zero-arg scalar UDFs: evaluate eagerly to a literal to avoid as_struct([]) panic in polars
            if args.is_empty() {
                let out_dtype = meta.as_ref().and_then(|m| m.returns.first()).cloned().unwrap_or(DataType::String);
                let udf_name_eval = name_lc.clone();
                if let Some(r) = reg.as_ref() {
                    let ctx_info = crate::scripts::ContextInfo::from_data_context(ctx);
                    let out_res: anyhow::Result<polars::prelude::Expr> = r.with_lua_function(&udf_name_eval, |lua, func| {
                        use mlua::Value as LVal;
                        use mlua::MultiValue;
                        // Register Rust context accessor function for on-demand access
                        let _ = crate::scripts::ScriptRegistry::register_context_accessor(lua, &ctx_info);
                        let mv = MultiValue::new();
                        let outv: LVal = func.call(mv)?;
                        let expr = match out_dtype.clone() {
                            DataType::Boolean => match outv { LVal::Boolean(b) => lit(b), LVal::Nil => lit(polars::prelude::Null {}), _ => lit(polars::prelude::Null {}) },
                            DataType::Int64 => match outv { LVal::Integer(i) => lit(i), LVal::Number(f) => lit(f as i64), LVal::Nil => lit(polars::prelude::Null {}), _ => lit(polars::prelude::Null {}) },
                            DataType::Float64 => match outv { LVal::Number(f) => lit(f), LVal::Integer(i) => lit(i as f64), LVal::Nil => lit(polars::prelude::Null {}), _ => lit(polars::prelude::Null {}) },
                            _ => match outv { LVal::String(s) => lit(s.to_str()?.to_string()), LVal::Nil => lit(polars::prelude::Null {}), _ => lit(polars::prelude::Null {}) },
                        };
                        Ok(expr)
                    });
                    return out_res.unwrap_or_else(|_| lit(polars::prelude::Null {}));
                } else {
                    return lit(polars::prelude::Null {});
                }
            }

            // Build argument expressions and wrap them into a Struct to access all arg values in the map closure.
            // For vector-similarity UDFs, allow native arrays (Polars List) to flow into Lua as tables; no forced coercion.
            let mut arg_exprs: Vec<Expr> = Vec::with_capacity(args.len());
            let mut field_names: Vec<String> = Vec::with_capacity(args.len());
            let _is_vec_udf = matches!(name_lc.as_str(), "cosine_sim" | "vec_l2" | "vec_ip");
            for (i, a) in args.iter().enumerate() {
                let fname = format!("__arg{}", i);
                field_names.push(fname.clone());
                let built = build_arith_expr(a, ctx);
                arg_exprs.push(built.alias(&fname));
            }
            let struct_expr = polars::lazy::dsl::as_struct(arg_exprs);

            // Determine output dtype from metadata (fallback to Utf8)
            let out_dtype = meta.as_ref().and_then(|m| m.returns.first()).cloned().unwrap_or(DataType::String);
            let udf_name_eval = name_lc.clone();
            let udf_name_field = name_lc.clone();
            let out_dtype_field = out_dtype.clone();
            let ctx_info = crate::scripts::ContextInfo::from_data_context(ctx);
            crate::tprintln!("[UDF] build: name='{}' out_dtype={:?} arg_fields={}", udf_name_eval, out_dtype, field_names.len());

            struct_expr.map(
                move |col: Column| {
                    // Retrieve args columns for this Polars chunk
                    let s = col.as_materialized_series();
                    let sc = s.struct_()?;
                    let fields = sc.fields_as_series();
                    // Diagnostics: log struct field count and dtypes for UDF args
                    if udf_name_eval == "cosine_sim" || udf_name_eval == "vec_l2" || udf_name_eval == "vec_ip" {
                        let mut info: Vec<String> = Vec::new();
                        for fs in &fields {
                            info.push(format!("{}:{:?}", fs.name(), fs.dtype()));
                        }
                        crate::tprintln!("[UDF] struct fields: name='{}' count={} schema=[{}]", udf_name_eval, fields.len(), info.join(", "));
                    }
                    let len = sc.len();
                    
                    let null_on_err = crate::system::get_null_on_error();

                    // Execute UDF once per row using a single Lua state and resolved function
                    if let Some(r) = reg.as_ref() {
                        let out_col: Column = r
                            .with_lua_function(&udf_name_eval, |lua, func| {
                                use mlua::Value as LVal;
                                use mlua::MultiValue;
                                // Register Rust context accessor function for on-demand access
                                let _ = crate::scripts::ScriptRegistry::register_context_accessor(lua, &ctx_info);
                                match out_dtype.clone() {
                                    DataType::Boolean => {
                                        let mut vals: Vec<Option<bool>> = Vec::with_capacity(len);
                                    for row_idx in 0..len {
                                        let mut mvals = MultiValue::new();
                                        for f in fields.iter().rev() {
                                            let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                            let lv = match av {
                                                polars::prelude::AnyValue::Null => LVal::Nil,
                                                polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                polars::prelude::AnyValue::List(ref inner) => {
                                                    // Convert Polars List to Lua array-style table
                                                    let tbl = lua.create_table()?;
                                                    let ser = inner; // Series
                                                    let len_list = ser.len();
                                                    for li in 0..len_list {
                                                        let av2 = ser.get(li).unwrap_or(polars::prelude::AnyValue::Null);
                                                        match av2 {
                                                            polars::prelude::AnyValue::Null => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                            polars::prelude::AnyValue::Boolean(b) => { tbl.set(li as i64 + 1, mlua::Value::Boolean(b))?; }
                                                            polars::prelude::AnyValue::Int64(v) => { tbl.set(li as i64 + 1, mlua::Value::Integer(v))?; }
                                                            polars::prelude::AnyValue::Float64(v) => { tbl.set(li as i64 + 1, mlua::Value::Number(v))?; }
                                                            polars::prelude::AnyValue::String(s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s)?))?; }
                                                            polars::prelude::AnyValue::StringOwned(ref s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s.as_str())?))?; }
                                                            _ => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                        }
                                                    }
                                                    LVal::Table(tbl)
                                                }
                                                _ => LVal::Nil,
                                            };
                                            mvals.push_front(lv);
                                        }
                                        let outv_result = func.call::<_, LVal>(mvals);
                                        let outv = if null_on_err {
                                            outv_result.unwrap_or(LVal::Nil)
                                        } else {
                                            outv_result?
                                        };
                                        match outv { LVal::Boolean(b) => vals.push(Some(b)), LVal::Nil => vals.push(None), _ => vals.push(None) }
                                    }
                                    let s = Series::new(udf_name_eval.as_str().into(), vals);
                                    Ok(s.into_column())
                                }
                                DataType::Int64 => {
                                    let mut vals: Vec<Option<i64>> = Vec::with_capacity(len);
                                    for row_idx in 0..len {
                                        let mut mvals = MultiValue::new();
                                        for f in fields.iter().rev() {
                                            let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                            let lv = match av {
                                                polars::prelude::AnyValue::Null => LVal::Nil,
                                                polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                polars::prelude::AnyValue::List(ref inner) => {
                                                    let tbl = lua.create_table()?;
                                                    let ser = inner;
                                                    let len_list = ser.len();
                                                    for li in 0..len_list {
                                                        let av2 = ser.get(li).unwrap_or(polars::prelude::AnyValue::Null);
                                                        match av2 {
                                                            polars::prelude::AnyValue::Null => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                            polars::prelude::AnyValue::Boolean(b) => { tbl.set(li as i64 + 1, mlua::Value::Boolean(b))?; }
                                                            polars::prelude::AnyValue::Int64(v) => { tbl.set(li as i64 + 1, mlua::Value::Integer(v))?; }
                                                            polars::prelude::AnyValue::Float64(v) => { tbl.set(li as i64 + 1, mlua::Value::Number(v))?; }
                                                            polars::prelude::AnyValue::String(s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s)?))?; }
                                                            polars::prelude::AnyValue::StringOwned(ref s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s.as_str())?))?; }
                                                            _ => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                        }
                                                    }
                                                    LVal::Table(tbl)
                                                }
                                                _ => LVal::Nil,
                                            };
                                            mvals.push_front(lv);
                                        }
                                        let outv_result = func.call::<_, LVal>(mvals);
                                        let outv = if null_on_err {
                                            outv_result.unwrap_or(LVal::Nil)
                                        } else {
                                            outv_result?
                                        };
                                        match outv { LVal::Integer(i) => vals.push(Some(i)), LVal::Number(f) => vals.push(Some(f as i64)), LVal::Nil => vals.push(None), _ => vals.push(None) }
                                    }
                                    let s = Series::new(udf_name_eval.as_str().into(), vals);
                                    Ok(s.into_column())
                                }
                                DataType::Float64 => {
                                    let mut vals: Vec<Option<f64>> = Vec::with_capacity(len);
                                    // For diagnostics: preview a few rows of arguments for vector UDFs
                                    let is_vec_name = udf_name_eval == "cosine_sim" || udf_name_eval == "vec_l2" || udf_name_eval == "vec_ip";
                                    let mut preview_printed = 0usize;
                                    let preview_limit = 3usize;
                                    for row_idx in 0..len {
                                        let mut mvals = MultiValue::new();
                                        for f in fields.iter().rev() {
                                            let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                            let lv = match av {
                                                polars::prelude::AnyValue::Null => LVal::Nil,
                                                polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                polars::prelude::AnyValue::List(ref inner) => {
                                                    let tbl = lua.create_table()?;
                                                    let ser = inner;
                                                    let len_list = ser.len();
                                                    for li in 0..len_list {
                                                        let av2 = ser.get(li).unwrap_or(polars::prelude::AnyValue::Null);
                                                        match av2 {
                                                            polars::prelude::AnyValue::Null => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                            polars::prelude::AnyValue::Boolean(b) => { tbl.set(li as i64 + 1, mlua::Value::Boolean(b))?; }
                                                            polars::prelude::AnyValue::Int64(v) => { tbl.set(li as i64 + 1, mlua::Value::Integer(v))?; }
                                                            polars::prelude::AnyValue::Float64(v) => { tbl.set(li as i64 + 1, mlua::Value::Number(v))?; }
                                                            polars::prelude::AnyValue::String(s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s)?))?; }
                                                            polars::prelude::AnyValue::StringOwned(ref s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s.as_str())?))?; }
                                                            _ => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                        }
                                                    }
                                                    LVal::Table(tbl)
                                                }
                                                _ => LVal::Nil,
                                            };
                                            mvals.push_front(lv);
                                        }
                                        if is_vec_name && preview_printed < preview_limit {
                                            let mut ivals: Vec<String> = Vec::with_capacity(fields.len());
                                            for f in fields.iter() {
                                                let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                                ivals.push(format!("{:?}", av));
                                            }
                                            crate::tprintln!("[UDF] args preview: name='{}' row={} args={:?}", udf_name_eval, row_idx, ivals);
                                            preview_printed += 1;
                                        }
                                        let outv_result = func.call::<_, LVal>(mvals);
                                        let outv = if null_on_err {
                                            outv_result.unwrap_or(LVal::Nil)
                                        } else {
                                            outv_result?
                                        };
                                        match outv {
                                            LVal::Number(f) => vals.push(Some(f)),
                                            LVal::Integer(i) => vals.push(Some(i as f64)),
                                            LVal::Nil => {
                                                if udf_name_eval == "cosine_sim" || udf_name_eval == "vec_l2" || udf_name_eval == "vec_ip" {
                                                    let mut ivals: Vec<String> = Vec::with_capacity(fields.len());
                                                    for f in fields.iter() {
                                                        let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                                        ivals.push(format!("{:?}", av));
                                                    }
                                                    crate::tprintln!("[UDF] eval Nil: name='{}' row={} args={:?}", udf_name_eval, row_idx, ivals);
                                                }
                                                vals.push(None)
                                            }
                                            _ => {
                                                if udf_name_eval == "cosine_sim" || udf_name_eval == "vec_l2" || udf_name_eval == "vec_ip" {
                                                    crate::tprintln!("[UDF] eval unexpected type (not number/integer/nil) for '{}' — coercing to NULL", udf_name_eval);
                                                }
                                                vals.push(None)
                                            }
                                        }
                                    }
                                    let s = Series::new(udf_name_eval.as_str().into(), vals);
                                    Ok(s.into_column())
                                }
                                DataType::String => {
                                    let mut vals: Vec<Option<String>> = Vec::with_capacity(len);
                                    for row_idx in 0..len {
                                        let mut mvals = MultiValue::new();
                                        for f in fields.iter().rev() {
                                            let av = f.get(row_idx).unwrap_or(polars::prelude::AnyValue::Null);
                                            let lv = match av {
                                                polars::prelude::AnyValue::Null => LVal::Nil,
                                                polars::prelude::AnyValue::Boolean(b) => LVal::Boolean(b),
                                                polars::prelude::AnyValue::Int64(v) => LVal::Integer(v),
                                                polars::prelude::AnyValue::Float64(v) => LVal::Number(v),
                                                polars::prelude::AnyValue::String(s) => LVal::String(lua.create_string(s)?),
                                                polars::prelude::AnyValue::StringOwned(ref s) => LVal::String(lua.create_string(s.as_str())?),
                                                polars::prelude::AnyValue::List(ref inner) => {
                                                    let tbl = lua.create_table()?;
                                                    let ser = inner;
                                                    let len_list = ser.len();
                                                    for li in 0..len_list {
                                                        let av2 = ser.get(li).unwrap_or(polars::prelude::AnyValue::Null);
                                                        match av2 {
                                                            polars::prelude::AnyValue::Null => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                            polars::prelude::AnyValue::Boolean(b) => { tbl.set(li as i64 + 1, mlua::Value::Boolean(b))?; }
                                                            polars::prelude::AnyValue::Int64(v) => { tbl.set(li as i64 + 1, mlua::Value::Integer(v))?; }
                                                            polars::prelude::AnyValue::Float64(v) => { tbl.set(li as i64 + 1, mlua::Value::Number(v))?; }
                                                            polars::prelude::AnyValue::String(s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s)?))?; }
                                                            polars::prelude::AnyValue::StringOwned(ref s) => { tbl.set(li as i64 + 1, mlua::Value::String(lua.create_string(s.as_str())?))?; }
                                                            _ => { tbl.set(li as i64 + 1, mlua::Value::Nil)?; }
                                                        }
                                                    }
                                                    LVal::Table(tbl)
                                                }
                                                _ => LVal::Nil,
                                            };
                                            mvals.push_front(lv);
                                        }
                                        let outv_result = func.call::<_, LVal>(mvals);
                                        let outv = if null_on_err {
                                            outv_result.unwrap_or(LVal::Nil)
                                        } else {
                                            outv_result?
                                        };
                                        match outv { LVal::String(s) => vals.push(Some(s.to_str()?.to_string())), LVal::Nil => vals.push(None), _ => vals.push(None) }
                                    }
                                    let s = Series::new(udf_name_eval.as_str().into(), vals);
                                    Ok(s.into_column())
                                }
                                _ => {
                                    let s = Series::new_null("_udf_null".into(), len);
                                    Ok(s.into_column())
                                }
                            }
                            })
                            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;
                        Ok(out_col)
                    } else {
                        Err(polars::error::PolarsError::ComputeError("Lua registry not initialized".into()))
                    }
                },
                move |_schema, _field| {
                    // Name result based on function name
                    Ok(Field::new(udf_name_field.to_string().into(), out_dtype_field.clone()))
                }
            )
        }
        ArithExpr::Func(df) => {            
            // Helper to ms unit multiplier
            fn unit_ms(p: &DatePart) -> i64 {
                match p { DatePart::Millisecond => 1, DatePart::Second => 1000, DatePart::Minute => 60_000, DatePart::Hour => 3_600_000, DatePart::Day => 86_400_000, DatePart::Month => 2_592_000_000, DatePart::Year => 31_536_000_000 }
            }
            match df {
                DateFunc::DatePart(part, a1) => {
                    let e = build_arith_expr(a1, ctx).cast(DataType::Int64).cast(DataType::Datetime(TimeUnit::Milliseconds, None));
                    let out = match part {
                        DatePart::Year => e.clone().dt().year().cast(DataType::Int64),
                        DatePart::Month => e.clone().dt().month().cast(DataType::Int64),
                        DatePart::Day => e.clone().dt().day().cast(DataType::Int64),
                        DatePart::Hour => e.clone().dt().hour().cast(DataType::Int64),
                        DatePart::Minute => e.clone().dt().minute().cast(DataType::Int64),
                        DatePart::Second => (e.clone().cast(DataType::Int64) / lit(1000i64)).cast(DataType::Int64),
                        DatePart::Millisecond => e.clone().cast(DataType::Int64),
                    };
                    out.cast(DataType::Float64)
                }
                DateFunc::DateAdd(part, n, d) => {
                    let e_n = build_arith_expr(n, ctx).cast(DataType::Int64);
                    let e_d = build_arith_expr(d, ctx).cast(DataType::Int64);
                    let mul = unit_ms(part);
                    (e_d + e_n * lit(mul)).cast(DataType::Float64)
                }
                DateFunc::DateDiff(part, a, b) => {
                    let e_a = build_arith_expr(a, ctx).cast(DataType::Int64);
                    let e_b = build_arith_expr(b, ctx).cast(DataType::Int64);
                    let div = unit_ms(part);
                    ((e_a - e_b) / lit(div)).cast(DataType::Float64)
                }
            }
        }
        ArithExpr::Slice { base, start, stop, step } => {            
            // Build base expression without forcing a cast; we will branch on dtype at runtime.
            let base_e = build_arith_expr(base, ctx);
            let start_b = start.clone();
            let stop_b = stop.clone();
            let step_v = *step;

            // Python-like slicing over Unicode scalar values (0-based with negative indices).
            fn python_slice(s: &str, start: Option<i64>, stop: Option<i64>, step: Option<i64>) -> String {
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let step = step.unwrap_or(1);
                // Graceful handling: a zero step is invalid in Python slicing; instead of panicking,
                // return an empty string to avoid crashing the server.
                if step == 0 { return String::new(); }
                // defaults based on step sign
                let (mut start_idx, mut stop_idx) = if step > 0 {
                    (0i64, len)
                } else {
                    (len - 1, -len - 1)
                };
                // normalize start
                if let Some(mut st) = start {
                    if st < 0 { st += len; }
                    if st < 0 { st = 0; }
                    if st > len { st = len; }
                    start_idx = st;
                }
                // normalize stop
                if let Some(mut sp) = stop {
                    if sp < 0 { sp += len; }
                    if step > 0 {
                        if sp < 0 { sp = 0; }
                    } else if sp < -1 { sp = -1; }
                    if sp > len { sp = len; }
                    stop_idx = sp;
                }
                let mut out = String::new();
                if step > 0 {
                    let mut i = start_idx;
                    while i < stop_idx && i < len {
                        if i >= 0 { out.push(chars[i as usize]); }
                        i += step;
                    }
                } else {
                    let mut i = start_idx;
                    while i > stop_idx && i >= 0 {
                        if i < len { out.push(chars[i as usize]); }
                        i += step; // step is negative
                    }
                }
                out
            }

            base_e.map(
                move |col: Column| {
                    use polars::prelude::AnyValue;
                    let s = col.as_materialized_series();
                    match s.dtype() {
                        DataType::String => {
                            // String slicing as before
                            let ca = s.str()?;
                            let out = ca.apply(|opt_val| {
                                opt_val.map(|val| {
                                    let s_i = match &start_b { Some(StrSliceBound::Index(v)) => Some(*v), _ => None };
                                    let e_i = match &stop_b { Some(StrSliceBound::Index(v)) => Some(*v), _ => None };
                                    let step_i = step_v;
                                    std::borrow::Cow::Owned(python_slice(val, s_i, e_i, step_i))
                                })
                            });
                            Ok(out.into_column())
                        }
                        DataType::List(_inner) => {
                            // Array/List slicing with Pythonic semantics (0-based, negatives, step). Output List(String) for robustness.
                            let len = s.len();
                            let mut out_elems: Vec<Series> = Vec::with_capacity(len);
                            for i in 0..len {
                                match s.get(i) {
                                    Ok(AnyValue::List(inner)) => {
                                        let n = inner.len() as i64;
                                        let step = step_v.unwrap_or(1);
                                        if step == 0 { out_elems.push(Series::new("__e".into(), Vec::<String>::new())); continue; }
                                        let mut start_idx: i64 = if step > 0 { 0 } else { n - 1 };
                                        let mut stop_idx: i64 = if step > 0 { n } else { -n - 1 };
                                        if let Some(StrSliceBound::Index(mut st)) = start_b { if st < 0 { st += n; } if st < 0 { st = 0; } if st > n { st = n; } start_idx = st; }
                                        if let Some(StrSliceBound::Index(mut sp)) = stop_b {
                                            if sp < 0 { sp += n; }
                                            if step > 0 { if sp < 0 { sp = 0; } }
                                            else if sp < -1 { sp = -1; }
                                            if sp > n { sp = n; }
                                            stop_idx = sp;
                                        }
                                        let mut row_vals: Vec<String> = Vec::new();
                                        if step > 0 {
                                            let mut j = start_idx;
                                            while j < stop_idx && j < n {
                                                if j >= 0 {
                                                    let av = inner.get(j as usize).unwrap_or(AnyValue::Null);
                                                    let s = match av { AnyValue::Null => String::new(), _ => av.to_string() };
                                                    row_vals.push(s);
                                                }
                                                j += step;
                                            }
                                        } else {
                                            let mut j = start_idx;
                                            while j > stop_idx && j >= 0 {
                                                if j < n {
                                                    let av = inner.get(j as usize).unwrap_or(AnyValue::Null);
                                                    let s = match av { AnyValue::Null => String::new(), _ => av.to_string() };
                                                    row_vals.push(s);
                                                }
                                                j += step; // step negative
                                            }
                                        }
                                        out_elems.push(Series::new("__e".into(), row_vals));
                                    }
                                    _ => {
                                        // Not a list at this row → push NULL list (empty)
                                        out_elems.push(Series::new("__e".into(), Vec::<String>::new()));
                                    }
                                }
                            }
                            Ok(Series::new(col.name().to_string().into(), out_elems).into_column())
                        }
                        _ => {
                            // Unsupported dtype: return NULLs (as strings) to avoid panics
                            let len = s.len();
                            let v: Vec<Option<String>> = (0..len).map(|_| None).collect();
                            Ok(Series::new(col.name().to_string().into(), v).into_column())
                        }
                    }
                },
                |_schema, field| {
                    // Choose output dtype based on input
                    match field.dtype() {
                        DataType::List(_) => Ok(Field::new(field.name().clone(), DataType::List(Box::new(DataType::String)))),
                        _ => Ok(Field::new(field.name().clone(), DataType::String)),
                    }
                }
            )
        }
        ArithExpr::Predicate(w) => {
            build_where_expr(w, ctx)
        }
        ArithExpr::Case { when_clauses, else_expr } => {
            // Build a CASE expression using nested when().then().otherwise()
            // Process from last to first to build nested structure
            if when_clauses.is_empty() {
                // Should not happen due to parser validation, but handle gracefully
                return lit(polars::prelude::Null {});
            }
            
            // Start with the else expression or NULL
            let mut result = if let Some(else_val) = else_expr {
                build_arith_expr(else_val, ctx)
            } else {
                lit(polars::prelude::Null {})
            };
            
            // Build nested when-then-otherwise from last to first
            for clause in when_clauses.iter().rev() {
                let cond = build_where_expr(&clause.0, ctx);
                let then_val = build_arith_expr(&clause.1, ctx);
                result = polars::lazy::dsl::when(cond).then(then_val).otherwise(result);
            }
            
            result
        }
    }
}

pub fn collect_where_columns(w: &WhereExpr, out: &mut Vec<String>) {
    match w {
        WhereExpr::Comp { left, right, .. } => {
            collect_from_arith(left, out);
            collect_from_arith(right, out);
        }
        WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
            collect_where_columns(a, out);
            collect_where_columns(b, out);
        }
        WhereExpr::IsNull { expr, .. } => {
            collect_from_arith(expr, out);
        }
        WhereExpr::Exists { subquery, .. } => {
            // Collect columns from subquery's WHERE clause
            if let Some(w) = &subquery.where_clause {
                collect_where_columns(w, out);
            }
        }
        WhereExpr::All { left, subquery, .. } | WhereExpr::Any { left, subquery, .. } => {
            // Collect columns from left expression
            collect_from_arith(left, out);
            // Collect columns from subquery's WHERE clause
            if let Some(w) = &subquery.where_clause {
                collect_where_columns(w, out);
            }
        }
    }
}

pub fn collect_from_arith(a: &ArithExpr, out: &mut Vec<String>) {
    match a {
        ArithExpr::Term(ArithTerm::Col { name, .. }) => out.push(name.clone()),
        ArithExpr::Term(ArithTerm::Number(_)) => {},
        ArithExpr::Term(ArithTerm::Str(_)) => {},
        ArithExpr::Term(ArithTerm::Null) => {},
        ArithExpr::Cast { expr, .. } => { collect_from_arith(expr, out); }
        ArithExpr::BinOp { left, right, .. } => { collect_from_arith(left, out); collect_from_arith(right, out); }
        ArithExpr::Func(df) => {            
            match df {
                DateFunc::DatePart(_, a1) => collect_from_arith(a1, out),
                DateFunc::DateAdd(_, n, d) => { collect_from_arith(n, out); collect_from_arith(d, out); }
                DateFunc::DateDiff(_, a, b) => { collect_from_arith(a, out); collect_from_arith(b, out); }
            }
        }
        ArithExpr::Slice { base, start, stop, .. } => {
            // collect from base expression
            collect_from_arith(base, out);
            // if bounds are dynamic expressions (non-literals), collect from them too            
            if let Some(StrSliceBound::Pattern { expr, .. }) = start { collect_from_arith(expr, out); }
            if let Some(StrSliceBound::Pattern { expr, .. }) = stop { collect_from_arith(expr, out); }
        }
        ArithExpr::Concat(parts) => {
            for p in parts { collect_from_arith(p, out); }
        }
        ArithExpr::Call { args, .. } => {
            for a in args { collect_from_arith(a, out); }
        }
        ArithExpr::Predicate(w) => {
            collect_where_columns(w, out);
        }
        ArithExpr::Case { when_clauses, else_expr } => {
            for (cond, val) in when_clauses {
                collect_where_columns(cond, out);
                collect_from_arith(val, out);
            }
            if let Some(else_val) = else_expr {
                collect_from_arith(else_val, out);
            }
        }
    }
}

pub fn sql_like_to_regex(pat: &str) -> String {
    // Convert SQL LIKE pattern to a Rust regex anchored at both ends.
    // % -> .*, _ -> . ; escape other regex meta chars.
    let mut out = String::from("^");
    let chars = pat.chars();
    for c in chars {
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            // Escape regex metacharacters
            '.' | '+' | '*' | '?' | '(' | ')' | '|' | '{' | '}' | '[' | ']' | '^' | '$' | '\\' => {
                out.push('\\'); out.push(c);
            }
            _ => out.push(c),
        }
    }
    out.push('$');
    out
}
