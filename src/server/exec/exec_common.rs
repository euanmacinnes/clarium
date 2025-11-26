use polars::prelude::*;
// Needed for Expr::map closure signature and output typing
use polars::prelude::Column;
use regex::Regex;


use crate::{query::{ArithExpr, ArithOp, ArithTerm, CompOp, WhereExpr, SqlType}};



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
fn regclass_oid_with_defaults(name: &str, db: Option<&str>, schema: Option<&str>) -> i32 {
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
                let d = db.unwrap_or("timeline");
                let s = schema.unwrap_or("public");
                format!("{}/{}/{}", d, s, parts[0])
            }
            2 => {
                let d = db.unwrap_or("timeline");
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
            use crate::query::{ArithExpr, ArithTerm};
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
            if let (crate::query::CompOp::Eq, Some(n)) = (op, const_number(right)) {
                if let Some(be) = to_bool_expr(left) {
                    return if (n - 1.0).abs() < f64::EPSILON { be } else { be.not() };
                }
            }
            // Handle 1/0 = RIGHT pattern
            if let (crate::query::CompOp::Eq, Some(n)) = (op, const_number(left)) {
                if let Some(be) = to_bool_expr(right) {
                    return if (n - 1.0).abs() < f64::EPSILON { be } else { be.not() };
                }
            }
            // Handle ordered comparisons between boolean predicate and 0/1
            // Map to equivalent boolean masks to avoid numeric-vs-string coercions.
            if let Some(n) = const_number(right) {
                if let Some(be) = to_bool_expr(left) {
                    use crate::query::CompOp;
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
                    use crate::query::CompOp;
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
                    if numeric_literal { l.clone().cast(DataType::Float64).eq(r.clone().cast(DataType::Float64)) } else { l.eq(r) }
                }
                CompOp::Ne => {
                    let numeric_literal = matches!(right, ArithExpr::Term(ArithTerm::Number(_))) || matches!(left, ArithExpr::Term(ArithTerm::Number(_)));
                    if numeric_literal { l.clone().cast(DataType::Float64).neq(r.clone().cast(DataType::Float64)) } else { l.neq(r) }
                }
                CompOp::Like | CompOp::NotLike => {
                    // Support LIKE when RHS is a literal string pattern by converting to a regex and applying via map
                    if let ArithExpr::Term(ArithTerm::Str(pat)) = right {
                        let regex_text = sql_like_to_regex(pat);
                        let re = Regex::new(&regex_text).unwrap();
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
                SqlType::Text | SqlType::Varchar(_) | SqlType::Char(_) | SqlType::Uuid | SqlType::Json | SqlType::Jsonb | SqlType::Interval | SqlType::TimeTz => {
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
            let mut arg_exprs: Vec<Expr> = Vec::with_capacity(args.len());
            let mut field_names: Vec<String> = Vec::with_capacity(args.len());
            for (i, a) in args.iter().enumerate() {
                let fname = format!("__arg{}", i);
                field_names.push(fname.clone());
                arg_exprs.push(build_arith_expr(a, ctx).alias(&fname));
            }
            let struct_expr = polars::lazy::dsl::as_struct(arg_exprs);

            // Determine output dtype from metadata (fallback to Utf8)
            let out_dtype = meta.as_ref().and_then(|m| m.returns.first()).cloned().unwrap_or(DataType::String);
            let udf_name_eval = name_lc.clone();
            let udf_name_field = name_lc.clone();
            let out_dtype_field = out_dtype.clone();
            let ctx_info = crate::scripts::ContextInfo::from_data_context(ctx);

            struct_expr.map(
                move |col: Column| {
                    // Retrieve args columns for this Polars chunk
                    let s = col.as_materialized_series();
                    let sc = s.struct_()?;
                    let fields = sc.fields_as_series();
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
                                        match outv { LVal::Number(f) => vals.push(Some(f)), LVal::Integer(i) => vals.push(Some(i as f64)), LVal::Nil => vals.push(None), _ => vals.push(None) }
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
            use crate::query::{DateFunc, DatePart};
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
            use crate::query::StrSliceBound;
            let base_e = build_arith_expr(base, ctx).cast(DataType::String);
            let start_b = start.clone();
            let stop_b = stop.clone();
            let step_v = *step;

            // Python-like slicing over Unicode scalar values
            fn python_slice(s: &str, start: Option<i64>, stop: Option<i64>, step: Option<i64>) -> String {
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let step = step.unwrap_or(1);
                if step == 0 { panic!("step=0 in string slice"); }
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
                        if i >= 0 {
                            out.push(chars[i as usize]);
                        }
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
                    let s = col.as_materialized_series();
                    let ca = s.str()?;
                    let out = ca.apply(|opt_val| {
                        opt_val.map(|val| {
                            // Only integer indices are supported now
                            let s_i = match &start_b { Some(StrSliceBound::Index(v)) => Some(*v), _ => None };
                            let e_i = match &stop_b { Some(StrSliceBound::Index(v)) => Some(*v), _ => None };
                            let step_i = step_v;
                            std::borrow::Cow::Owned(python_slice(val, s_i, e_i, step_i))
                        })
                    });
                    Ok(out.into_column())
                },
                |_schema, field| {
                    Ok(Field::new(field.name().clone(), DataType::String))
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
            use crate::query::DateFunc;
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
            use crate::query::StrSliceBound;
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
