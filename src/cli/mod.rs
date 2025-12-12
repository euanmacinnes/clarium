use serde_json::Value;

// Render query results as an ASCII table.
// Returns true if a table was printed (i.e., detected rows/columns), false otherwise.
pub fn print_query_result(val: &Value) -> bool {
    println!("query results:");
    // Honor env override to force JSON output
    if std::env::var("CSQL_OUTPUT").map(|v| v.eq_ignore_ascii_case("json")).unwrap_or(false) {
        return false;
    }

    // Try common shapes
    let (cols_opt, rows_opt, metrics_opt) =
        extract_table(val)
            .or_else(|| { // sometimes directly under top-level
                let cols = val.get("columns");
                let rows = val.get("rows");
                let metrics = val.get("metrics");
                Some((cols.cloned(), rows.cloned(), metrics.cloned()))
            })
            .unwrap_or_else(|| try_from_top_level_array(val));

    let cols_v = match cols_opt { Some(v) => v, None => return false };
    let rows_v = match rows_opt { Some(v) => v, None => return false };
    let cols = match normalize_columns(&cols_v) { Some(v) => v, None => return false };
    let rows = match normalize_rows(&rows_v) { Some(v) => v, None => return false };

    // If there are no rows, stick to JSON as per requirement (only print table when rows are returned)
    if rows.is_empty() { return false; }

    // Compute widths
    let max_col_width: usize = 80; // cap to keep output readable
    let mut widths: Vec<usize> = cols.iter().map(|s| s.len().min(max_col_width)).collect();
    for r in &rows {
        for (i, cell) in r.iter().enumerate().take(cols.len()) {
            let w = display_len(cell);
            if w > widths[i] { widths[i] = w.min(max_col_width); }
        }
    }

    // Header
    let sep = build_separator(&widths);
    println!("{}", sep);
    println!("{}", build_row(&cols, &widths));
    println!("{}", sep);
    // Rows
    for r in &rows {
        println!("{}", build_row(r, &widths));
    }
    println!("{}", sep);

    // Footer summary
    let rows_count = rows.len();
    let cols_count = cols.len();
    let mut summary = format!("rows: {}, cols: {}", rows_count, cols_count);
    if let Some(ms) = extract_elapsed_ms(val).or_else(|| extract_elapsed_ms_from(metrics_opt.as_ref())) {
        summary.push_str(&format!(", elapsed_ms: {}", ms));
    }
    println!("{}", summary);

    true
}

fn extract_table(val: &Value) -> Option<(Option<Value>, Option<Value>, Option<Value>)> {
    let res = val.get("results")?;
    let cols = res.get("columns");
    let rows = res.get("rows");
    let metrics = res.get("metrics");
    Some((cols.cloned(), rows.cloned(), metrics.cloned()))
}

// If the top-level JSON is an array (e.g., a list of objects), synthesize a table.
// Returns (Some(columns), Some(rows), None) or (None, None, None) if not applicable.
fn try_from_top_level_array(val: &Value) -> (Option<Value>, Option<Value>, Option<Value>) {
    match val {
        Value::Array(arr) => {
            if arr.is_empty() {
                return (None, None, None);
            }
            // Determine columns:
            // - If elements are objects, use the union of keys across all rows (sorted).
            // - If elements are scalars/arrays, map to single column named "value".
            let mut all_keys: Vec<String> = Vec::new();
            let mut is_all_objects = true;
            for el in arr {
                if let Value::Object(map) = el {
                    for k in map.keys() { if !all_keys.contains(k) { all_keys.push(k.clone()); } }
                } else {
                    is_all_objects = false;
                }
            }
            if is_all_objects && !all_keys.is_empty() {
                all_keys.sort();
                // Build rows as arrays in the order of all_keys
                let mut rows: Vec<Value> = Vec::with_capacity(arr.len());
                for el in arr {
                    if let Value::Object(map) = el {
                        let mut r: Vec<Value> = Vec::with_capacity(all_keys.len());
                        for k in &all_keys {
                            r.push(map.get(k).cloned().unwrap_or(Value::Null));
                        }
                        rows.push(Value::Array(r));
                    }
                }
                let cols: Vec<Value> = all_keys.into_iter().map(Value::String).collect();
                return (Some(Value::Array(cols)), Some(Value::Array(rows)), None);
            } else {
                // Single column fallback
                let rows: Vec<Value> = arr.iter().map(|el| Value::Array(vec![el.clone()])).collect();
                let cols = Value::Array(vec![Value::String("value".to_string())]);
                return (Some(cols), Some(Value::Array(rows)), None);
            }
        }
        _ => (None, None, None),
    }
}

fn normalize_columns(v: &Value) -> Option<Vec<String>> {
    match v {
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for c in arr {
                match c {
                    Value::String(s) => out.push(s.clone()),
                    Value::Object(map) => {
                        if let Some(Value::String(name)) = map.get("name") { out.push(name.clone()); }
                        else { out.push("col".to_string()); }
                    }
                    _ => out.push(c.to_string()),
                }
            }
            Some(out)
        }
        _ => None,
    }
}

fn normalize_rows(v: &Value) -> Option<Vec<Vec<String>>> {
    match v {
        Value::Array(arr) => {
            let mut out: Vec<Vec<String>> = Vec::with_capacity(arr.len());
            for row in arr {
                match row {
                    Value::Array(cols) => {
                        out.push(cols.iter().map(to_cell_string).collect());
                    }
                    Value::Object(obj) => {
                        // Map-like rows are flattened by value order; fall back to JSON
                        // Keep a stable order by key sort
                        let mut keys: Vec<&String> = obj.keys().collect();
                        keys.sort();
                        out.push(keys.into_iter().map(|k| to_cell_string(obj.get(k).unwrap())).collect());
                    }
                    x => {
                        // Scalar row
                        out.push(vec![to_cell_string(x)]);
                    }
                }
            }
            Some(out)
        }
        _ => None,
    }
}

fn to_cell_string(v: &Value) -> String {
    match v {
        Value::Null => String::from("NULL"),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        // keep objects/arrays compact
        other => {
            let s = other.to_string();
            // strip quotes in simple cases already handled; else leave JSON
            s
        }
    }
}

fn display_len(s: &String) -> usize { s.chars().count() }

fn build_separator(widths: &[usize]) -> String {
    let mut s = String::new();
    s.push('+');
    for w in widths {
        s.push_str(&"-".repeat(*w + 2));
        s.push('+');
    }
    s
}

fn build_row(cells: &[String], widths: &[usize]) -> String {
    let mut s = String::new();
    s.push('|');
    for (i, w) in widths.iter().enumerate() {
        let cell = cells.get(i).cloned().unwrap_or_default();
        let (text, align_right) = (truncate(&cell, *w), is_numeric_like(&cell));
        s.push(' ');
        if align_right {
            let pad = w.saturating_sub(display_len(&text));
            s.push_str(&" ".repeat(pad));
            s.push_str(&text);
        } else {
            s.push_str(&text);
            let pad = w.saturating_sub(display_len(&text));
            s.push_str(&" ".repeat(pad));
        }
        s.push(' ');
        s.push('|');
    }
    s
}

fn truncate(s: &str, max: usize) -> String {
    let len = s.chars().count();
    if len <= max { return s.to_string(); }
    if max <= 1 { return "…".to_string(); }
    let take = max - 1;
    s.chars().take(take).collect::<String>() + "…"
}

fn is_numeric_like(s: &str) -> bool {
    // crude detection for aligning numbers to right
    let st = s.trim();
    if st.is_empty() { return false; }
    let mut has_digit = false;
    for ch in st.chars() {
        if ch.is_ascii_digit() { has_digit = true; continue; }
        if ".-+eE,_".contains(ch) { continue; }
        return false;
    }
    has_digit
}

fn extract_elapsed_ms(v: &Value) -> Option<u64> {
    if let Some(ms) = v.get("elapsed_ms").and_then(|x| x.as_u64()) { return Some(ms); }
    if let Some(ms) = v.get("duration_ms").and_then(|x| x.as_u64()) { return Some(ms); }
    None
}

fn extract_elapsed_ms_from(v: Option<&Value>) -> Option<u64> {
    let m = v?;
    if let Some(ms) = m.get("elapsed_ms").and_then(|x| x.as_u64()) { return Some(ms); }
    if let Some(ms) = m.get("duration_ms").and_then(|x| x.as_u64()) { return Some(ms); }
    None
}
