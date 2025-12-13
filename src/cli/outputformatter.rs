use serde_json::Value;

use terminal_size::{Width, Height, terminal_size};

// Render query results as an ASCII table.
// Returns true if a table was printed (i.e., detected rows/columns), false otherwise.
pub fn print_query_result(val: &Value) -> bool {
    // Header label
    println!("query results:");
    // Honor env override to force JSON output
    if std::env::var("CSQL_OUTPUT").map(|v| v.eq_ignore_ascii_case("json")).unwrap_or(false) {
        return false;
    }

    // Try common shapes
    let (cols_opt, rows_opt, metrics_opt) =
        extract_table(val)
            .or_else(|| { // sometimes directly under top-level
                let cols = val.get("columns").cloned();
                let rows = val.get("rows").cloned();
                let metrics = val.get("metrics").cloned();
                // Only treat as a table if at least columns or rows exist; otherwise, allow fallback
                if cols.is_some() || rows.is_some() { Some((cols, rows, metrics)) } else { None }
            })
            .unwrap_or_else(|| try_from_top_level_array(val));

    let cols_v = match cols_opt { Some(v) => v, None => return false };
    let rows_v = match rows_opt { Some(v) => v, None => return false };
    let cols = match normalize_columns(&cols_v) { Some(v) => v, None => return false };
    let rows = match normalize_rows(&rows_v) { Some(v) => v, None => return false };

    // If there are no rows, stick to JSON as per requirement (only print table when rows are returned)
    if rows.is_empty() { return false; }

    // Detect terminal width once for this rendering
    let termw = get_terminal_width();
    crate::tprintln!("[cli.outputformatter] detected terminal width={} columns", termw);

    let mut widths: Vec<usize> = cols.iter().map(|s| s.len().min(termw)).collect();
    for r in &rows {
        for (i, cell) in r.iter().enumerate().take(cols.len()) {
            let w = display_len(cell);
            if w > widths[i] { widths[i] = w.min(termw); }
        }
    }

    // Header
    let sep = build_separator(&widths);
    println!("{}", fit_line_to_width(&sep, termw));
    let header = build_row_header_colored(&cols, &widths);
    println!("{}", fit_line_to_width(&header, termw));
    println!("{}", fit_line_to_width(&sep, termw));
    // Rows
    for r in &rows {
        let line = build_row(r, &widths);
        println!("{}", fit_line_to_width(&line, termw));
    }
    println!("{}", fit_line_to_width(&sep, termw));

    // Footer summary
    let rows_count = rows.len();
    let cols_count = cols.len();
    let mut summary = format!("rows: {}, cols: {}", rows_count, cols_count);
    if let Some(ms) = extract_elapsed_ms(val).or_else(|| extract_elapsed_ms_from(metrics_opt.as_ref())) {
        summary.push_str(&format!(", elapsed_ms: {}", ms));
    }
    println!("{}", fit_line_to_width(&summary, termw));

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

fn display_len(s: &String) -> usize { visible_len(s) }

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
            let pad = w.saturating_sub(visible_len(&text));
            s.push_str(&" ".repeat(pad));
            s.push_str(&text);
        } else {
            s.push_str(&text);
            let pad = w.saturating_sub(visible_len(&text));
            s.push_str(&" ".repeat(pad));
        }
        s.push(' ');
        s.push('|');
    }
    s
}

// Build header row with column names colored green. Keep padding based on visible width.
fn build_row_header_colored(cells: &[String], widths: &[usize]) -> String {
    let mut s = String::new();
    s.push('|');
    for (i, w) in widths.iter().enumerate() {
        let cell = cells.get(i).cloned().unwrap_or_default();
        let text = truncate(&cell, *w);
        let colored = format!("\x1b[32m{}\x1b[0m", text); // green
        s.push(' ');
        // headers left-aligned
        s.push_str(&colored);
        let pad = w.saturating_sub(visible_len(&text));
        s.push_str(&" ".repeat(pad));
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

// --- Terminal fitting & ANSI helpers ---

fn get_terminal_width() -> usize {
    let size = terminal_size();
    if let Some((Width(w), Height(_h))) = size {
        return (w-4) as usize;
    }
    
    return 80;    
}

fn fit_line_to_width(s: &str, maxw: usize) -> String {
    let vlen = visible_len(s);
    if vlen <= maxw { return s.to_string(); }
    elide_middle_preserving_ansi(s, maxw)
}

fn visible_len(s: &str) -> usize {
    // Count visible Unicode chars, skipping ANSI escape sequences
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut count = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1B { // ESC
            // Skip CSI: ESC [ ... letter
            i += 1;
            if i < bytes.len() && bytes[i] == b'[' {
                i += 1;
                while i < bytes.len() {
                    let b = bytes[i];
                    i += 1;
                    if (b as char).is_ascii_alphabetic() { break; }
                }
            }
            continue;
        }
        // advance by one UTF-8 char
        let ch = s[i..].chars().next().unwrap();
        count += 1;
        i += ch.len_utf8();
    }
    count
}

fn elide_middle_preserving_ansi(s: &str, maxw: usize) -> String {
    if maxw <= 3 { return "…".repeat(maxw.min(1)); }
    let need = visible_len(s);
    if need <= maxw { return s.to_string(); }
    let budget = maxw.saturating_sub(3);
    let front_keep = budget / 2;
    let back_keep = budget - front_keep;

    // Tokenize into (is_ansi, text, visible_len)
    #[derive(Clone)]
    struct Tok { ansi: bool, text: String, vis: usize }
    let mut toks: Vec<Tok> = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1B { // ESC sequence
            let start = i;
            i += 1;
            if i < bytes.len() && bytes[i] == b'[' {
                i += 1;
                while i < bytes.len() {
                    let b = bytes[i];
                    i += 1;
                    if (b as char).is_ascii_alphabetic() { break; }
                }
            }
            let end = i;
            toks.push(Tok { ansi: true, text: s[start..end].to_string(), vis: 0 });
            continue;
        }
        // collect a run of non-ANSI text
        let start = i;
        while i < bytes.len() && bytes[i] != 0x1B {
            let ch = s[i..].chars().next().unwrap();
            i += ch.len_utf8();
        }
        let text = &s[start..i];
        let vis = text.chars().count();
        toks.push(Tok { ansi: false, text: text.to_string(), vis });
    }

    // Build front
    let mut front = String::new();
    let mut collected = 0usize;
    for t in &toks {
        if t.ansi {
            front.push_str(&t.text);
        } else if collected + t.vis <= front_keep {
            front.push_str(&t.text);
            collected += t.vis;
        } else {
            // take partial
            let need = front_keep.saturating_sub(collected);
            if need > 0 {
                let mut taken = 0;
                for ch in t.text.chars() {
                    if taken >= need { break; }
                    front.push(ch);
                    taken += 1;
                }
            }
            break;
        }
    }

    // Build back
    let mut back = String::new();
    let mut collected = 0usize;
    let mut ri = toks.len();
    while ri > 0 {
        ri -= 1;
        let t = &toks[ri];
        if t.ansi {
            back = format!("{}{}", t.text, back);
        } else if collected + t.vis <= back_keep {
            back = format!("{}{}", t.text, back);
            collected += t.vis;
        } else {
            let need = back_keep.saturating_sub(collected);
            if need > 0 {
                let mut buf = String::new();
                let total = t.text.chars().count();
                let skip = total.saturating_sub(need);
                let mut idx = 0;
                for ch in t.text.chars() {
                    if idx >= skip { buf.push(ch); }
                    idx += 1;
                }
                back = format!("{}{}", buf, back);
            }
            break;
        }
    }

    let mut out = String::new();
    out.push_str(&front);
    out.push_str("...");
    out.push_str(&back);
    // Ensure color reset at end to avoid bleed if an escape was truncated
    out.push_str("\x1b[0m");
    out
}
