# General project guidelines

Ensure no code file gets larger than about 600 lines long (python or rust). Prioritize breaking down long match statements to call functions in other modules first. 

primary interfaces should be kept thin. Large functions should be avoided.

Search for existing functions, and change from private to public and relocate them if beneficial to do so, rather than re-=implement.

Ensure unit tests are comprehensive and cover multiple levels.

Instead of minimal features, the preference will be towards fully-featured and complex scalable, performant and robust solutions for enterprise use, not MVP.

Keep query paring and execution separate and keep each DDL in separate files for each.

Add permanent debug tprintln commands to enable faster debugging. These commands will not impact release performance.

DDL error handling should not rely on bail or panics, but exit gracefully to report back to the user without
terminating the thread.

Do not use workarounds for bugs or incomplete features, instead, complete the features and fix the bugs.

Do not use shortcuts. Plan for the proper full implementation of the feature.


# Junie Polars Guidelines (Polars 0.51+)

These guidelines summarize patterns we updated while removing deprecated Polars APIs. Follow them to keep code compiling across Polars 0.51+ and future versions.

## 1) String access from Series — avoid `utf8()` and iterators

Don’t:

- `series.utf8()?.into_iter()`
- `series.utf8()?.iter()`

Do:

- Prefer index-based access via `Series::get(i)` and convert safely:

```rust
let s = match series.get(i) {
    Ok(v) => v.get_str().map(|x| x.to_string()).unwrap_or_else(|| v.to_string()),
    Err(_) => String::new(),
};
```

- If you expect numeric values, use `try_extract::<T>()`:

```rust
let n: f64 = series
    .get(i)
    .ok()
    .and_then(|v| v.try_extract::<f64>().ok())
    .unwrap_or(0.0);
```

Rationale: `Utf8Chunked::iter` / `Series::utf8()` dependent patterns are brittle across versions and feature flags. `Series::get` + `AnyValue` conversions are stable and dtype-agnostic.

## 2) Boolean masks for row filtering

Don’t:

- Build masks via deprecated iterator chains on `Utf8Chunked` or rely on old expression-only APIs when you already have a `DataFrame`.

Do:

- Build a `Vec<bool>`; wrap into a `Series<bool>`; call `DataFrame::filter`:

```rust
let mut mask: Vec<bool> = Vec::with_capacity(col.len());
for i in 0..col.len() {
    let keep = match col.get(i) {
        Ok(v) => v
            .try_extract::<i64>()
            .ok()
            .map(|n| n >= t0 && n <= t1)
            .unwrap_or_else(|| {
                // fallback: parse from string
                parse_time_to_i64(&v.to_string())
                    .map(|n| n >= t0 && n <= t1)
                    .unwrap_or(false)
            }),
        Err(_) => Some(false),
    }
    .unwrap_or(false);
    mask.push(keep);
}
let mask_series = Series::new("__mask", mask);
let df = df.filter(mask_series.bool()?)?;
```

## 3) Creating DataFrames — ensure correct column types

Don’t:

- Pass mismatched name types or rely on implicit conversions that vary between versions.

Do:

- Always pass a name convertible to `Cow<'static, str>` (e.g., `&str`, `String`, or use `.into()`), and convert `Series` to the expected type when constructing `DataFrame` rows vector:

```rust
let df = DataFrame::new(vec![
    Series::new("node_id", nodes).into(),
    Series::new("ord", ord).into(),
])?;
```

Note: In our codebase we use `.into()` to satisfy constructors expecting `Column` (type alias over `Series` in some versions). Using `.into()` keeps code portable.

## 4) Sorting with options — use `IdxSize` for `limit`

Don’t:

- Set `SortMultipleOptions.limit` with `usize` directly.

Do:

- Cast to `IdxSize` (Polars index size type):

```rust
use polars::prelude::{SortMultipleOptions, IdxSize};

let opts = SortMultipleOptions {
    descending: vec![true],
    nulls_last: vec![true],
    maintain_order: true,
    multithreaded: true,
    limit: topk.map(|k| k as IdxSize),
};
let df2 = df
    .lazy()
    .sort_by_exprs(vec![col("score")], opts)
    .collect()?;
```

## 5) Robust string parsing for vector columns (ANN examples)

Don’t:

- Assume column dtype is `Utf8`; avoid `utf8()?.iter()`.

Do:

- Extract per-row strings safely, then parse:

```rust
let ser = df.column(col_name)?;
let mut scores = Vec::with_capacity(df.height());
for i in 0..ser.len() {
    let s_owned = match ser.get(i) {
        Ok(v) => v.get_str().map(|x| x.to_string()).unwrap_or_default(),
        Err(_) => String::new(),
    };
    let v = parse_vec_literal(&s_owned).unwrap_or_default();
    scores.push(vec_l2(&v, &qvec));
}
let mut df2 = df.clone();
df2.with_column(Series::new("__ann_score", scores))?;
```

## 6) Prefer dtype-agnostic conversions for resilience

- Strings: `get(i)` then `get_str()` fallback to `to_string()`.
- Integers/floats: `try_extract::<i64>()` / `try_extract::<f64>()` with sane defaults.
- Dates/times: accept either numeric epochs or ISO8601 text, parse at the boundary.

## 7) Lazy vs eager boundaries

- For simple in-memory sorts or filters when you already hold a `DataFrame`, it’s fine to use eager `DataFrame::filter`. For multi-step transformations, collect via `LazyFrame` pipelines with explicit options to avoid deprecated implicit behaviors.

## 8) Naming conventions and temporaries

- Use unique temporary column names like `"__mask"`, `"__ann_score"`, and drop/select to original schema afterward.
- Column names: pass `&str` or `String`; `.into()` when in doubt.

## 9) Error handling

- Treat per-cell extraction failures as data nulls; avoid panics. Prefer `unwrap_or_default()` or propagate errors with `anyhow` when the failure should abort the operation.

---

Quick Before → After Reference

- String column iteration
  - Before: `for v in df.column("name")?.utf8()?.into_iter() { ... }`
  - After: `for i in 0..col.len() { let s = col.get(i)?.get_str().map(|x| x.to_string()).unwrap_or_default(); }`

- Boolean mask building
  - Before: chaining on `Utf8Chunked` or deprecated mask builders
  - After: build `Vec<bool>` → `Series::new` → `bool()?` → `DataFrame::filter(...)`

- Sorting with limit
  - Before: `SortMultipleOptions { limit: Some(topk) as Option<usize>, ... }`
  - After: `SortMultipleOptions { limit: topk.map(|k| k as IdxSize), ... }`

- DataFrame construction
  - Before: sometimes passing `Series` without consistent name types or column conversions
  - After: `DataFrame::new(vec![Series::new("name", data).into(), ...])?`

Locations already using these patterns:

- `src/server/exec/exec_graph_runtime.rs`
- `src/server/exec/select_stages/order_limit.rs`
