
### Proposed Full Plan (phased)
1) Type system and parsing
- Add `SqlType::Array(Box<SqlType>)` to `query_common.rs`. ✓
- Extend `parse_type_name` to detect `[]` suffix (e.g., `int4[]`, `text[]`, `timestamp[]`) and produce `SqlType::Array(inner)`. Handle nesting once 1‑D stabilizes. ✓
- Add `ARRAY[...]` literal parsing and simple `array[expr, ...]` constructor; 1‑D only in first pass. ✓

2) Mapping to Polars
- Extend `exec_common.rs` cast mapping from `SqlType::Array(inner)` to `DataType::List(map(inner))` including date/time/decimal per `oids.rs`. ✓
- Ensure inference and expression evaluation preserve `List` types; keep large match statements thin by delegating element casting to helpers. ✓

3) Expressions and functions
- Implement minimal array ops: `array_length(arr, 1)`, subscript `arr[i]` (1‑based), concatenation `arr1 || arr2`, and `unnest(arr)` TVF. ✓
- Bridge to existing Lua UDFs (`string_to_array`, `array_to_string`, `regexp_split_to_array`). ✓

4) Pgwire parameter and result handling
- Add parsing of array parameter types in `parse.rs` when user casts `$1::int4[]`, `$1::text[]`, etc.; map to array OIDs using `inline.rs` helpers. ✓
- Implement array parameter decoding in Bind (text `'{}'` and binary), producing `AnyValue::List` of correct element `AnyValue`s. ✓
- Ensure text-mode output formats arrays in PG brace notation when needed. ✓

5) DDL and catalog
- Allow `CREATE TABLE ... col int4[]` (internal mapping to `List(Int32)`). ✓
- Ensure schema serialization/deserialization roundtrips. ✓

6) Tests (dedicated files)
- Parser tests: type names with `[]`, `ARRAY[...]` literals, casts. 
- Exec tests: array_length, indexing, concat, unnest; mixed nulls; dtype coercions; error boundaries (no panics). 
- Pgwire tests: parameter binding (text/binary), result roundtrip for all supported inner types. 
- DDL tests: create/insert/select arrays. 
- Compare behavior to vectors where applicable (dtype is `List(Float64)` vs generic `List(T)`). 

7) Diagnostics and error handling
- Add `tracing::debug!` markers around array parsing, casting, and pgwire paths. 
- Graceful errors (no panics), aligned with guidelines. 

8) Performance considerations
- Use index-based `Series::get` and `AnyValue` conversions; avoid deprecated `utf8()?.iter()` patterns (Junie Polars guidelines). 
- Keep array encode/decode helpers thin and isolated for maintainability.
