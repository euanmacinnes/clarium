### What I reviewed
I traced the SELECT execution path across the staged executor and its integrations to locate hardcoded edge cases and brittle behaviors.

Key areas inspected (read-only):
- src/server/exec/select_stages/from_where.rs
- src/server/exec/select_stages/order_limit.rs
- src/server/exec/select_stages/by_or_groupby.rs
- src/server/exec/select_stages/project_select.rs
- src/server/exec/select_stages/having.rs
- src/server/exec/where_subquery.rs
- src/pgwire_server.rs (SELECT-like interception and output)

Below is a consolidated list of hardcoded/special-cased logic, their likely impacts, and recommendations aligned with your guidelines.

### Hardcoded edge cases and brittle behaviors

1) Magic/temporary column names scattered across stages
- Examples observed:
  - from_where.rs: uses an implicit single-row "no FROM" path (hinted by logs) and special matching behaviors; also writes debug tprintln.
  - by_or_groupby.rs: temporary columns like `"__tmp__"`, `"__udf_temp_<alias>"`, `"__argN"`, and sentinel key `"__ALL__"` for global aggregates.
  - order_limit.rs: uses `"__ann_score"` as a computed score column and depends on presence of `"__row_id"` or `"alias.__row_id"` as stable tiebreakers.
- Risks: Name collisions with user columns, leakage of temporaries if not consistently dropped, coupling between stages via magic strings.
- Recommendations:
  - Centralize all internal column names in a `constants` module (e.g., `internal::cols::{ANN_SCORE, ROW_ID, TMP, ARG_PREFIX, ALL_GROUP_KEY}`) and consume via functions.
  - Guarantee automatic drop of temporaries at the end of each stage; prefer scoped `LazyFrame` selects to avoid polluting the working `DataFrame`.
  - Add a prefix policy and validation guard (e.g., forbid user-created columns beginning with `"__"` unless explicitly quoted) or implement transparent renaming/shadowing.

2) ANN ORDER BY heuristics and mixed responsibilities in order_limit.rs
- Lines ~21–120, 430–850:
  - Hardcoded parsing for ANN expressions (e.g., `vec_l2(col, [..])`, `cosine_sim`) with a best-effort RHS scalar evaluation.
  - Filesystem traversal for `.vindex` discovery under the store root to diagnose/attempt ANN: scans `db/schema/*.vindex` directly.
  - Magic `"__ann_score"` column used for sorting and tie-breaking logic with optional `"__row_id"`.
  - Silent tolerance for ORDER BY keys not in projection when a global flag `system::get_strict_projection()` is false (skips keys instead of erroring).
- Risks: Tightly-coupled FS discovery (bypasses catalog), implicit behavior changes via global flags, and reliance on magic names.
- Recommendations:
  - Route index discovery through a catalog API (table/column → index metadata) instead of FS scanning.
  - Encapsulate ANN ORDER BY planning in a dedicated planner module that returns a typed plan (exact vs ANN with candidates), keeping the ORDER/LIMIT stage thin.
  - Use stable constants for internal columns and drop them after sort.
  - Prefer explicit diagnostics returned to the client rather than silent skip of ORDER BY keys in non-strict mode; at minimum emit a warning via tracing.

3) Stable row-id detection is ad-hoc
- order_limit.rs lines ~465–481: heuristics to find `"__row_id"` or `"alias.__row_id"`; mentions a legacy form `"__row_id.<alias>"`.
- Risks: Fragile tie-breaking and inconsistent behavior across projections/aliases.
- Recommendations:
  - Add a uniform row-id resolution API in `DataContext` (e.g., `ctx.resolve_row_id(&df) -> Option<String>`) backed by table metadata; eliminate legacy patterns.

4) WHERE mask computed twice and UDF validation is manual
- from_where.rs ~405–415: builds mask via `eval_where_mask` twice (once to count kept rows, again to filter). This doubles compute for complex predicates.
- Same file ~365–404: manual AST walk to collect UDF names; bails if missing in the registry.
- Risks: Unnecessary overhead; duplicate logic vs parser; potential drift if WHERE grammars expand.
- Recommendations:
  - Compute mask once; reuse for stats and filter. Provide a single helper returning both mask and keep-count if needed.
  - Centralize UDF resolution during parsing/planning (capture UDF calls in the AST with resolved symbols) and have executor trust the plan.

5) JOIN behavior has hard stop on certain forms
- from_where.rs ~344–355: `RIGHT/FULL JOIN with pure non-equi conditions requires at least one equality in ON clause` → immediate bail.
- Risks: Hardcoded limitation presented as a runtime error; no planner hint or partial support path.
- Recommendations:
  - Move validation to planning with clear error codes and documentation, or implement a general nested-loop join fallback with guards for performance.

6) Case-insensitive manual column resolution sprinkled around
- order_limit.rs ~457–499 resolves columns by case-insensitive matching against `df.get_column_names()`; similar patterns likely elsewhere.
- Risks: Ambiguity if two columns differ only by case; inconsistency vs SQL identifier semantics.
- Recommendations:
  - Centralize name resolution in `DataContext` with deterministic rules (SQL standard quoted vs unquoted identifiers), and use it uniformly.

7) SHOW/CURRENT_USER and SELECT-like handling hardcoded in pgwire_server.rs
- pgwire_server.rs ~436–447: Treats `SHOW` and `SELECT CURRENT_USER` via special-case paths; responds with fabricated rows.
- pgwire_server.rs ~419–426: Naive semicolon splitter; comment admits it doesn’t handle semicolons in quoted strings.
- pgwire_server.rs ~435: `upper.starts_with("SELECT") || upper.starts_with("WITH ") || upper.starts_with("SHOW ")` (first 32 chars only) to decide SELECT-like flow.
- Risks: Protocol behavior diverges from PostgreSQL spec; parsing errors on complex statements; magic behavior for CURRENT_USER.
- Recommendations:
  - Delegate statement classification to the SQL parser; the pgwire layer should be thin: parse → route by AST type.
  - Implement meta-commands (e.g., CURRENT_USER) through normal SELECT function support or a catalog-backed function.
  - Replace ad-hoc semicolon splitting with the parser’s multi-statement support or a robust tokenizer.

8) Global flags change semantics implicitly
- `system::get_strict_projection()`, ANN tuning like `get_vector_preselect_alpha()` and `get_vector_ef_search()` alter behavior deep in executor.
- Risks: Hidden behavior changes across environments/tests; hard to reason about.
- Recommendations:
  - Surface these as planner options stored in the query plan (immutable during execution), with defaults injected at the planning boundary.

9) Parser/executor contract mixes responsibilities
- order_limit.rs re-parses ORDER BY expressions for ANN detection because the executor lacks structured expression nodes for ORDER BY (`q.order_by_raw` used as a fallback).
- Risks: String-parsing is brittle across quoting and aliases; duplicated logic.
- Recommendations:
  - Ensure the parser populates structured ORDER BY expression AST (including function calls and RHS literals). Executor should consume typed nodes only.

10) Potential DFS over store filesystem during ANN candidate selection
- order_limit.rs ~509–526: reads directories under the store root to find `.vindex` files for the selected table/column.
- Risks: IO-heavy, unbounded traversal, bypasses security/capsulation.
- Recommendations:
  - Replace with catalog lookups; the store should provide an API to enumerate indexes per table/column.

11) Legacy compatibility special-cases noted in comments
- order_limit.rs mentions `legacy: __row_id.<alias>`; project_select.rs references parity with legacy in HAVING.
- Risks: Hidden legacy modes with minimal documentation.
- Recommendations:
  - Isolate legacy compatibility shims behind feature flags or planner rewrites with tests documenting the behavior.

### Alignment with your Polars and project guidelines
- Observed code often follows your updated Polars patterns (building masks as `Vec<bool>`, dtype-agnostic conversions, `IdxSize` casting, temp columns like `"__ann_score"`). Keep this consistent across all stages.
- Add permanent `tprintln` diagnostics are already present; consider standardizing tags/targets and ensuring they’re no-ops in release.

### Concrete next steps (no code changes made)
- Planning refactors (thin interfaces, no giant functions):
  - Create `internal::constants` for temp/sentinel names; migrate references across stages.
  - Add `DataContext` helpers: `resolve_column(...)`, `resolve_row_id(...)`, `drop_temporaries(df)`.
  - Introduce an `AnnOrderPlanner` that returns a typed plan, separates index discovery (via catalog) from execution, and avoids FS scans.
  - Parser work: ensure ORDER BY expressions are structured; capture UDF symbols at parse-time.
  - Pgwire: route through parser for statement classification; remove ad-hoc CURRENT_USER handling in favor of functions/catalog.

- Test coverage to add (separate test files, not inline):
  - Temp column collision tests (user column named `__ann_score`, `__tmp__`, etc.)
  - ORDER BY with non-projected columns under strict vs non-strict modes (explicit warnings expected in non-strict).
  - ANN ORDER BY with/without indexes; verify no FS traversal by mocking catalog.
  - JOIN validations: RIGHT/FULL non-equi error path vs fallback implementation if added.
  - WHERE mask single-evaluation correctness and performance sanity.
  - Case-sensitive vs insensitive column resolution behavior.
  - Pgwire multi-statement parsing with semicolons in strings; CURRENT_USER via SELECT function.


### Plan to proceed with deterministic column resolution and no-duplicate outputs

#### Scope of this PR series (Phase 1–2)
- Phase 1 (core infra + SELECT/ORDER BY wiring):
  1) Introduce `internal::constants` for reserved/internal column names and prefixes.
  2) Add `NameResolverSpec` and `ProjectionContract` to planner/ctx.
  3) Wire SELECT projection stage to consume `ProjectionContract` and enter output-id mode.
  4) Replace ad-hoc resolution in ORDER BY with centralized resolver; allow temporary sort columns without leaking to output.
  5) Tests: column resolution, star expansion, duplicates suppression, ORDER BY by non-projected keys.
- Phase 2 (GROUP BY/HAVING/WHERE + UDF integration):
  6) Apply resolver to GROUP BY and HAVING; ensure keys don’t leak to output unless selected.
  7) Centralize WHERE/UDF symbol validation at planning, compute mask once.
  8) Extend CTE scope resolution and alias rules uniformly.
  9) Tests: GROUP BY/HAVING clause resolution, UDF columns, CTE behaviors.

---

### Phase 1 — Detailed steps
1) Internal constants module
- Add `src/server/exec/internal/constants.rs`:
  - `INTERNAL_PREFIX: &str = "__"`
  - `ROW_ID: &str = "__row_id"`
  - Temporary names: `ANN_SCORE`, `MASK`, etc.
- Replace string literals across stages (initially in ORDER/LIMIT and DataContext helpers) with constants.

2) NameResolverSpec (ctx-level)
- In `DataContext` add structure capturing resolution policy at plan time:
  - Visible sources and their aliases
  - Case folding rules for quoted/unquoted identifiers
  - Hidden/internal columns policy (exclude from `*` by default)
- Provide methods:
  - `resolve_column(df, name) -> Result<String>` (already exists)
  - `resolve_column_at_stage(df, name, stage) -> Result<String>` (exists — keep)
  - `resolve_row_id(df) -> Result<Option<String>>` (done)
  - `is_internal(name: &str) -> bool`

3) ProjectionContract (planner → executor)
- Create struct persisted in `Query` plan:
  - `final_order: Vec<FinalCol>` where `FinalCol { final_name: String, producer: ProducerId }`
  - `expanded_items` snapshot for `*` and `t.*` (fully enumerated at plan time)
  - `hidden_allowed: HashSet<String>` for temporary columns permitted during execution
- Construction rules:
  - Expand `*` based on FROM sources left-to-right; exclude internal names.
  - Expand `t.*` only for that alias; exclude internal names.
  - De-duplicate: explicit SELECT items override expanded collisions.
  - For collisions between expanded items from multiple tables, qualify display names deterministically (`t.c`) unless user aliased.

4) Project stage integration
- `project_select.rs` will:
  - Build DataFrame strictly in the order of `ProjectionContract.final_order`.
  - Register user columns for finalization.
  - Enter output-id mode (`ctx.enter_output_id_mode`) immediately after projection.

5) ORDER/LIMIT integration
- Use resolver for all key resolutions (already partially in place).
- Allow keys not in projection: add temporary columns for evaluation; ensure they’re dropped before finalization.
- Keep ANN exploration confined; all temporary `__ann_score` dropped at final.

6) Finalization
- At the very end (`exec_select.rs`), run:
  - `enter_output_id_mode` (already done post-projection) → other stages operate on ids
  - `finalize_output_names` to:
    - Drop internal columns (e.g., `__row_id`, temporary sorts)
    - Use contract for final display names and order

7) Tests (new dedicated files)
- `exec/tests/column_resolution_star.rs`:
  - `SELECT *` over two tables with same column names → final outputs contain qualified names, no duplicates
  - `SELECT t1.*, t2.*` with aliases and an explicit override `SELECT id as my_id, *` where `id` exists in `t1` → `my_id` preserved and no duplicate raw `id`
- `exec/tests/column_resolution_order.rs`:
  - ORDER BY unprojected key: sorted output, no extra column leaked
  - ORDER BY with alias reference when allowed by dialect
- `exec/tests/udf_projection.rs`:
  - UDF column with implicit alias gets deterministic name; temporary subcolumns removed

---

### Phase 2 — Detailed steps
8) GROUP BY and HAVING
- Resolve keys via resolver; permit grouping on expressions not in projection without leaking columns.
- Ensure final output is governed only by `ProjectionContract`.

9) WHERE mask optimization and UDF symbol capture
- Move UDF presence validation to planner based on parsed AST.
- Compute mask once and reuse for both stats and filtering.

10) CTE scope and alias rules
- Ensure CTE-introduced tables integrate into resolver scope with correct priority.

11) Tests (additional)
- Ambiguity errors for unqualified names when multiple matches exist.
- Quoted identifier behavior for mixed-case names across clauses.

---

### Dialect choices to confirm
- Should ORDER BY allow SELECT-list aliases? Default: enable.
- For `SELECT *` across multiple tables, prefer qualified display names (e.g., `t1.id`, `t2.id`) to avoid duplicates. Confirm this as default.

---

### Build & verification
- Use `cargo check` after Phase 1 edits; then `cargo build`.
- Run the new tests after all Phase 1 steps are complete; Phase 2 tests when expanded support is merged.

---

### Rollout plan
- PR1: constants + resolver tweaks + ProjectionContract skeleton + project_select wiring + ORDER BY resolver; include Phase 1 tests.
- PR2: GROUP BY/HAVING/WHERE integration, UDF symbol capture at planner, mask single-eval; include Phase 2 tests.

I’ll start with PR1 implementation unless you want to adjust dialect defaults (ORDER BY aliasing and `*` display-name qualification).

### Plan created for the current issue

#### Scope overview
Continue Phase 1 refactor to centralize internals and thin SELECT stages; then implement resolver + projection contract, and proceed into Phase 2 (GROUP BY/HAVING/WHERE + UDFs, mask single‑eval). Keep behavior stable until finalization wiring is in place. Follow Polars 0.51+ patterns and permanent `tprintln` diagnostics.

### Plan (phased)

1) Internal constants rollout and hygiene (no behavior change)
- Replace remaining magic strings with `internal::constants` in: `having.rs` and any strays in SELECT stages and helpers.
- Ensure constants cover: masks, ANN score, window/order temps, UDF temp args, join trackers, unit row.
- Confirm Polars usage: `Series::get(i)`, `try_extract`, boolean mask via `Vec<bool>` → `Series` → `DataFrame::filter`, `IdxSize` for sort limits.

2) Helper: temporary lifecycle
- Finalize `internal::utils::drop_internal_columns(df)` usage: wire at stage exits to prevent temp leakage (FromWhere, By/Group, Project, OrderLimit, Having). Keep behavior by dropping only `is_internal` columns.
- Standardize `tprintln` tags on stage enter/exit for traceability.

3) Name resolution foundation (DataContext)
- Introduce `NameResolverSpec` (aliases, case rules, internal name policy).
- Unify `resolve_column` and `resolve_column_at_stage` semantics and error messages; keep `resolve_row_id` based on `ROW_ID` with scope preferences.

4) ProjectionContract (+ thin Project stage)
- Define `ProjectionContract` with `final_order`, expanded `*`/`alias.*`, and `hidden_allowed` for temporary/sort columns.
- Wire `project_select.rs` to emit columns strictly in `final_order` and enter output‑id mode early. Support ORDER BY referencing SELECT‑list aliases (confirmed).

5) ORDER/LIMIT integration with resolver
- Replace ad‑hoc resolution with centralized resolver.
- Allow ORDER BY on non‑projected keys via temporary eval columns; ensure they are dropped before finalization.
- Keep ANN internals scoped; use `ANN_SCORE`; never leak to output.

6) Finalization step
- Implement `finalize_output_names`: drop internal columns; apply `ProjectionContract` display names and deterministic order; suppress duplicates deterministically.

7) Tests — Phase 1 (dedicated files)
- `exec/tests/column_resolution_star.rs`: `SELECT *` across two tables with overlapping columns → qualified, no duplicates.
- `exec/tests/column_resolution_order.rs`: ORDER BY unprojected key allowed; output sorted; no temp leakage.
- Extend `qualified_naming_tests.rs` for alias precedence and ambiguity errors.

8) Build & verify
- `cargo check` → `cargo build` for incremental edits.
- Run new tests; fix regressions.

9) Phase 2 — GROUP BY/HAVING/WHERE + UDFs, mask optimization
- Apply resolver to GROUP BY/HAVING; ensure keys don’t leak to outputs.
- Validate UDF symbols during planning from AST; remove manual scans in executor.
- Compute WHERE mask once; add helper returning `(mask, kept_count)` for reuse.

10) Tests — Phase 2
- Grouping/HAVING resolution, UDF presence errors, WHERE mask single‑eval correctness; quoted identifiers and ambiguity behaviors.

11) Future cleanup (follow‑up PRs)
- ANN: replace filesystem scans with catalog index discovery; move ANN planning to dedicated module returning typed plans.
- Pgwire: route classification through parser; remove ad‑hoc CURRENT_USER handling; robust multi‑statement parsing.

#### Progress markers
- 1: in progress (most constants migrated; finish `having.rs` + sweep)
- 2: pending (wire drops at stage exits)
- 3: pending (scaffolding)
- 4: pending
- 5: pending
- 6: pending
- 7: pending
- 8: pending
- 9: pending
- 10: pending
- 11: pending

If you want any step reordered (e.g., write tests first), tell me and I will adjust immediately.


