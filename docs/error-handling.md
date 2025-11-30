### Unified error handling across HTTP, WebSocket, and pgwire

This document summarizes the common error model used throughout clarium and how errors are mapped for each frontend. The goals are:

- No panics or task crashes on user/engine errors; keep connections alive where appropriate.
- Clear, structured error responses with consistent fields.
- Appropriate protocol mappings (HTTP status, pgwire SQLSTATE/severity).

#### Common error model

Code: `src/error.rs`

- `AppError` variants:
  - `UserInput { code, message }`
  - `NotFound { code, message }`
  - `Conflict { code, message }`
  - `Auth { code, message }`
  - `Csrf { code, message }`
  - `Ddl { code, message }` — DDL/user mistakes
  - `Exec { code, message }` — execution/runtime
  - `Io { code, message }`
  - `Internal { code, message }`

Helpers:
- `http_status()` maps to HTTP status codes (e.g., `Exec` → 422, `Ddl` → 400, `NotFound` → 404, `Internal` → 500).
- `pgwire_fields()` maps to `(SQLSTATE, severity, message)` (e.g., `NotFound` → `42P01`).

#### HTTP mapping

- Success: `200` with JSON `{"status":"ok", "results": ...}`
- Error (preferred shape): `{ "status":"error", "code":"<code>", "message":"<message>" }`
- Status codes:
  - Use `AppError::http_status()` when an `AppError` is returned.
  - Otherwise, default to `422 Unprocessable Entity` for exec/semantic failures.
- Panic guard: unexpected panics are caught and converted into `500` with `{ "status":"error", "code":"internal_panic", "message":"internal server error" }`.

#### WebSocket mapping

- Success frames: `{ "status":"ok", "results": ... }`
- Error frames: `{ "status":"error", "code":"<code>", "message":"<message>" }`
- Keep the socket open on exec/user errors; only close on I/O or explicit client close.
- Panic guard: panics are caught; send one `{code:"internal_panic"}` frame and continue best-effort.

#### Pgwire mapping (PostgreSQL wire protocol)

- On engine/user errors, send `ErrorResponse` with fields from `pgwire_fields()` (`SQLSTATE`, severity, message); keep the connection alive and rely on `Sync` to clear error state.
- Only close on I/O errors or explicit `Terminate`.
- Never use `unwrap/expect` in the pgwire path; validate all lengths and parse results.

#### DDL and exec guidelines

- DDL errors should be categorized under `AppError::Ddl` with actionable messages; never panic.
- User mistakes (bad identifiers, invalid options) → `UserInput` or `Ddl`.
- Engine/runtime conditions (e.g., compute failures) → `Exec`.
- Missing objects → `NotFound` (unless `IF EXISTS` handling applies).

#### Examples

HTTP JSON:
```
{"status":"error","code":"exec_error","message":"ORDER BY column 'z' does not exist in the result set"}
```

WS frame:
```
{"status":"error","code":"ddl_error","message":"CREATE TABLE cannot target a .time table"}
```

Pgwire `ErrorResponse` (conceptual):
- Severity: `ERROR`
- SQLSTATE: `42P01` (undefined_table)
- Message: `relation "public.missing" does not exist`

#### Operational notes

- Enable `RUST_BACKTRACE=1` when diagnosing panics; guards ensure clients get a 500/structured error instead of losing the connection.
- Use the `pgwire` tracing target (`CLARIUM_PGWIRE_TRACE=1`) to inspect wire-level interactions without risking panics.
