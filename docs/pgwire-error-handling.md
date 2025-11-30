### Pgwire error handling guidelines

This document defines conventions for robust, user-friendly error handling in the pgwire integration.

Goals:

- Never terminate the connection/task prematurely on recoverable errors.
- Convert all internal failures into protocol-compliant ErrorResponse frames followed by ReadyForQuery after Sync.
- Avoid panics in the pgwire path. Do not use `unwrap`/`expect` in request parsing, parameter decoding, or SQL substitution.
- Keep the extended query cycle semantics: once an error occurs, set `in_error = true` and continue accepting messages until `Sync`, at which point send `ReadyForQuery` and clear the error state.

Rules and practices:

- No `unwrap`/`expect` in `src/pgwire_server.rs`. Prefer `Option`/`Result` and propagate with `anyhow::bail!` or return an error up to the caller that translates to `send_error()`.
- Regex compilation and capture handling must not panic. Use `Regex::new(...)?` and `if let`/`and_then` to access captures safely.
- When decoding binary parameters, never assume lengths. Validate byte slice sizes and return a graceful error or fallback to text when possible.
- On parse/bind/execute/close/describe handlers, catch all `Result` errors and:
  - Call `send_error(socket, msg).await`;
  - Set `state.in_error = true`;
  - Return `Ok(())` to keep the connection alive; do not bubble the error to the connection loop unless the transport is broken.
- For unknown or unsupported frames, respond with `ErrorResponse` and set `in_error = true`.
- Only terminate the connection on I/O errors (socket read/write failures) or explicit `Terminate` messages.
- For Simple Query protocol, emit `ErrorResponse` on each failing statement, then continue with the next statement in the same message; finally send a single `ReadyForQuery` at the end.
- For Extended Query protocol, do not send `ReadyForQuery` until a `Sync` message is received. After `Sync`, send `ReadyForQuery` even if the previous cycle had errors.

Developer checklist for new code in pgwire path:

- [ ] No `unwrap`/`expect` in pgwire code.
- [ ] All conversions from bytes validate lengths and return `None`/`Err` instead of panicking.
- [ ] Any `bail!` is caught at the handler boundary and converted to `send_error()` with `state.in_error = true`.
- [ ] After errors in extended flow, ensure we continue reading until `Sync` and only then send `ReadyForQuery`.
- [ ] Add debug logs sufficient to diagnose issues without crashing (`pgwire` target).

Testing notes:

- Simulate malformed frames and invalid parameter formats; verify the server returns `ErrorResponse` and the connection remains usable after `Sync`.
- Execute statements that cause engine-level `bail!` and ensure pgwire responds with `ErrorResponse` instead of closing the socket.
