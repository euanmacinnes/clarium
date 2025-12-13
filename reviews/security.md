### Summary
Here’s a targeted security review of the repository you shared, with prioritized recommendations to harden HTTP/WS, pgwire, authentication/authorization, scripting, filestore, build/runtime, and supply chain. I’ve highlighted concrete implementation actions for each finding.

---

### Highest-risk findings (address first)

#### 1) Default admin credential creation (weak default)
- Finding: `security::ensure_default_admin` creates a global `clarium` user with password `"clarium"` when `db_root/user.parquet` does not exist.
- Risk: First-run environments may remain with predictable credentials if operators forget to rotate. Attackers can trivially gain admin access.
- Recommendations:
  - Remove default static password. On first run, generate a random strong password and print it once to console + write to a root-owned file with `0600` permissions; or better, require an initial admin password to be passed via environment or interactive setup.
  - Force password change on first login.
  - Provide a `--no-bootstrap-admin` flag for production images; default to “no default admin in production.”

#### 2) pgwire authentication and transport
- Findings:
  - `pgwire_server` supports a “trust mode” via `CLARIUM_PGWIRE_TRUST` (skips password auth). It also implements `AuthenticationCleartextPassword` handling, and sends a fixed “BackendKeyData secret=12345”. No TLS is negotiated in the server (it listens on plain TCP), and Docker defaults enable pgwire: `CLARIUM_PGWIRE=true` and `EXPOSE 5433`.
- Risks: Cleartext passwords over the network, predictable backend secret, and trust mode can lead to account compromise. Exposed pgwire on all interfaces enables remote attack surface by default.
- Recommendations:
  - Disable pgwire by default in production images; require explicit `CLARIUM_PGWIRE=true` to enable.
  - Remove or dev-gate trust mode; only allow when `RUST_LOG=debug` or `CLARIUM_ENV=dev` and listening on loopback.
  - Implement TLS for pgwire (rustls) or require running behind a TLS proxy. Support SCRAM-SHA-256 (RFC 5802/7677) instead of cleartext. Do not use `AuthenticationCleartextPassword` except in local dev.
  - Generate random `BackendKeyData` secret per connection; never a constant value.
  - Bind pgwire to `127.0.0.1` by default; document how to expose it safely behind mTLS or a trusted network.

#### 3) Session management hardening (HTTP)
- Findings:
  - Cookies are set `HttpOnly; Secure; SameSite=Strict; Path=/` — good. Session IDs and CSRF tokens are securely random. Sessions stored in-memory maps.
  - No session expiration, idle timeout, rotation on privilege change, or IP/UA binding observed.
- Risks: Long-lived sessions increase impact of theft; lack of rotation enables fixation risk.
- Recommendations:
  - Add absolute and idle timeouts (e.g., absolute 24h; idle 30m). Persist `issued_at`/`last_seen` per session and enforce on each request.
  - Rotate session ID at login and on privilege changes.
  - Optionally bind session to user-agent hash and recent source IP with tolerance for NAT/proxies; invalidate on anomalies.

#### 4) Brute-force and abuse protections
- Findings: No explicit rate-limiting on `/login` or other endpoints. No account lockout policy.
- Risks: Credential stuffing and password spraying against default admin or other accounts.
- Recommendations:
  - Add IP- and account-based rate limits for sensitive endpoints (e.g., sliding window: 5/min per tuple (IP, username); exponential backoff).
  - Implement temporary account lockout after N failed attempts, with jittered unlock, and a secure admin unlock path.
  - Add global request body size/time limits and concurrency bounds to mitigate DoS.

#### 5) Password hashing parameters
- Findings: Uses `argon2::Argon2::default()`. Defaults may be acceptable but are conservative for servers.
- Risk: Under-provisioned Argon2 parameters reduce cost to brute-force.
- Recommendations:
  - Move to Argon2id with explicitly tuned parameters (e.g., memory 256–512 MiB per hash, iterations t=2–3, lanes p=1–2) appropriate for your hardware, with a config knob.
  - Store algorithm and params in the PHC string (already done) and support gradual rehash on successful login when params upgrade.

---

### Medium-risk findings

#### 6) pgwire default exposure in Docker
- Findings: Dockerfile enables pgwire by default and exposes port 5433.
- Risk: Users may run public images and unintentionally expose pgwire unauthenticated or in cleartext.
- Recommendations:
  - Set `CLARIUM_PGWIRE=false` by default in container. Only expose 5433 conditionally or document strongly that it must be firewalled.
  - Provide sample `docker run` with `--network host` OFF, mapped port off by default, and a compose profile that enables pgwire only behind TLS reverse proxy.

#### 7) CSRF coverage and CORS
- Findings: CSRF tokens are validated on logout and several state-changing endpoints (`validate_csrf` is checked at multiple sites, including WS upgrade). Good. CORS defaults not clearly set.
- Risks: If any mutating endpoint lacks CSRF checks or if CORS is permissive, browser-based exploitation is possible.
- Recommendations:
  - Ensure CSRF validation wraps all state-changing endpoints (write, DDL, admin actions). Consider an Axum layer/middleware that enforces CSRF for `POST/PUT/PATCH/DELETE`.
  - Lock CORS to expected origins; block credentials from untrusted origins. Explicitly set `Access-Control-Allow-Origin` to a strict allowlist.

#### 8) WebSockets authentication
- Findings: WS upgrade path references `validate_csrf`. Good.
- Recommendations:
  - Also require a valid session on WS; reject upgrades without a session cookie match. Limit message sizes and set per-connection rate limits.

#### 9) Filestore and path security
- Findings: Filestore has an ACL layer with caching. Underlying file and git operations aren’t fully reviewed here.
- Risks: Path traversal, symlink following, or repo URL injection can lead to data exfiltration or RCE via git backends.
- Recommendations:
  - Canonicalize paths and enforce a strict chroot to a configured base dir; reject `..` components and symlinks that escape root.
  - For git features, validate remotes against an allowlist pattern (e.g., only certain domains or SSH CA).
  - Sanitize archive extraction and uploaded content types. Enforce size and type limits (MIME sniffing).

#### 10) Lua UDF sandboxing
- Findings: `mlua` with `vendored` feature is enabled; no obvious sandbox configuration in the snippets.
- Risks: Lua scripts may access OS features or loop indefinitely.
- Recommendations:
  - Run Lua in a restricted environment: remove `os`, `io`, `debug` libs; set instruction step limit/yield to cap CPU; limit memory with `mlua` memory limit hooks.
  - Provide a capability-based API surface for safe functions only.

---

### Lower-risk but important improvements

#### 11) Logging and secrets hygiene
- Findings: Uses `tracing`. Startup logs include paths and env; `tprintln!` debug hooks are present.
- Recommendations:
  - Redact secrets in logs (passwords, tokens). Never log full request bodies on auth routes.
  - Add structured audit logs for: login attempts, password changes, DDL/tenancy ops (your tenancy plan mentions this—ensure it’s implemented consistently).

#### 12) Input validation and limits
- Recommendations:
  - Set global Axum body limits (`DefaultBodyLimit`) and timeouts (read header/body timeouts). Reject oversized JSON payloads early.
  - Validate table/database names against strict regex; avoid path-influenced identifiers reaching filesystem ops.

#### 13) Authorization coverage
- Findings: `Perms` include `select/insert/calculate/delete` and `is_admin`; there’s `Scope` for Global/Database. Mapping from commands to checks exists in `server.rs`.
- Recommendations:
  - Ensure authorization is enforced for all query kinds including DDL, SHOW, TIME, GRAPH, FILESTORE, and tenancy control-plane APIs.
  - Consider role-based permissions with object-level granularity (db/schema/table) and deny-by-default. Log authorization denials with enough context.

#### 14) Filesystem permissions and storage
- Findings: User database stored in Parquet at `db_root`. Writer creates directories with default perms; no explicit OS perms.
- Recommendations:
  - Restrict file perms: `0600` for user store. Avoid world-readable data dirs. On Windows, set restrictive ACLs.
  - Consider migrating credentials to a dedicated keystore or meta store with atomic updates and journaling.

#### 15) Build and runtime hardening
- Recommendations:
  - Docker: run as non-root user, drop Linux capabilities, set `readOnlyRootFilesystem`, add basic `HEALTHCHECK`. Use `USER 65532:65532` (nonroot). Mount DB dir with correct ownership.
  - Enable seccomp/apparmor profiles and disallow `ptrace`.
  - Provide systemd unit with `ProtectHome=yes`, `PrivateTmp=yes`, `NoNewPrivileges=yes`.

#### 16) TLS for HTTP
- Findings: Axum server is plain HTTP; likely intended to sit behind a TLS terminator.
- Recommendations:
  - Document and/or implement first-class TLS via rustls for standalone deployments. Support mTLS for intra-cluster control plane when tenancy features are added.

#### 17) Supply-chain security
- Recommendations:
  - Add CI steps: `cargo audit`, `cargo deny`, SBoM generation (`cargo about`), and minimal-deps checks.
  - Pin Docker base images by digest; run `apt-get` with `--no-install-recommends` (already used) and clean apt caches (already done).
  - Use `RUSTFLAGS="-C strip=symbols"` in release builds and enable `panic=abort` if acceptable; consider enabling `-Z sanitizer` in CI for fuzz targets.

#### 18) Observability for security
- Recommendations:
  - Emit metrics for auth failures, rate-limit events, pgwire TLS usage, ACL cache hit/miss/evictions (partially present), and anomaly scores.
  - Provide a security diagnostics endpoint gated by admin role.

---

### Concrete implementation checklist

- Authentication
  - Replace default admin password with generated or env-provided, force change on first login.
  - Tune Argon2id parameters; implement “rehash if needed” on login.
  - Add brute-force rate limits and temporary lockouts.
- Sessions and CSRF
  - Add absolute and idle session timeouts; rotate IDs at login/priv-change.
  - Enforce CSRF via middleware on all non-GET/HEAD routes; tighten CORS allowlist.
- pgwire
  - Default disabled in container; bind to loopback by default.
  - Add TLS and SCRAM-SHA-256; remove trust mode or dev-gate it; randomize BackendKeyData secret.
- Filestore & Lua
  - Path canonicalization, symlink refusal, enforce base dir, remote allowlists for git.
  - Restrict Lua stdlibs, set CPU step and memory caps; expose only safe APIs.
- HTTP hardening
  - Body size/time limits; per-route limits for uploads and WS message size; per-connection rate limits.
- Build/runtime
  - Non-root container, drop caps, read-only root FS, healthcheck.
  - CI: cargo-audit/deny, SBoM, image scanning; pin base images by digest.
- Logging/Audit
  - Redact secrets; structured audit logs for admin actions and SYNC tenancy ops.

---

### Notes aligned with your project guidelines
- Avoid panics/bails in security paths: return structured errors, especially for pgwire and tenancy control plane.
- Keep interfaces thin: implement CSRF/session middleware and reusable authorization checks instead of duplicating logic per endpoint.
- Add permanent debug `tprintln!` gates only for non-sensitive metadata; never log secrets.
- Separate parsing vs execution: keep security checks at well-defined boundaries (router, coordinator) as you build tenancy features.

If you’d like, I can draft specific patches for: session expiry/rotation, Argon2 parameter tuning, CSRF middleware, pgwire SCRAM/TLS scaffolding, and Docker hardening (non-root user and entrypoint changes).