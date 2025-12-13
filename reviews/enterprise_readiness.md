### Executive Summary
Your current stack is promising for an internal service, but modern enterprise IT approval will require significant upgrades in identity, transport security, secrets/keys, auditability, container hardening, SDLC supply‑chain security, and resilience. The highest risks observed:
- Default admin user provisioned with a default password stored in local Parquet (`src/security.rs::ensure_default_admin("clarium")`).
- Custom auth with cleartext password exchange in pgwire path (`src/pgwire_server/security.rs::AuthenticationCleartextPassword`) and no in‑app TLS; Docker image exposes ports without TLS termination.
- Local user/perm store in Parquet files without KMS envelope encryption or rotation; no account lockout/MFA.
- Container runs as root; no drop‑caps or read‑only filesystem in Dockerfile.
- Limited audit logs and no SIEM export; no policy engine (OPA) for fine‑grained authorization.

Below is a prioritized, actionable enterprise readiness plan.

---

### Must‑Have Recommendations (Blockers for Enterprise Adoption)
#### 1) Identity & Access Management (SSO + Strong Auth)
- Integrate SSO using OIDC (preferred) or SAML2. Provide a standard OAuth2/OIDC Authorization Code flow for the HTTP UI/API.
- Replace/augment local Parquet user store with:
  - External IdP (Azure AD, Okta, Ping) as the source of identity and MFA.
  - Local RBAC/ABAC mapping layer only (roles/groups synchronized from IdP claims via `groups`/`roles` claim).
- For programmatic access, support service accounts with OAuth2 client credentials or PATs that are time‑scoped and revocable.
- For pgwire, require TLS and a modern auth mechanism (SCRAM‑SHA‑256). Disable `AuthenticationCleartextPassword` in production.
- Enforce password policies and account lockout for any fallback local users. Remove default password creation; require admin bootstrap via one‑time secret.

#### 2) Transport Security Everywhere
- Terminate TLS at an enterprise ingress (e.g., NGINX Ingress, Envoy, ALB/NLB with TLS). Support mTLS for intra‑service calls in zero‑trust networks.
- Provide first‑class TLS configuration: modern ciphers, TLS 1.2+ minimum, certificate rotation via ACME or enterprise PKI.
- For pgwire (5433), require TLS with client/server certs; offer SCRAM and optional mTLS.

#### 3) Secrets and Key Management
- Remove secrets from code and environment defaults. Load from enterprise secret stores: Azure Key Vault, AWS Secrets Manager, HashiCorp Vault, or Kubernetes Secrets + SealedSecrets.
- Implement envelope encryption for any at‑rest sensitive data (user tables, WAL, delta logs). Use cloud KMS (AWS KMS, Azure Key Vault, GCP KMS) with periodic key rotation policies.

#### 4) Authorization Model Hardened
- Implement RBAC with least privilege and optional ABAC via an embedded policy engine (e.g., OPA/cedar policies) for table/schema/database operations.
- Introduce resource‑scoped policies for multi‑tenant isolation (DB‑, schema‑ or row‑level policies). Enforce at planner/executor boundaries.

#### 5) Audit, Logging, and Monitoring
- Emit structured logs (JSON) with user, session, statement text, resource, decision (allow/deny), and timing. Route to SIEM (Splunk, Sentinel, QRadar).
- Create an immutable audit trail for security events: logins, role changes, data access to regulated domains.
- Add metrics (Prometheus/OpenTelemetry) for authentication failures, latency, error rates, WAL/delta health.

#### 6) Container and Runtime Hardening
- Run as non‑root; add `USER 10001` (or similar), `readOnlyRootFilesystem: true`, drop Linux capabilities, and set a strict seccomp profile.
- Pin minimal base images (distroless or Wolfi/Chainguard); verify with SBOMs.
- Health/readiness probes; resource quotas/limits; network policies to restrict egress.

#### 7) SDLC & Supply‑Chain Security
- Add CI checks: `cargo audit`, `cargo deny`, SAST (CodeQL/Semgrep), secret scanning, license policy.
- Produce SBOM (CycloneDX/SPDX) and sign containers (Sigstore cosign). Enforce provenance (SLSA‑L3 where possible).
- Dependabot/Renovate for dependency updates with policy gates.

#### 8) Backup, DR, and Data Durability
- Define RPO/RTO targets. Implement automated encrypted backups of Parquet/WAL/graphstore and validate via periodic restore drills.
- WAL/Delta logs: fsync policies, log rotation, corruption detection, and recovery validation.

---

### Should‑Have Recommendations (Strongly Recommended)
- Rate limiting and DoS protection at the edge (WAF with managed rules; per‑user and per‑endpoint quotas).
- Data classification and masking: PII detection, masking functions, and query‑time redaction policies.
- Multi‑tenant guardrails: namespace isolation, per‑tenant resource limits, storage quotas.
- DLP hooks to block exfiltration (sized results, unusual query patterns, exporting to external endpoints).
- Feature flags and config profiles to disable risky endpoints in regulated environments (e.g., disabling Lua UDFs unless explicitly enabled with sandboxing).

### Could‑Have Enhancements
- Session recording for admin actions; tamper‑evident audit storage (WORM/S3 Object Lock).
- Row‑level security (RLS) primitives integrated with claims.
- Data lineage emission for enterprise catalogs (OpenLineage).

---

### Findings Mapping to Your Codebase
- `src/security.rs`
  - Creates default admin with password "clarium" and stores credentials in Parquet. Replace with IdP bootstrap flow and harden local fallback store. Add account lockout, password policy, and remove default credentials.
  - Uses `argon2` correctly for hashing, but storage lacks salt/param rotation policies and KMS at rest. Add KDF parameter versioning and rehash on login if policy upgraded.
- `src/pgwire_server/security.rs`
  - Sends `AuthenticationCleartextPassword`. Replace with SCRAM‑SHA‑256 (or SCRAM‑SHA‑256‑PLUS with channel binding) and require TLS.
- `Dockerfile`
  - No USER, runs as root; no distroless runtime; no read‑only FS; no drop‑caps. Apply hardening below.
- `src/server.rs`
  - No evidence of TLS or OIDC middleware. Add OIDC/OAuth middleware for HTTP endpoints; set secure cookies, CSRF protections, and session rotation.

---

### Concrete Hardening Examples
#### Hardened Dockerfile sketch
```dockerfile
FROM rust:1.89-slim-bookworm AS build
WORKDIR /app
# build steps ...

FROM gcr.io/distroless/cc-debian12@sha256:<pinned>
# or chainguard/wolfi minimal base
USER 10001:10001
WORKDIR /opt/clarium
COPY --from=build /app/target/release/clarium /usr/local/bin/clarium
ENV CLARIUM_DB_FOLDER=/var/lib/clarium/dbs \
    CLARIUM_HTTP_PORT=7878 \
    CLARIUM_PG_PORT=5433 \
    CLARIUM_PGWIRE=true
EXPOSE 7878 5433
# Read‑only root; app writes only to mounted volume
VOLUME ["/var/lib/clarium/dbs"]
ENTRYPOINT ["/usr/local/bin/clarium"]
```

#### Kubernetes securityContext and networkPolicy
```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        fsGroup: 10001
      containers:
      - name: clarium
        image: your-registry/clarium:sha-<pinned>
        securityContext:
          readOnlyRootFilesystem: true
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
        volumeMounts:
        - name: data
          mountPath: /var/lib/clarium/dbs
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: clarium-data
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
spec:
  podSelector: { }
  policyTypes: ["Ingress", "Egress"]
  ingress:
  - from: [{ podSelector: { matchLabels: { app: ingress } } }]
  egress:
  - to: [{ namespaceSelector: { matchLabels: { name: observability } } }]
    ports: [{ protocol: TCP, port: 9090 }]
```

#### HTTP security headers and cookies (Axum example outline)
```rust
// Pseudocode: add tower-http layers for security headers and cookies
use tower_http::cors::CorsLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use headers::HeaderValue;

let app = Router::new()
    // .route(...)
    .layer(CorsLayer::permissive()) // tighten for prod
    .layer(SetResponseHeaderLayer::overriding(
        header::STRICT_TRANSPORT_SECURITY, HeaderValue::from_static("max-age=31536000; includeSubDomains")
    ));
// Set cookies with Secure, HttpOnly, SameSite=Strict
```

---

### 30/60/90‑Day Remediation Plan
#### 0–30 days (Foundational)
- Remove default admin creation; add bootstrap flow requiring an admin‑provided one‑time secret.
- Introduce enterprise TLS termination path and require TLS for pgwire; disable cleartext auth.
- Container hardening: non‑root, read‑only, drop caps; health probes; resource limits.
- Add structured logging and ship to SIEM; basic authz decision logs.
- CI: `cargo audit`, `cargo deny`, secret scanning, SBOM generation; enable Dependabot/Renovate.

Acceptance: All services run under TLS, image is non‑root, CI fails on critical vulns, SIEM receives logs.

#### 31–60 days (Identity & Authorization)
- Implement OIDC login for HTTP API; map IdP groups to roles. Add RBAC enforcement across commands.
- Implement SCRAM‑SHA‑256 for pgwire; enforce TLS; provide service account token flow.
- Secrets managed via Vault/AKV/ASM; KMS for at‑rest keys.
- Add audit trail endpoints and retention policies.

Acceptance: SSO working against enterprise IdP; RBAC enforced; zero local passwords required in prod.

#### 61–90 days (Resilience & Governance)
- DR: automated encrypted backups, restore testing; document RPO/RTO.
- Multi‑tenant isolation controls (namespace/schema), quotas and rate limits.
- Policy engine (OPA/Cedar) for ABAC; data masking and export controls.
- Complete hardening docs and run external pen test.

Acceptance: Successful restore drill, pen test fixes closed, policies enforced, governance docs approved.

---

### Compliance Alignment (quick mapping)
- ISO 27001/2: A.9 (Access Control), A.10 (Crypto), A.12 (Ops Security), A.16 (Incident Mgmt).
- SOC 2: CC6/7 (Access/Change), CC8 (Availability), CC9 (Risk). HIPAA/PCI: encrypt in transit/at rest; audit logs; MFA.

---

### Next Steps
1) Confirm target IdP and desired auth flows (human vs machine). Provide SCIM/Just‑in‑Time role mapping scope.
2) Choose TLS termination pattern (K8s ingress vs sidecar vs in‑app) and posture for pgwire.
3) Approve CI/CD security baseline and container hardening changes.
4) We can draft a read‑only PR with the scaffolding for OIDC, SCRAM, and Docker hardening for your review.

If you share your preferred IdP and deployment environment (Kubernetes distro, cloud provider), I’ll tailor concrete configs and a minimal PR skeleton to accelerate adoption.