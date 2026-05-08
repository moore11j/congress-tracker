# Security Audit Report - Congress Tracker / Capitol Ledger

Date: 2026-05-07

Scope: Defensive, non-destructive source review and limited unauthenticated public smoke checks for the FastAPI backend (`congress-tracker-api`) and Next.js frontend (`congress-tracker-two.vercel.app`).

Constraints honored:
- No destructive exploitation performed.
- No production data intentionally mutated.
- No production secrets, tokens, passwords, payment data, private user data, or database contents are printed here.
- Sensitive local values observed in environment files are redacted as `[REDACTED]`.
- Suspected data-exposing endpoints were not broadly enumerated.

## Summary

The application has several strong ownership checks around watchlists, saved screens, monitoring inbox, signals, admin reports, admin user actions, and Stripe webhooks. However, the audit found multiple launch-blocking issues in auth/session design, password reset, unauthenticated notification/admin utility endpoints, CORS, and dependency posture.

Public unauthenticated smoke checks:

| Endpoint | Method | Result |
|---|---:|---:|
| `/health` | GET | 200 |
| `/api/admin/settings` | GET | 401 |
| `/api/account/settings` | GET | 401 |
| `/api/watchlists` | GET | 401 |
| `/api/saved-screens` | GET | 401 |
| `/api/monitoring/inbox` | GET | 401 |
| `/api/signals/all` | GET | 401 |
| `/api/screener/export.csv` | GET | 402 |
| `/api/notification-subscriptions?source_id=__audit_nonexistent__` | GET | 200 |
| `/api/notifications/deliveries?subscription_id=-1&limit=1` | GET | 200 |
| API CORS preflight from arbitrary origin | OPTIONS | 200, `Access-Control-Allow-Origin: *` |

Sensitive route authorization summary:

| Area | Endpoints inspected | Auth/admin result |
|---|---|---|
| Admin settings/users/reports | `/api/admin/settings`, `/api/admin/users`, admin exports, plan/price/feature updates, suspend/delete/batch | Server-side `require_admin_user` present. Smoke check confirmed `/api/admin/settings` returns 401 unauthenticated. |
| Legacy/admin utility routes | `/admin/seed-demo`, `/admin/ensure_data` | `/admin/seed-demo` has no auth and mutates data. `/admin/ensure_data` uses `ADMIN_TOKEN` query parameter, not session/admin auth. |
| Account/subscription | `/api/auth/me`, `/api/account/*`, `/api/billing/*` | Account and billing customer endpoints require current user, except public auth and Stripe webhook endpoints. |
| Watchlists | `/api/watchlists*`, `/api/watchlists/{id}/feed`, events/signals routes | Server-side current-user and ownership checks present. |
| Saved screens | `/api/saved-screens*` | Server-side current-user and ownership checks present. |
| Monitoring inbox | `/api/monitoring/*` | Server-side current-user checks present; source-level ownership checks present for watchlist source mutations. |
| Notifications | `/api/notification-subscriptions`, `/api/notifications/deliveries`, `/api/notifications/digest/run` | Several endpoints lack auth/ownership checks. |
| Screener/export/signals/backtests | `/api/screener`, `/api/screener/export.csv`, `/api/signals/*`, `/api/backtests/run` | Entitlement checks present. Backtest run requires auth. |
| Public market data | feed/events/tickers/members/insiders/news/leaderboards | Mostly intentionally public; expensive endpoints need rate limiting. |
| Debug/meta/health | `/api/debug/ticker-meta`, `/api/meta`, `/health` | `/api/debug/ticker-meta` is protected only by optional query token and is open if `ADMIN_TOKEN` is unset. |

## Critical

### Password Reset Token Returned Directly to Caller

- Severity: Critical
- Affected area/file/endpoint: `backend/app/routers/accounts.py`, `/api/auth/password-reset/request`; `frontend/components/auth/ResetPasswordPanel.tsx`
- What was tested or inspected: Source inspection of the password reset request and frontend reset panel.
- Risk: Anyone who knows or guesses an account email can request a reset and receive the reset token/link in the HTTP response. That allows account takeover without access to the email inbox.
- Evidence without exposing secrets/private data: `request_password_reset` generates a token, stores only its hash, then returns `reset_path` containing the raw token. The frontend displays an "Open secure reset link" link from that response.
- Recommended patch: Never return reset tokens in API responses outside local-only development. Send reset links through a verified email provider, rate-limit requests, always return a generic message, and consider invalidating all existing sessions after password reset.
- Status: Confirmed.

### Notification Subscription and Delivery APIs Expose/Mutate Data Without Auth

- Severity: Critical
- Affected area/file/endpoint: `backend/app/routers/notifications.py`; `/api/notification-subscriptions`, `/api/notification-subscriptions/{subscription_id}`, `/api/notifications/digest/run`, `/api/notifications/deliveries`
- What was tested or inspected: Source inspection and non-sensitive production smoke checks using impossible IDs to avoid retrieving real data.
- Risk: Anonymous callers can list notification subscriptions, filter by email/source, delete subscriptions by ID, view delivery records, and run digest generation. Depending on stored records, this can expose email addresses, digest body text, alert contents, and operational state, and can cause unwanted email sending if `send=true` is accepted.
- Evidence without exposing secrets/private data: Source shows list/delete/deliveries/digest-run endpoints have no `current_user` or admin dependency. Production returned 200 for filtered nonexistent subscription and delivery queries, confirming anonymous route access without returning real records.
- Recommended patch: Require authenticated users for user-level notification endpoints and enforce ownership by user ID, not by caller-supplied email/source fields. Require admin auth for digest-run and global delivery inspection. Store notification subscriptions with `user_id`; deny delete unless owner/admin. Consider one-click unsubscribe with a separate signed unsubscribe token.
- Status: Confirmed.

### Unauthenticated Admin Demo Seed Route Mutates Data

- Severity: Critical
- Affected area/file/endpoint: `backend/app/main.py`, `/admin/seed-demo`
- What was tested or inspected: Source inspection only; the production POST endpoint was not called.
- Risk: Any unauthenticated caller can insert demo member/security/filing/transaction records into the production database if the demo record is absent. This violates data integrity and creates an unauthenticated write primitive.
- Evidence without exposing secrets/private data: The route is decorated with `@app.post("/admin/seed-demo")` and has only `db: Session = Depends(get_db)`, with no admin token, session, or `require_admin_user` check.
- Recommended patch: Remove this route from production builds or require `require_admin_user`. Prefer moving demo seeding to a local-only script guarded by `APP_ENV != "production"`.
- Status: Confirmed.

### Admin Role Can Be Granted by Registering a Hardcoded Admin Email

- Severity: Critical
- Affected area/file/endpoint: `backend/app/auth.py`, `backend/app/routers/accounts.py`, `/api/auth/register`
- What was tested or inspected: Source inspection of admin email handling and registration logic.
- Risk: Admin status is derived from email address and registration sets `role="admin"` when the submitted email matches the configured/hardcoded admin list. Without email verification, a caller who registers a matching admin email before or instead of the legitimate owner could receive admin privileges.
- Evidence without exposing secrets/private data: `admin_emails()` includes configured values plus a hardcoded personal email `[REDACTED]`. Registration sets `user.role = "admin"` when `email in admin_emails()`; no email verification step is present in this path.
- Recommended patch: Remove hardcoded admin emails. Never grant admin role during public registration. Require an existing admin session to promote users, or use a one-time bootstrap command unavailable over public HTTP. Add email verification before any privileged role can be activated.
- Status: Confirmed source issue; exploitability depends on whether the affected email/account already exists and on operational registration controls.

### Critical Next.js Dependency Advisories

- Severity: Critical
- Affected area/file/endpoint: `frontend/package.json`, `frontend/package-lock.json`
- What was tested or inspected: `npm audit --omit=dev` in `frontend`.
- Risk: Installed `next@15.1.11` is within the audited vulnerable range. Reported advisories include middleware authorization bypass, SSRF via middleware redirect handling, request smuggling in rewrites, DoS issues, image optimizer issues, and dev-server origin verification issues.
- Evidence without exposing secrets/private data: `npm audit --omit=dev` reported 1 critical vulnerability against `next` and 1 moderate vulnerability against transitive `postcss`.
- Recommended patch: Upgrade Next.js to a patched version compatible with the app and rerun tests/build. Do not use `npm audit fix --force` blindly; choose a known-good Next version and validate middleware/auth behavior.
- Status: Confirmed by dependency audit.

## High

### Session Tokens Have No Expiration and Are Stored in JavaScript-Readable Storage

- Severity: High
- Affected area/file/endpoint: `backend/app/auth.py`, `backend/app/routers/accounts.py`, `frontend/lib/api.ts`, `frontend/lib/serverAuth.ts`, `frontend/middleware.ts`
- What was tested or inspected: Source inspection of session signing, auth response, frontend token storage, and middleware.
- Risk: A stolen token remains valid indefinitely unless the secret changes or user is suspended. Because the frontend stores the token in `localStorage` and a JavaScript-readable cookie, any XSS or malicious client-side script can steal it. The cookie is not `HttpOnly` or `Secure`.
- Evidence without exposing secrets/private data: `_auth_response_for_user` signs only `uid` and `email`; `verify_session_token` does not check `exp`, `iat`, rotation, or revocation. `rememberAuthToken` writes to `localStorage` and `document.cookie` with `SameSite=Lax` and `Max-Age=30 days`, but not `HttpOnly`/`Secure`.
- Recommended patch: Move session issuance to the backend using `Set-Cookie` with `HttpOnly`, `Secure`, `SameSite=Lax` or `Strict`, and explicit expiration. Add `exp`/`iat` to tokens, short TTLs, refresh/rotation or server-side session revocation, and logout that clears the cookie server-side.
- Status: Confirmed.

### Production CORS Allows Any Origin

- Severity: High
- Affected area/file/endpoint: `backend/app/main.py`, FastAPI CORS middleware
- What was tested or inspected: Source inspection and production CORS preflight.
- Risk: Any website can call the API from a browser with custom headers such as `Authorization`. This does not expose cookies because credentials are disabled, but it expands abuse surface, makes token-based browser calls easier from malicious origins, and ignores intended frontend origin controls.
- Evidence without exposing secrets/private data: Source config uses `allow_origins=["*"]`, `allow_methods=["*"]`, `allow_headers=["*"]`, `allow_credentials=False`. Production preflight from an arbitrary origin returned `Access-Control-Allow-Origin: *` and allowed `authorization`.
- Recommended patch: Use an environment-driven allowlist such as `FRONTEND_ORIGINS` or `CORS_ALLOW_ORIGINS`, reject wildcard in production, and allow only the Vercel production/preview origins that should call the API.
- Status: Confirmed.

### Admin/Debug Token Passed in Query String and Debug Route Opens if Token Missing

- Severity: High
- Affected area/file/endpoint: `backend/app/main.py` `/admin/ensure_data`; `backend/app/routers/debug.py` `/api/debug/ticker-meta`
- What was tested or inspected: Source inspection only; no production token route was called.
- Risk: Query tokens can leak via logs, browser history, referers, proxies, and monitoring tools. The debug route becomes anonymous if `ADMIN_TOKEN` is unset, and the admin ensure-data route uses token auth instead of the server-side admin session model.
- Evidence without exposing secrets/private data: `/admin/ensure_data` accepts `token: str | None = Query(default=None)` and compares against `ADMIN_TOKEN`. `/api/debug/ticker-meta` accepts `token` and explicitly leaves the route open when `ADMIN_TOKEN` is not set.
- Recommended patch: Remove query-token auth. Protect operational routes with `require_admin_user` or a backend-only job mechanism. Disable debug routes in production or require admin auth unconditionally.
- Status: Confirmed.

### Missing Rate Limiting on Auth, Reset, Export, and Expensive Data Endpoints

- Severity: High
- Affected area/file/endpoint: `/api/auth/login`, `/api/auth/register`, `/api/auth/password-reset/request`, `/api/screener`, `/api/screener/export.csv`, `/api/signals/*`, `/api/backtests/run`, news/provider-backed endpoints, export/report endpoints
- What was tested or inspected: Source inspection for rate limiting or throttling controls.
- Risk: Login and reset endpoints can be abused for credential attacks and account enumeration pressure. Expensive screeners/signals/backtests/exports can drive provider costs, database load, or availability issues. Password reset is especially severe because tokens are returned in responses.
- Evidence without exposing secrets/private data: No rate limiter middleware, per-user quota check, IP throttling, or provider-cost guard was found in the inspected FastAPI app.
- Recommended patch: Add centralized rate limiting with separate buckets for anonymous auth/reset, authenticated user actions, admin/export actions, and provider-backed endpoints. Add audit logs for throttled sensitive actions.
- Status: Confirmed absence in source.

### Local Secret Files Contain Real-Looking Values

- Severity: High
- Affected area/file/endpoint: `backend/.env.local`, `frontend/.env.local`, `frontend/.env.prodapi.local`
- What was tested or inspected: File discovery and variable-name-only parsing. Values were not printed.
- Risk: These files are currently ignored by git, but they contain populated sensitive variables locally. They can be accidentally copied, attached, deployed, or exposed by backup/sync tooling.
- Evidence without exposing secrets/private data: `backend/.env.local` contains populated values for `DATABASE_URL`, `GOOGLE_CLIENT_SECRET`, `SECRET_KEY`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `FMP_API_KEY`, `POLYGON_API_KEY`, `MASSIVE_API_KEY`, and others. Frontend local env files contain populated public configuration values such as `NEXT_PUBLIC_*`.
- Recommended patch: Keep `.env*.local` ignored. Add secret scanning/pre-commit hooks. Move non-local secrets only to Fly/Vercel/GitHub secret stores. Consider rotating local secrets if they were ever shared or committed historically.
- Status: Confirmed locally; not confirmed committed. `git ls-files` showed these env files are not tracked.

## Medium

### Security Headers Are Mostly Missing on the Frontend

- Severity: Medium
- Affected area/file/endpoint: `frontend/next.config.js`, production frontend response headers
- What was tested or inspected: Source inspection and GET to the public frontend root.
- Risk: Missing headers reduce browser-side defense-in-depth against XSS, clickjacking, MIME sniffing, referrer leakage, and permissions abuse.
- Evidence without exposing secrets/private data: `next.config.js` is empty. Production root response had Vercel HSTS, but no app-provided `Content-Security-Policy`, `X-Frame-Options`/`frame-ancestors`, `X-Content-Type-Options`, `Referrer-Policy`, or `Permissions-Policy` in the checked response.
- Recommended patch: Add headers in `next.config.js` or middleware. At minimum: `Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `Referrer-Policy`, `Permissions-Policy`, and `frame-ancestors 'none'` or an explicit allowlist.
- Status: Confirmed.

### Backend Scripts Can Log Database URLs

- Severity: Medium
- Affected area/file/endpoint: `backend/app/backfill_events_from_trades.py`, migration/verification scripts
- What was tested or inspected: Source search for `DATABASE_URL` and raw logging.
- Risk: If run in production or CI, logs can expose connection strings with usernames/passwords/hosts. Logs may persist in terminals, Fly logs, CI logs, or local files.
- Evidence without exposing secrets/private data: `backfill_events_from_trades.py` logs `DATABASE_URL` directly. Migration tools accept explicit Postgres URLs and write diagnostic reports/logs.
- Recommended patch: Add a common URL redaction helper and use it everywhere a DB URL might be logged. Redact userinfo, password, host if needed, and query parameters.
- Status: Confirmed.

### Admin and Account User Payloads Include Sensitive Operational Identifiers

- Severity: Medium
- Affected area/file/endpoint: `backend/app/routers/accounts.py`, `_public_user`, `/api/auth/me`, `/api/account/settings`, `/api/admin/settings`, `/api/admin/users`
- What was tested or inspected: Source inspection of user serialization.
- Risk: Own-account responses include billing address fields and Stripe customer/subscription IDs. Admin responses include these for all users. This may be intended, but it increases impact if any auth bypass, XSS, or admin endpoint exposure occurs.
- Evidence without exposing secrets/private data: `_public_user` includes address fields, billing location, `stripe_customer_id`, `stripe_subscription_id`, subscription state, price overrides, and admin flags.
- Recommended patch: Split serializers by audience: minimal self profile, billing profile, admin user list row, and admin user detail. Exclude Stripe IDs unless required for a specific admin support workflow.
- Status: Confirmed design risk.

### Password Policy Is Stronger on Change Than Registration/Reset

- Severity: Medium
- Affected area/file/endpoint: `backend/app/routers/accounts.py`, `/api/auth/register`, `/api/auth/password-reset/confirm`, `/api/account/password`
- What was tested or inspected: Source inspection.
- Risk: Registration and reset require only 8 characters via schema/hash function, while password change requires a letter, number, and special character. Users can set weaker passwords through registration or reset than through account settings.
- Evidence without exposing secrets/private data: `_password_meets_account_rules` is enforced in `update_account_password`; registration and reset call `hash_password` directly.
- Recommended patch: Apply the same password policy to registration, reset, and change. Consider breached-password checks and minimum length 12+.
- Status: Confirmed.

### Public Debug Parameters Can Reveal Internal Query Metadata

- Severity: Medium
- Affected area/file/endpoint: `backend/app/routers/events.py` `/api/events?debug=true`; `backend/app/routers/signals.py` debug mode on authenticated signals
- What was tested or inspected: Source inspection.
- Risk: Debug responses can reveal internal filter decisions, counts, and SQL hints. This is lower risk than direct data exposure, but it helps attackers profile data shape and expensive filters.
- Evidence without exposing secrets/private data: `/api/events` accepts `debug` and returns received params, applied filters, count after filters, and SQL hint text. It is public.
- Recommended patch: Disable public debug output in production or require admin auth for debug payloads.
- Status: Confirmed.

### Local Database, Logs, Images, and Generated Artifacts Exist in Workspace

- Severity: Medium
- Affected area/file/endpoint: repo root and `backend/` local artifacts
- What was tested or inspected: File discovery only; contents were not printed.
- Risk: Local `.db`, `.log`, screenshot, and generated comparison artifacts can contain data, URLs, stack traces, or other sensitive operational details. They are mostly ignored, but root logs/images are visible in the working tree.
- Evidence without exposing secrets/private data: Found local artifacts such as `backend/app_local.db`, local backend/frontend logs, screenshots, and JSON comparison checkpoints. `.gitignore` ignores DBs, local env files, Next build output, and backend artifacts, but root log/image patterns are not comprehensively ignored.
- Recommended patch: Extend `.gitignore` for `*.log`, local screenshots, and generated comparison files unless intentionally tracked. Periodically clean local artifacts before sharing archives.
- Status: Confirmed locally.

## Low

### Stripe Webhook Signature Verification Lacks Timestamp Tolerance

- Severity: Low
- Affected area/file/endpoint: `backend/app/routers/accounts.py`, `/api/billing/stripe/webhook`
- What was tested or inspected: Source inspection of `_verify_stripe_signature`.
- Risk: The code verifies HMAC signature but does not enforce a timestamp tolerance window. A captured valid webhook payload/signature could be replayed within persistence/idempotency limits. Idempotency by event ID reduces impact for duplicate event IDs.
- Evidence without exposing secrets/private data: `_verify_stripe_signature` parses `t` and `v1` and compares HMAC, but does not reject old timestamps. `StripeWebhookEvent` event IDs are persisted after processing.
- Recommended patch: Use Stripe's official webhook verification helper or enforce a reasonable timestamp tolerance before accepting the event.
- Status: Confirmed.

### `APP_SESSION_SECRET` Falls Back to `ADMIN_TOKEN` or Dev Secret

- Severity: Low
- Affected area/file/endpoint: `backend/app/auth.py`
- What was tested or inspected: Source inspection.
- Risk: If `APP_SESSION_SECRET` is unset, session signing may depend on `ADMIN_TOKEN` or a default development string. In production, a weak/missing secret would make tokens forgeable.
- Evidence without exposing secrets/private data: `session_secret()` returns `APP_SESSION_SECRET`, then `ADMIN_TOKEN`, then `"dev-session-secret"`.
- Recommended patch: Require `APP_SESSION_SECRET` in production and fail startup if it is missing or too short. Do not reuse admin tokens as signing keys.
- Status: Confirmed source issue; production configuration not verified.

### Frontend Middleware Gating Is Not Comprehensive, Though Backend Checks Cover Sensitive APIs

- Severity: Low
- Affected area/file/endpoint: `frontend/middleware.ts`
- What was tested or inspected: Source inspection.
- Risk: Frontend middleware protects only selected page prefixes and is based on a JavaScript-readable cookie. This should not be considered an authorization boundary.
- Evidence without exposing secrets/private data: Protected prefixes are `/watchlists`, `/monitoring`, `/signals`, and `/leaderboards`. Admin/account/billing pages rely on server-side page/auth and backend API checks.
- Recommended patch: Keep all authorization server-side. Use frontend middleware only for UX redirects. After moving to `HttpOnly` cookies, ensure server components and route handlers validate auth consistently.
- Status: Confirmed; mostly informational because backend checks exist.

## Informational

### Watchlist, Saved Screen, Monitoring, Signals, and Backtest IDOR Checks Are Mostly Strong

- Severity: Informational
- Affected area/file/endpoint: `backend/app/main.py`, `backend/app/routers/events.py`, `backend/app/routers/signals.py`, `backend/app/routers/saved_screens.py`, `backend/app/routers/backtests.py`
- What was tested or inspected: Source inspection.
- Risk: No direct IDOR was found in these areas during this pass.
- Evidence without exposing secrets/private data: Watchlist routes call `_require_account` and `_get_owned_watchlist`. Saved screen routes use `_require_screen_owner`. Watchlist event/signal routes filter by `Watchlist.owner_user_id == user.id`. Backtest presets filter watchlists/saved screens by current user, and backtest run passes `user_id` to the engine.
- Recommended patch: Add regression tests for cross-user access on every route accepting `watchlist_id`, `saved_screen_id`, monitoring source ID, and alert ID.
- Status: Confirmed positive control.

### Admin Reports, Batch User Actions, Price Overrides, Plan Changes, Suspend/Delete, and Exports Require Admin

- Severity: Informational
- Affected area/file/endpoint: `backend/app/routers/accounts.py`, `/api/admin/*`
- What was tested or inspected: Source inspection and smoke check of `/api/admin/settings`.
- Risk: No missing server-side admin check was found in these admin routes.
- Evidence without exposing secrets/private data: Admin route handlers call `require_admin_user`; unauthenticated production GET `/api/admin/settings` returned 401.
- Recommended patch: Add tests that a normal authenticated user receives 403 for every `/api/admin/*` endpoint, including XLSX/PDF exports and batch actions.
- Status: Confirmed positive control.

### Stripe Checkout and Entitlements Are Server-Side Controlled

- Severity: Informational
- Affected area/file/endpoint: `backend/app/routers/accounts.py`, `backend/app/entitlements.py`
- What was tested or inspected: Source inspection.
- Risk: No direct self-upgrade endpoint was found outside Stripe/admin flows.
- Evidence without exposing secrets/private data: Checkout session creation uses server-side Stripe price IDs by tier/interval; webhook signature verification exists; admin-only routes handle manual premium and plan config changes; entitlement checks are performed in backend routes for premium features.
- Recommended patch: Add tests for attempts to self-set `entitlement_tier`, `manual_tier_override`, and premium feature access with only frontend changes or `X-CT-Entitlement-Tier`.
- Status: Confirmed positive control, with note that `CT_ALLOW_ENTITLEMENT_HEADER` must remain disabled in production.

### SQL Injection Review Found Mostly Parameterized ORM Usage

- Severity: Informational
- Affected area/file/endpoint: Backend routers/services/scripts
- What was tested or inspected: Search for `text()`, `execute()`, dynamic sort/order handling, and f-string SQL.
- Risk: No confirmed SQL injection in public endpoints was found in this pass. Some scripts use dynamic SQL for internal migration/verification operations with quoted internal names.
- Evidence without exposing secrets/private data: Public filters generally use SQLAlchemy expressions and bounded enums/patterns. Dynamic admin sort columns use `Literal` type inputs and column maps. Migration scripts use f-string SQL around known/reflected identifiers.
- Recommended patch: Keep dynamic SQL out of request handlers. Add tests for sort/filter injection attempts and keep identifier quoting helpers centralized.
- Status: Confirmed positive control with residual script risk.

### GitHub Actions Workflow Does Not Expose Secrets in Obvious Ways

- Severity: Informational
- Affected area/file/endpoint: `.github/workflows/daily_ingest.yml`
- What was tested or inspected: Workflow file inspection.
- Risk: No unsafe PR/fork trigger or explicit secret echo was found.
- Evidence without exposing secrets/private data: Workflow uses `workflow_dispatch` and `schedule`, not `pull_request_target`. Fly token is referenced through `secrets.FLY_API_TOKEN`; script echoes only selected job mode.
- Recommended patch: Pin or checksum external installer scripts if practical, or use an official Fly action. Keep secrets out of command echoes and PR-triggered workflows.
- Status: Confirmed.

### Python Dependency Audit Was Not Run

- Severity: Informational
- Affected area/file/endpoint: `backend/requirements.txt`
- What was tested or inspected: Attempted `pip-audit --version`.
- Risk: Python dependency vulnerabilities were not assessed by an automated advisory database in this pass.
- Evidence without exposing secrets/private data: `pip-audit` is not installed in the local environment. Requirements include FastAPI, Uvicorn, SQLAlchemy, psycopg, requests, lxml, and python-dateutil.
- Recommended patch: Add `pip-audit` or `uv pip audit` to a CI security job and run it regularly.
- Status: Confirmed tool unavailable.

## Prioritized Patch Plan

### 1. Must Patch Before Launch

1. Fix password reset so tokens are sent only by email and never returned in API responses.
2. Lock down notification subscription/delivery/digest endpoints with auth, ownership, and admin checks.
3. Remove or admin-protect `/admin/seed-demo`; disable demo/admin utility routes in production.
4. Remove hardcoded admin email bootstrap and prevent public registration from assigning admin role.
5. Upgrade Next.js to a patched version and rerun `npm audit --omit=dev`.
6. Replace localStorage/JavaScript-readable session storage with backend-issued `HttpOnly; Secure` cookies and token expiration.
7. Restrict production CORS to explicit frontend origins.

### 2. Should Patch Soon

1. Add rate limiting for login, registration, password reset, exports, screeners, signals, backtests, news/provider-backed endpoints, and notification actions.
2. Add frontend security headers: CSP, frame controls, `nosniff`, referrer policy, and permissions policy.
3. Remove query-string admin/debug tokens; require server-side admin auth or internal job access.
4. Redact database URLs and other sensitive config from all scripts/logs.
5. Split user serializers so Stripe IDs and billing details are exposed only to routes that truly need them.
6. Enforce the same password complexity rules on registration, reset, and password change.

### 3. Hardening / Nice-to-Have

1. Add cross-user IDOR regression tests for watchlists, saved screens, monitoring alerts, billing history, notification subscriptions, exports, and admin endpoints.
2. Disable public debug payloads in production.
3. Add timestamp tolerance to Stripe webhook signature verification or use Stripe's official helper.
4. Add secret scanning and dependency audit jobs in CI.
5. Extend `.gitignore` for local logs/screenshots/generated reports and periodically clean local artifacts.
6. Add startup validation that production requires strong `APP_SESSION_SECRET`, restricted CORS origins, Stripe webhook secret, and disabled entitlement override headers.
