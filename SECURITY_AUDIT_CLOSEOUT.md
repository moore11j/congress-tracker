# Security Audit Closeout — Congress Tracker / Capitol Ledger

## Date

2026-05-09

## Scope

This closeout tracks remediation of the findings documented in `SECURITY_AUDIT_REPORT.md` for Congress Tracker / Capitol Ledger. It summarizes the completed security hardening batches, production verification, accepted residual risks, and deferred items that should remain visible before future scale-up or payment launch work.

## Executive Summary

Major critical and high severity findings from the original audit have been remediated. Password reset token disclosure, unauthenticated notification/admin utilities, unsafe admin promotion, broad CORS, critical frontend dependency advisories, exposed debug/admin paths, missing production guards, and sensitive response fields were addressed across the hardening cycle.

Production smoke checks passed after key backend deploys, and a temporary auth regression that affected Signals, Screener, Leaderboards, and Backtesting was rolled back/fixed. Admin access now works again, the full UI/features were restored, and no readable `ct_session` cookie is present; only non-secret `ct_auth_hint` / `ct_entitlement_hint` cookies remain.

Remaining items are intentionally deferred, accepted with compensating controls, or future-scale work. The main residual areas are Stripe production readiness before billing launch, removal of the bearer/localStorage transition path, shared-store rate limiting before multi-machine scaling, additional cross-user IDOR regression tests, and continued dependency/secret scanning.

## Remediation Status Table

| Original Finding | Original Severity | Status | Remediation Summary | Follow-Up |
|---|---|---|---|---|
| Password reset token returned to caller | Critical | Fixed | Password reset responses no longer return raw reset tokens or reset links in production. | Keep reset-token response config guarded; never return reset tokens in production. |
| Notification subscription/delivery APIs unauthenticated | Critical | Fixed | Subscription, delivery, and digest endpoints were locked down with auth, ownership, and admin checks as appropriate. | Continue testing user-owned notification access paths. |
| Unauthenticated `/admin/seed-demo` | Critical | Fixed | `/admin/seed-demo` is protected/disabled in production. | Keep demo seeding local/admin-only. |
| Admin role granted by hardcoded/admin email registration | Critical | Fixed | Public registration and Google upsert no longer auto-promote admin users. | Use explicit administrative processes for admin grants. |
| Critical Next.js advisories | Critical | Fixed | Next.js was upgraded from `15.1.11` to `15.5.18`. | Keep frontend dependency audit in CI and rerun after framework releases. |
| Session tokens no expiration / JS-readable storage | High | Partially Fixed | Session tokens now include `iat`/`exp`; backend issues `ct_session` as HttpOnly/Secure/SameSite cookie; logout clears the backend cookie; frontend stopped writing readable `ct_session`; `APP_SESSION_SECRET` was rotated after old readable token exposure. Bearer/localStorage fallback remains temporarily for the cross-site frontend/API transition. | Remove bearer/localStorage fallback once frontend and API are same-site or the API is proxied under the product domain. |
| Production CORS allows any origin | High | Fixed | CORS is restricted to explicit frontend origins. | Keep startup validation blocking unsafe production CORS. |
| Admin/debug query token and debug route open risk | High | Fixed | `/api/debug/ticker-meta` and `/admin/ensure_data` require admin; legacy debug/admin token flags are guarded; `events?debug=true` only returns debug metadata to admins in production. | Do not expose query-string admin tokens. |
| Missing rate limiting | High | Partially Fixed | Backend rate limiting was added for login, registration, password reset, exports, screener/signals/events/provider-heavy endpoints, backtests, notifications, digest runs, and admin mutations. The v1 limiter is in-memory and per-machine. | Move to Redis or another shared store before scaling beyond one backend machine. |
| Local secret files contain values | High | Partially Fixed | Local secret/artifact paths are ignored, secret scanning was added, no tracked matches remain, and local log artifacts were removed from tracking. | Rotate any secret that was ever shared, committed, or exposed outside a trusted local environment. |
| Missing frontend security headers | Medium | Fixed | Production frontend headers were added and verified: Content-Security-Policy, X-Content-Type-Options, Referrer-Policy, Permissions-Policy, and X-Frame-Options. | Consider stricter CSP after validating Google, Stripe, charts, and script compatibility. |
| Backend scripts can log DB URLs | Medium | Fixed | Shared redaction helpers were added, DB URL logging was redacted, and SQLAlchemy `hide_parameters` was enabled. | Keep logs redacted by default. |
| Admin/account payloads expose sensitive operational IDs | Medium | Fixed | User serializers were split by audience; normal, self, and account payloads no longer expose Stripe/customer/subscription IDs or override metadata; billing history response was minimized. | Keep sensitive operational IDs admin/server-side only. |
| Password policy inconsistent | Medium | Fixed | Password complexity is consistent across registration, reset confirmation, and account password change. | Keep shared validation paths covered by tests. |
| Public debug params reveal metadata | Medium | Fixed | Public debug metadata was restricted; production debug output is admin-only where applicable. | Avoid adding public debug flags without admin gating. |
| Local DB/log/image artifacts in workspace | Medium | Partially Fixed | `.gitignore` was hardened and tracked logs were removed. Local cleanup remains operational hygiene. | Periodically remove local DB, log, screenshot, generated audit, and temporary artifacts. |
| Stripe webhook timestamp tolerance | Low | Deferred | Stripe is not yet live and remains intentionally disabled pending incorporation/billing setup. | Add timestamp tolerance and full Stripe production hardening before enabling billing. |
| `APP_SESSION_SECRET` fallback | Low | Fixed | Production startup validation requires a strong `APP_SESSION_SECRET`. | Keep the secret strong and rotate it if exposed. |
| Frontend middleware not comprehensive | Low | Partially Fixed / Accepted | Backend remains the authority for auth and entitlement decisions; frontend middleware is UX only. Cookie/bearer transition work is ongoing. | Remove temporary fallback paths after same-site/proxy migration. |
| IDOR checks mostly strong | Informational | Accepted | Existing ownership checks were reviewed as mostly strong across protected user resources. | Add more cross-user IDOR regression tests. |
| Admin reports/actions require admin | Informational | Fixed | Admin access controls were confirmed/restored after the auth regression fix. | Keep admin smoke tests in release checks. |
| Stripe checkout/entitlements server-side controlled | Informational | Deferred | Stripe remains intentionally not fully enabled until incorporation and billing setup are ready. | Re-audit Stripe checkout, webhook, entitlement, and customer flows before launch. |
| SQL injection review mostly parameterized | Informational | Accepted | The reviewed code remains mostly parameterized; SQLAlchemy parameter hiding and redaction were added. | Continue using parameterized queries and review raw SQL additions. |
| GitHub Actions no obvious secret exposure | Informational | Fixed | Security workflow was added for frontend audit, backend audit, and gitleaks secret scanning; workflow avoids repository secret exposure. | Keep scanner findings redacted and narrow any ignore entries. |
| Python dependency audit not run | Informational | Fixed | `pip-audit` was run after dependency updates and reports no known vulnerabilities. | Keep backend dependency audit in CI and rerun after dependency changes. |

## Production Verification

Latest verification and security checks included:

- `/health` returned `200`.
- Unauthenticated `/api/admin/settings` returned `401`.
- `/api/events?limit=5` returned `200`.
- Frontend security headers were verified in production.
- Admin access was restored on Signals, Screener, Leaderboards, and Backtesting after the auth regression fix.
- `pip-audit` reported no known vulnerabilities after the FastAPI, Starlette, and Requests advisory patch.
- `npm audit --omit=dev --audit-level=high` passed for the frontend.
- Gitleaks secret scanning passed with redacted output and narrow false-positive ignores.

## Deferred / Future Work

1. Add Stripe webhook timestamp tolerance and complete Stripe production hardening before billing launch.
2. Remove bearer/localStorage fallback once frontend/API are same-site or the API is proxied under the product domain.
3. Move rate limiting to Redis or another shared store before scaling beyond one backend machine.
4. Add more cross-user IDOR regression tests.
5. Periodically rerun the security workflow and dependency scans.
6. Consider stricter CSP after confirming Google, Stripe, charts, and script compatibility.
7. Clean local artifacts periodically.

## Operational Notes

- Do not re-enable `CT_ALLOW_ENTITLEMENT_HEADER` in production.
- Do not return password reset tokens in production.
- Do not expose query-string admin tokens.
- Keep `APP_SESSION_SECRET` strong and rotate it if exposed.
- Keep Stripe disabled until incorporation and billing setup are ready.
- Keep SQLite/Postgres migration rollback artifacts untouched unless explicitly retired.

## Final Status

Launch-blocking security findings are remediated or have an explicit safe deferral. Remaining work is hardening, scale readiness, and payment readiness.

The security posture is materially improved versus the original audit.
