# Required Environment Variables

This file lists canonical env var names only. Do not add secret values to this file.

## Fly Backend

Required for production runtime:

| Var | Purpose | Notes |
| --- | --- | --- |
| `APP_ENV` | Runtime mode | Use `production` for production. |
| `APP_SESSION_SECRET` | Signed session cookies and password pepper | Required by startup checks in production. Must be strong and rotated during the cookie-only auth deploy to invalidate previously exposed bearer/localStorage tokens. |
| `APP_SESSION_COOKIE_SAMESITE` | Session cookie SameSite policy | Required as `none` on Fly production while the Vercel frontend calls the Fly API cross-site. Local/dev may omit this and use the `lax` default. |
| `DATABASE_URL` | Production PostgreSQL database | Production must use the actual Postgres connection string, or be omitted only when Fly injects it automatically through the attached Postgres database. Do not set production to SQLite. |
| `ADMIN_TOKEN` | Admin/token-protected backend paths | Keep distinct from `APP_SESSION_SECRET`. |
| `FRONTEND_ORIGINS` | Credentialed CORS origins | Prefer this over `FRONTEND_URL`. |
| `FRONTEND_BASE_URL` | Current app URL for checkout, billing, emails, notifications | Current code reads this. Future canonical target is `PUBLIC_APP_URL`. |
| `APP_BASE_URL` | Current app URL for email templates and auth URL generation | Current code reads this. Future canonical target is `PUBLIC_APP_URL`. |
| `PUBLIC_SITE_URL` | Marketing/public site URL | Optional if default `https://walnutmarkets.com` is correct. |
| `SUPPORT_EMAIL` | Canonical support address | Defaults to `support@walnutmarkets.com` if absent. |
| `FMP_API_KEY` | Financial Modeling Prep provider access | Required for provider-backed market data and AI Growth Article-Reactive X campaigns. Server env/Fly secret only; never store in admin DB settings. |
| `FMP_ALLOW_SYNC_USER_FETCH` | User-route live FMP fetch guardrail | Keep explicit while hybrid cache/hydration is active. |
| `INSIGHTS_DATA_MODE` | Insights data source mode | Default is `builder_safe`; use cached EOD ETF proxies plus FRED macro cache and avoid FMP add-on endpoints. See `docs/runbooks/insights_data_sources.md` before changing after an FMP plan upgrade. |
| `FMP_ALLOW_BOUNDED_TICKER_REFRESH` | Bounded ticker hydration refresh | Keep if live bounded refresh is enabled. |
| `FMP_BACKGROUND_REFRESH_ENABLED` | Background refresh kill switch | Keep for queue safety. |
| `FMP_PLAN_CALLS_PER_MINUTE` | FMP operational plan budget | Canonical budget var; Admin Provider Usage reports the Enterprise / 500 contract assumption separately from live guardrails. |
| `FMP_SOFT_LIMIT_PER_MINUTE` | Soft provider budget | Canonical budget var. |
| `FMP_HARD_LIMIT_PER_MINUTE` | Hard provider budget | Canonical budget var. |
| `DATA_ENRICHMENT_QUEUE_ENABLED` | Cron queue gate | Also set in `backend/fly.toml`. |
| `DATA_ENRICHMENT_QUEUE_BATCH_SIZE` | Queue batch size | Also set in `backend/fly.toml`. |
| `DATA_ENRICHMENT_QUEUE_MAX_SECONDS` | Queue max runtime | Also set in `backend/fly.toml`. |
| `EMAIL_DELIVERY_ENABLED` | Enables real email delivery | Keep false for dry-run environments. |
| `EMAIL_PROVIDER` | Email provider selector | `postmark` or `resend`. |
| `POSTMARK_SERVER_TOKEN` | Postmark sending token | Required when `EMAIL_PROVIDER=postmark`. |
| `RESEND_API_KEY` | Resend sending token | Required only when `EMAIL_PROVIDER=resend`. |
| `EMAIL_FROM_SUPPORT` | Support/account fallback sender | Fallback only when template sender fields are blank. |
| `EMAIL_FROM_ALERTS` | Alert fallback sender | Fallback only when template sender fields are blank. |
| `EMAIL_FROM_BILLING` | Billing fallback sender | Fallback only when template sender fields are blank. |
| `EMAIL_REPLY_TO` | Canonical fallback reply-to | Fallback only when template reply-to is blank. |
| `EMAIL_DIGEST_SCHEDULE_ENABLED` | Enables scheduled digest cron sends | Required only if scheduled sends should run. |
| `EMAIL_ALERT_INTRADAY_ENABLED` | Enables intraday alert sends | Required only if intraday sends should run. |
| `OPENAI_API_KEY` | AI Outreach suggestion generation | Server env/Fly secret only. Required to generate suggested replies; do not store in admin DB settings. |
| `AI_MARKETING_MODEL` | AI Outreach model override | Server env only. Optional; defaults to `gpt-5.4-mini`; do not store in admin DB settings. |
| `AI_GROWTH_DIGEST_RECIPIENT` | AI Growth draft approval recipient | Defaults to `jarod@walnutmarkets.com`; set explicitly for production article-reactive approvals. |
| `AI_GROWTH_ARTICLE_AUTOMATION_ENABLED` | Enables scheduled Article-Reactive X cron runs | Optional; set `true` only after reviewing dry-runs/manual runs. |
| `AI_GROWTH_ARTICLE_MAX_DAILY_DRAFTS` | Optional global article draft cap hint | Optional; campaign settings still clamp article drafts to max 2 per day. |
| `REDDIT_CLIENT_ID` | Reddit official API OAuth client ID | Server env/Fly secret only. Required only for AI Outreach Reddit discovery; do not store in admin DB settings. |
| `REDDIT_CLIENT_SECRET` | Reddit official API OAuth client secret | Server env/Fly secret only. Required only for AI Outreach Reddit discovery; do not store in admin DB settings. |
| `REDDIT_USER_AGENT` | Reddit official API user agent | Server env only. Required only for AI Outreach Reddit discovery; do not store in admin DB settings. |
| `STRIPE_SECRET_KEY` | Stripe API access | Required for checkout, portal, admin sync. |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook verification | Required for webhook sync. |
| `STRIPE_PRICE_ID_PREMIUM_MONTHLY` | Premium monthly price | Canonical price var. |
| `STRIPE_PRICE_ID_PREMIUM_ANNUAL` | Premium annual price | Canonical price var. |
| `STRIPE_PRICE_ID_PRO_MONTHLY` | Pro monthly price | Canonical price var. |
| `STRIPE_PRICE_ID_PRO_ANNUAL` | Pro annual price | Canonical price var. |
| `STRIPE_CUSTOMER_PORTAL_RETURN_URL` | Stripe portal return URL | Must use `https://app.walnutmarkets.com/...` if set. |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID | Can be overridden by admin setting for client ID, but env is safest for production. |
| `GOOGLE_CLIENT_SECRET` | Google OAuth secret | Required for Google OAuth. |
| `GOOGLE_REDIRECT_URI` | Google OAuth redirect override | Optional if generated callback URL matches Google Console. |
| `MASSIVE_API_KEY` | Massive options/alternate data provider | Required only if options flow provider is live. |
| `OPTIONS_FLOW_PROVIDER` | Options flow provider selector | Defaults to Massive. |

Cookie-only auth deployment note:

- Set `APP_ENV=production` on Fly so session cookies are emitted as `Secure` and startup validation runs in production mode.
- Set `APP_SESSION_COOKIE_SAMESITE=none` on Fly while the app frontend and API are cross-site (`https://app.walnutmarkets.com` to `https://congress-tracker-api.fly.dev`). The backend will fail startup in production if cross-site cookie auth would otherwise use `lax` or `strict`.
- Keep `APP_ALLOW_BEARER_SESSION_AUTH` unset or false. It is only a non-production escape hatch for local test/dev sessions and production startup rejects it.
- Rotate `APP_SESSION_SECRET` in the same deploy that removes bearer-session auth, so any session tokens previously exposed to browser JavaScript or localStorage stop working.
- Confirm `FRONTEND_ORIGINS=https://app.walnutmarkets.com` unless an additional production Walnut app domain is intentionally enabled.
- Confirm production `DATABASE_URL` resolves to Postgres, or is supplied automatically by Fly Postgres. SQLite URLs such as `sqlite:////data/app.db` are local/dev only and must not replace production Postgres.
- Keep web-machine startup maintenance disabled unless actively recovering data:
  - `AUTOHEAL_ON_STARTUP=0`
  - `AUTO_REPAIR_EVENTS_ON_STARTUP=0`
  - `AUTO_BACKFILL_EVENTS_ON_STARTUP=0`
  These tasks are scheduled in the background when explicitly enabled, but normal production repairs/backfills should run through cron/admin jobs rather than blocking app readiness.
- Create optional large-table performance indexes with the maintenance command in `docs/runbooks/production_indexes.md`, not during normal web startup.
- Manage AI Outreach provider credentials with Fly secrets/server env only. The admin settings API ignores and rejects DB-stored provider values for `OPENAI_API_KEY`, `AI_MARKETING_MODEL`, `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, and `REDDIT_USER_AGENT`; see `docs/runbooks/ai_outreach_provider_credentials.md` for cleanup of deprecated rows.
- Smoke test login, Google OAuth, `/api/auth/me`, admin, account, watchlists, screener, logout, and refresh after deploy.

Optional backend tuning vars that are safe to omit unless tuning production behavior:

`DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `DB_POOL_TIMEOUT`, `DB_POOL_RECYCLE_SECONDS`, `DB_CHECKOUT_SLOW_LOG_MS`, `DB_SESSION_SLOW_LOG_MS`, `QUOTE_LOOKUP_MAX_FETCH`, `HEAVY_ROUTE_MAX_CONCURRENCY`, `HEAVY_ROUTE_WAIT_SECONDS`, `TICKER_CHART_MAX_CONCURRENCY`, `TICKER_WIDGET_MAX_CONCURRENCY`, `TICKER_RESPONSE_CACHE_TTL_SECONDS`, `TICKER_FUNDAMENTALS_CACHE_TTL_SECONDS`, `TICKER_CHART_DEDUPE_WAIT_SECONDS`, `FMP_TICKER_REFRESH_MAX_CALLS_PER_SYMBOL`, `FMP_TICKER_REFRESH_LOCK_TTL_SECONDS`, `FMP_TICKER_REFRESH_WATCHLIST_ONLY`, `PRIORITY_TICKER_PREWARM_SYMBOL_LIMIT`, `PRIORITY_TICKER_PREWARM_POPULAR_LIMIT`.

Remove before live Stripe checkout or do not keep long-term after verification:

`APP_FRONTEND_URL`, `MARKETING_SITE_URL`, `STRIPE_CHECKOUT_SUCCESS_URL`, `STRIPE_CHECKOUT_CANCEL_URL`, `STRIPE_PRICE_ID_MONTHLY`, `STRIPE_PRICE_ID_ANNUAL`, `EMAIL_FROM`, `PASSWORD_RESET_FROM`, `EMAIL_REPLY_TO_SUPPORT`.

## Vercel Frontend

Vercel currently returned no production env rows via CLI. The frontend has production defaults for core URLs, but these are the intended public names:

| Var | Purpose | Current code status |
| --- | --- | --- |
| `NEXT_PUBLIC_API_BASE` | Current API base URL used by frontend code | Current code reads this. Set to `https://congress-tracker-api.fly.dev` if overriding defaults. |
| `NEXT_PUBLIC_API_BASE_URL` | Preferred future API base URL | `LOCAL_DEV.md` only today. Add code support before relying on it. |
| `NEXT_PUBLIC_SITE_URL` | Public marketing site URL | Used by member metadata/admin email preview. |
| `NEXT_PUBLIC_APP_URL` | Public app URL | Used by landing/legal/admin email preview. |
| `NEXT_PUBLIC_APP_BASE_URL` | Legacy app URL alias | Used by admin email preview. Deprecate after consolidation. |
| `NEXT_PUBLIC_APP_ENV` | Public app environment label | Used only with debug fetch behavior. |

Do not add `NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY` unless client-side Stripe is introduced. Current Stripe checkout flow is backend-driven.

## GitHub Actions

| Var | Scope | Purpose |
| --- | --- | --- |
| `FLY_API_TOKEN` | GitHub repository secret | Required by `.github/workflows/daily_ingest.yml` to run remote Fly ingest jobs. |
| `GITHUB_TOKEN` | GitHub-provided | Used by gitleaks action in `.github/workflows/security.yml`; no manual secret required. |
| `GITLEAKS_ENABLE_COMMENTS` | Workflow env | Set to `false`. |
| `GITLEAKS_ENABLE_UPLOAD_ARTIFACT` | Workflow env | Set to `false`. |

## Future Canonical URL Target

The requested canonical URL names are good, but backend/frontend code does not fully read all of them yet.

Target backend names after a compatibility PR:

```text
PUBLIC_SITE_URL=https://walnutmarkets.com
PUBLIC_APP_URL=https://app.walnutmarkets.com
API_BASE_URL=https://congress-tracker-api.fly.dev
```

Target frontend names after a compatibility PR:

```text
NEXT_PUBLIC_API_BASE_URL=https://congress-tracker-api.fly.dev
NEXT_PUBLIC_SITE_URL=https://walnutmarkets.com
NEXT_PUBLIC_APP_URL=https://app.walnutmarkets.com
```
