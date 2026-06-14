# Environment Variable Audit

Date: 2026-06-14

This audit covers repo references, backend Fly secret names, frontend Vercel env names, cron/scripts, Docker/Fly config, GitHub Actions, and docs. It intentionally records names and usage only. No secret values are included.

Generated source inventory:

```bash
python scripts/check_env_usage.py
```

## Executive Summary

- Backend Fly secret names were checked with `flyctl secrets list -a congress-tracker-api` using names/status only. Values were not read or printed.
- Vercel CLI connected to `moore11js-projects/congress-tracker`, but `npx vercel env ls production` returned no variable rows. Treat frontend dashboard envs as unset unless manually verified in Vercel.
- Production DB is PostgreSQL through `DATABASE_URL`. Do not touch SQLite files.
- Email template sender fields are already authoritative in `backend/app/services/email_delivery.py`: `EmailTemplate.from_email`, `from_name`, and `reply_to` win; env vars are fallback only when template fields are blank.
- Current frontend code reads `NEXT_PUBLIC_API_BASE`, not the preferred `NEXT_PUBLIC_API_BASE_URL`. `NEXT_PUBLIC_API_BASE_URL` exists only in `LOCAL_DEV.md`.
- Stripe code uses canonical `STRIPE_PRICE_ID_PREMIUM_*` and `STRIPE_PRICE_ID_PRO_*` names, with legacy `STRIPE_PRICE_ID_MONTHLY`, `STRIPE_PRICE_ID_ANNUAL`, and `STRIPE_PRICE_ID` aliases still supported.
- Deployed Fly has legacy/not-read vars: `APP_FRONTEND_URL`, `MARKETING_SITE_URL`, `STRIPE_CHECKOUT_SUCCESS_URL`, and `STRIPE_CHECKOUT_CANCEL_URL`.

## Deployed Backend Fly Secrets

Names currently deployed on Fly, status `Deployed`: `ADMIN_TOKEN`, `APP_BASE_URL`, `APP_FRONTEND_URL`, `APP_SESSION_SECRET`, `AUTOHEAL_ON_STARTUP`, `AUTO_BACKFILL_EVENTS_ON_STARTUP`, `AUTO_REPAIR_EVENTS_ON_STARTUP`, `DATABASE_URL`, `DATA_ENRICHMENT_QUEUE_BATCH_SIZE`, `DATA_ENRICHMENT_QUEUE_ENABLED`, `DATA_ENRICHMENT_QUEUE_MAX_SECONDS`, `DB_MAX_OVERFLOW`, `DB_POOL_SIZE`, `DB_POOL_TIMEOUT`, `EMAIL_DELIVERY_ENABLED`, `EMAIL_DIGEST_SCHEDULE_ENABLED`, `EMAIL_FROM`, `EMAIL_FROM_ALERTS`, `EMAIL_FROM_BILLING`, `EMAIL_FROM_SUPPORT`, `EMAIL_PROVIDER`, `EMAIL_REPLY_TO`, `EMAIL_REPLY_TO_SUPPORT`, `FMP_ALLOW_BOUNDED_TICKER_REFRESH`, `FMP_ALLOW_SYNC_USER_FETCH`, `FMP_API_KEY`, `FMP_BACKGROUND_REFRESH_ENABLED`, `FMP_HARD_LIMIT_PER_MINUTE`, `FMP_PLAN_CALLS_PER_MINUTE`, `FMP_SOFT_LIMIT_PER_MINUTE`, `FMP_TICKER_REFRESH_LOCK_TTL_SECONDS`, `FMP_TICKER_REFRESH_MAX_CALLS_PER_SYMBOL`, `FMP_TICKER_REFRESH_WATCHLIST_ONLY`, `FRONTEND_BASE_URL`, `FRONTEND_ORIGINS`, `FRONTEND_URL`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REDIRECT_URI`, `HEAVY_ROUTE_MAX_CONCURRENCY`, `MARKETING_SITE_URL`, `MASSIVE_API_KEY`, `OPTIONS_FLOW_PROVIDER`, `PASSWORD_RESET_FROM`, `POSTMARK_SERVER_TOKEN`, `QUOTE_LOOKUP_MAX_FETCH`, `STRIPE_CHECKOUT_CANCEL_URL`, `STRIPE_CHECKOUT_SUCCESS_URL`, `STRIPE_CUSTOMER_PORTAL_RETURN_URL`, `STRIPE_PRICE_ID_ANNUAL`, `STRIPE_PRICE_ID_MONTHLY`, `STRIPE_PRICE_ID_PREMIUM_ANNUAL`, `STRIPE_PRICE_ID_PREMIUM_MONTHLY`, `STRIPE_PRICE_ID_PRO_ANNUAL`, `STRIPE_PRICE_ID_PRO_MONTHLY`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `SUPPORT_EMAIL`.

## 1. Core App/Auth

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `APP_SESSION_SECRET` | `backend/app/auth.py`, `backend/app/routers/accounts.py`, `backend/app/security/startup_checks.py` | Required in production | dev fallback outside prod | Yes | none | Keep |
| `ADMIN_TOKEN` | `backend/app/auth.py`, `backend/app/routers/accounts.py`, `backend/app/security/startup_checks.py` | Required for token-admin paths | none | Yes | none | Keep |
| `ADMIN_EMAILS` | `backend/app/auth.py` | Optional | empty | Yes, admin role mapping | none | Keep if using email admin mapping |
| `APP_ENV` / `ENV` / `NODE_ENV` | auth, accounts/events routers, startup checks | Optional but recommended | inferred, or non-prod behavior | Yes | runtime aliases | Keep one canonical `APP_ENV=production`; keep `NODE_ENV` for frontend/build runtime |
| `APP_SESSION_TTL_SECONDS` | `backend/app/auth.py` | Optional | 30 days | Yes | none | Keep optional |
| `APP_SESSION_COOKIE_SAMESITE` | `backend/app/auth.py` | Optional | `lax` | Yes | none | Keep optional |
| `FRONTEND_ORIGINS`, `CORS_ALLOW_ORIGINS`, `FRONTEND_URL` | `backend/app/security/startup_checks.py` | Optional if defaults fit | built-in Walnut allowlist | Yes | overlapping CORS origin inputs | Keep `FRONTEND_ORIGINS`; deprecate `FRONTEND_URL` after CORS verification |
| `CT_ALLOW_ENTITLEMENT_HEADER` | `backend/app/entitlements.py`, startup checks | Optional, must be off in prod | `0` | Test/admin only in prod unsafe | none | Keep off in production |
| `CT_ALLOW_INSECURE_RESET_LINK_RESPONSE` | `backend/app/routers/accounts.py`, startup checks | Optional, must be off in prod | off | Dev/test only | none | Keep off in production |
| `CT_ALLOW_ADMIN_QUERY_TOKEN`, `CT_ALLOW_DEBUG_QUERY_TOKEN`, `CT_ENABLE_ADMIN_TOKEN_QUERY_AUTH`, `CT_ENABLE_DEBUG_TOKEN_QUERY_AUTH` | `backend/app/security/startup_checks.py` | Optional, must be off in prod | off | Legacy debug only | duplicate legacy flags | Do not set in production |
| `CT_DEFAULT_TIER` | `backend/app/entitlements.py` | Optional | free/normal default | Admin/test/dev use | none | Keep only for controlled test/staging |
| `LEGACY_WATCHLIST_OWNER_EMAIL` | `backend/app/auth.py` | Optional | built-in legacy email | Migration compatibility | none | Remove after legacy watchlists are attached |
| `RATE_LIMIT_ENABLED` | `backend/app/rate_limit.py` | Optional | true | Yes | none | Keep optional |
| `FEED_OUTCOME_ENQUEUE_LIMIT` | `backend/app/routers/events.py` | Optional | `100` | Yes | none | Keep optional |

## 2. Database

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `DATABASE_URL` | `backend/app/db.py`, migration docs | Required in production | `sqlite:////data/app.db` | Yes, production Postgres | none | Keep; production must stay Postgres |
| `DB_POOL_SIZE` | `backend/app/db.py`, migration script/docs | Optional | `8` app, `5` migration | Yes | none | Keep optional |
| `DB_MAX_OVERFLOW` | `backend/app/db.py`, migration script/docs | Optional | `4` app, `5` migration | Yes | none | Keep optional |
| `DB_POOL_TIMEOUT` | `backend/app/db.py`, migration script/docs | Optional | `2` app, `30` migration | Yes | none | Keep optional |
| `DB_POOL_RECYCLE_SECONDS` | `backend/app/db.py` | Optional | `1800` | Yes | none | Keep optional |
| `DB_CHECKOUT_SLOW_LOG_MS` | `backend/app/db.py` | Optional | `250` | Yes logging | none | Keep optional |
| `DB_SESSION_SLOW_LOG_MS` | `backend/app/db.py` | Optional | `2000` | Yes logging | none | Keep optional |
| `POSTGRES_DATABASE_URL`, `POSTGRES_BACKEND_URL`, `SQLITE_BACKEND_URL`, `POSTGRES_MIGRATION_TARGET_APPROVED` | `docs/postgres_migration_runbook.md`, migration tool | Manual migration only | none | No runtime | docs/script-only | Docs only |

## 3. Email/Postmark

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `EMAIL_DELIVERY_ENABLED` | `backend/app/services/email_delivery.py` | Required to send email | `false` | Yes | none | Keep |
| `EMAIL_PROVIDER` | email delivery/digest/intraday services | Required to choose provider | `resend` | Yes | provider selector | Keep, currently `postmark` if using `POSTMARK_SERVER_TOKEN` |
| `POSTMARK_SERVER_TOKEN` | `backend/app/services/email_delivery.py` | Required if provider is Postmark | none | Yes | none | Keep |
| `RESEND_API_KEY` | `backend/app/services/email_delivery.py` | Required if provider is Resend | none | Yes only if provider `resend` | alternative to Postmark | Keep only if using Resend |
| `SUPPORT_EMAIL` | `backend/app/services/email_templates.py`, `backend/app/routers/accounts.py` | Optional but canonical | `support@walnutmarkets.com` | Yes | also used for Stripe support display | Keep |
| `EMAIL_FROM_SUPPORT` | `backend/app/services/email_delivery.py` | Fallback only | none | Yes if template sender blank | category fallback | Keep as canonical fallback |
| `EMAIL_FROM_ALERTS` | `backend/app/services/email_delivery.py` | Fallback only | none | Yes if template sender blank | category fallback | Keep as canonical fallback |
| `EMAIL_FROM_BILLING` | `backend/app/services/email_delivery.py` | Fallback only | none | Yes if template sender blank | category fallback | Keep as canonical fallback |
| `EMAIL_REPLY_TO` | `backend/app/services/email_delivery.py` | Fallback only | none | Yes if template reply-to blank | canonical reply-to fallback | Keep |
| `EMAIL_FROM` | `backend/app/services/email_delivery.py` | Legacy fallback | none | Only if template/category sender blank | duplicate of category-specific vars | Deprecate after template sender verification |
| `PASSWORD_RESET_FROM` | `backend/app/services/email_delivery.py` | Legacy fallback | none | Only password-reset template if blank | duplicate of support sender | Deprecate after account template verification |
| `EMAIL_REPLY_TO_SUPPORT` | `backend/app/services/email_delivery.py` | Legacy/category fallback | none | Only if template reply-to blank | duplicate of `EMAIL_REPLY_TO` | Deprecate after verification |
| `EMAIL_REPLY_TO_ALERTS`, `EMAIL_REPLY_TO_BILLING` | `backend/app/services/email_delivery.py` | Optional fallback | none | Not deployed | category fallback | Do not add unless a category-specific reply-to is needed |
| `EMAIL_DIGEST_SCHEDULE_ENABLED` | digest script/docs | Optional cron gate | `0` | Yes for Fly cron | none | Keep if scheduled digests are enabled |
| `EMAIL_DIGEST_SCHEDULE_DRY_RUN` | digest script/docs | Optional | `0` | Yes | none | Keep optional |
| `EMAIL_DIGEST_SCHEDULE_LIMIT` | digest script/docs | Optional | `100` | Yes | none | Keep optional |
| `EMAIL_DIGEST_SCHEDULE_LOOKBACK_DAYS` | digest script | Optional | `1` | Yes | none | Keep optional |
| `EMAIL_ALERT_INTRADAY_ENABLED` | intraday service/docs | Optional | `false` | Yes | none | Keep optional |
| `EMAIL_ALERT_SCHEDULE_DRY_RUN` | intraday service/script/docs | Optional | `true` | Yes | none | Keep optional |
| `EMAIL_ALERT_MIN_SCORE` | intraday service/docs | Optional | `80` | Yes | none | Keep optional |
| `EMAIL_ALERT_MIN_FLOW_USD` | intraday service/docs | Optional | `250000` | Yes | none | Keep optional |
| `EMAIL_ALERT_SWEEP_LOOKBACK_MINUTES` | intraday service/script/docs | Optional | `60` | Yes | none | Keep optional |
| `EMAIL_ALERT_SWEEP_LIMIT` | intraday script/docs | Optional | `100` | Yes | none | Keep optional |
| `SMTP_HOST` | startup check only | Optional legacy warning | none | No current sender path | legacy SMTP marker | Remove after startup warning is updated |

Email policy status: implemented. `EmailTemplate.from_name`, `from_email`, and `reply_to` are authoritative. Env vars are fallback defaults only when template fields are blank.

## 4. Stripe

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `STRIPE_SECRET_KEY` | accounts router, billing readiness | Required for checkout/admin Stripe calls | none | Yes | none | Keep |
| `STRIPE_WEBHOOK_SECRET` | accounts router, billing readiness | Required for webhook sync | none | Yes | none | Keep |
| `STRIPE_PRICE_ID_PREMIUM_MONTHLY` | billing readiness, accounts admin | Required for Premium monthly | none | Yes | canonical | Keep |
| `STRIPE_PRICE_ID_PREMIUM_ANNUAL` | billing readiness, accounts admin | Required for Premium annual | none | Yes | canonical | Keep |
| `STRIPE_PRICE_ID_PRO_MONTHLY` | billing readiness, accounts admin | Required for Pro monthly | none | Yes | canonical | Keep |
| `STRIPE_PRICE_ID_PRO_ANNUAL` | billing readiness, accounts admin | Required for Pro annual | none | Yes | canonical | Keep |
| `STRIPE_PRICE_ID_MONTHLY` | billing readiness | Legacy fallback | none | Yes as Premium monthly fallback | duplicate of `STRIPE_PRICE_ID_PREMIUM_MONTHLY` | Remove after checkout smoke |
| `STRIPE_PRICE_ID_ANNUAL` | billing readiness | Legacy fallback | none | Yes as Premium annual fallback | duplicate of `STRIPE_PRICE_ID_PREMIUM_ANNUAL` | Remove after checkout smoke |
| `STRIPE_PRICE_ID` | billing readiness/tests | Legacy fallback | none | Not deployed | old default price | Do not add |
| `STRIPE_PRO_PRICE_ID`, `STRIPE_PRO_PRICE_ID_MONTHLY`, `STRIPE_PRO_PRICE_ID_ANNUAL` | billing readiness/tests | Legacy fallback | none | Not deployed | old Pro aliases | Do not add |
| `STRIPE_PRICE_PREMIUM_MONTHLY`, `STRIPE_PRICE_PREMIUM_ANNUAL`, `STRIPE_PRICE_PRO_MONTHLY`, `STRIPE_PRICE_PRO_ANNUAL` | searched | n/a | n/a | No code references | not used | Do not add |
| `STRIPE_CUSTOMER_PORTAL_RETURN_URL` | accounts router | Optional | generated app billing URL | Yes | app URL override | Keep; must be `app.walnutmarkets.com` |
| `STRIPE_CHECKOUT_SUCCESS_URL`, `STRIPE_CHECKOUT_CANCEL_URL` | deployed Fly only | Not read | generated in code | No repo use | legacy deployed vars | Remove after checkout smoke |
| `STRIPE_WEBHOOK_TOLERANCE_SECONDS` | accounts router | Optional | `300` | Yes | none | Keep optional |
| `BILLING_ENABLED`, `STRIPE_ENABLED`, `CT_BILLING_ENABLED`, `CT_STRIPE_ENABLED` | billing readiness/startup tests | Optional flags | inferred by readiness | Yes only if explicitly forcing billing on | duplicate enable flags | Prefer not setting; readiness can infer |
| `STRIPE_BUSINESS_NAME`, `STRIPE_SUPPORT_EMAIL`, `STRIPE_SUPPORT_URL`, `STRIPE_SUPPORT_PHONE`, `PUBLIC_BUSINESS_NAME`, `SUPPORT_URL`, `SUPPORT_PHONE` | accounts router | Optional Stripe business support display | none | Admin/readiness display | duplicate support metadata | Keep only if needed for admin readiness |
| `NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY` | searched | n/a | n/a | No frontend code references | not used | Do not add unless client-side Stripe is introduced |

## 5. FMP/Provider/Cache

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `FMP_API_KEY` | FMP client, ingest, ticker, price, news, fundamentals services | Required for provider calls | none | Yes | none | Keep |
| `FMP_ALLOW_SYNC_USER_FETCH` | `fmp_client.py`, `provider_usage.py` | Optional guardrail | `false` | Yes | preferred live-user-fetch switch | Keep if intentional |
| `FMP_ALLOW_BOUNDED_TICKER_REFRESH` | `ticker_hydration.py` | Optional | `false` | Yes | none | Keep if bounded refresh is enabled |
| `FMP_TICKER_REFRESH_MAX_CALLS_PER_SYMBOL` | `ticker_hydration.py` | Optional | `4` | Yes | none | Keep |
| `FMP_TICKER_REFRESH_LOCK_TTL_SECONDS` | `ticker_hydration.py` | Optional | `60` | Yes | none | Keep |
| `FMP_TICKER_REFRESH_WATCHLIST_ONLY` | `ticker_hydration.py` | Optional | `false` | Yes | none | Keep |
| `FMP_BACKGROUND_REFRESH_ENABLED` | enrichment worker/service | Optional | `true` | Yes | background kill switch | Keep |
| `FMP_PLAN_CALLS_PER_MINUTE` | `provider_usage.py` | Optional | `750` | Yes | replaces `FMP_CALLS_PER_MINUTE` | Keep |
| `FMP_SOFT_LIMIT_PER_MINUTE` | `provider_usage.py` | Optional | 80% of plan | Yes | replaces `FMP_CALLS_PER_MINUTE_SOFT_LIMIT` | Keep |
| `FMP_HARD_LIMIT_PER_MINUTE` | `provider_usage.py` | Optional | plan or legacy soft | Yes | replaces `FMP_CALLS_PER_MINUTE_HARD_LIMIT` | Keep |
| `FMP_CALLS_PER_MINUTE`, `FMP_CALLS_PER_MINUTE_SOFT_LIMIT`, `FMP_CALLS_PER_MINUTE_HARD_LIMIT`, `FMP_CALLS_PER_MINUTE_WARN_LIMIT` | `provider_usage.py` | Optional legacy aliases | defaults | Yes if set | legacy budget names | Do not add; migrate to canonical names |
| `FMP_PROVIDER_ENABLED` | `fmp_client.py` | Optional | `true` | Yes | inverse of disabled flag | Keep optional |
| `FMP_PROVIDER_DISABLED` | `provider_usage.py` | Optional | `false` | Yes | inverse of enabled flag | Prefer one switch in a later cleanup |
| `FMP_LIVE_FETCH_ON_PAGE_LOAD`, `FMP_LIVE_USER_ROUTES_ENABLED`, `FMP_CACHE_ONLY_USER_ROUTES`, `FMP_CACHE_MISS_LIVE_FETCH_ENABLED` | FMP client/provider usage | Optional legacy/alternate switches | mostly false/cache-only | Yes if set | overlap with `FMP_ALLOW_SYNC_USER_FETCH` | Do not add unless needed for rollback |
| `FMP_EXPLICIT_USER_REFRESH` | `provider_usage.py` | Optional | `false` | Admin/user refresh attribution | none | Keep optional |
| `FMP_PERSIST_USAGE_EVENTS` | `provider_usage.py` | Optional | `true` | Yes telemetry | none | Keep optional |
| `FMP_CACHE_MODE`, `CACHE_STORE_MODE`, `REDIS_URL`, `CACHE_HOT_ENABLED`, `CACHE_HOT_FALLBACK_MEMORY` | cache/provider usage | Optional | memory/no Redis | Yes if Redis cache used | cache mode aliases | Keep only if Redis/hot cache is enabled |
| `DATA_ENRICHMENT_QUEUE_ENABLED` | Fly config, enrichment script/docs | Required for cron behavior | `true` in Fly config | Yes | none | Keep |
| `DATA_ENRICHMENT_QUEUE_BATCH_SIZE` | Fly config, ingest script/docs | Optional | `50` | Yes | `FMP_ENRICHMENT_WORKERS` fallback | Keep |
| `DATA_ENRICHMENT_QUEUE_MAX_SECONDS` | Fly config, ingest script/docs | Optional | `45` | Yes | none | Keep |
| `DATA_ENRICHMENT_QUEUE_LOCK_DIR` | enrichment script | Optional | `/tmp/data_enrichment_queue.lock` | Yes | none | Keep optional |
| `FMP_ENRICHMENT_WORKERS` | `backend/app/ingest_run.py` | Legacy fallback | `25` | Yes if batch size unset | duplicate of queue batch size | Do not add |
| `MASSIVE_API_KEY`, `MASSIVE_BASE_URL`, `POLYGON_API_KEY`, `POLYGON_BASE_URL`, `OPTIONS_FLOW_PROVIDER` | price/options services | Optional by feature | Massive defaults to public URL | Yes for options/alternate price source | Massive/Polygon alternatives | Keep `MASSIVE_API_KEY` if options flow is live |

## 6. Google OAuth

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `GOOGLE_CLIENT_ID` | `backend/app/routers/accounts.py` | Required for Google OAuth unless stored in admin settings | admin setting fallback | Yes | setting can override | Keep |
| `GOOGLE_CLIENT_SECRET` | `backend/app/routers/accounts.py` | Required for Google OAuth | none | Yes | none | Keep |
| `GOOGLE_REDIRECT_URI` | `backend/app/routers/accounts.py` | Optional | generated app callback URL | Yes | app URL derived if absent | Keep only if Google console needs explicit override |

## 7. App/Domain URLs

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `FRONTEND_BASE_URL` | accounts router, billing reminders, email digests, notifications | Currently required by code paths | `http://localhost:3000` | Yes | app URL | Keep until backend code supports `PUBLIC_APP_URL` |
| `APP_BASE_URL` | accounts router, email templates, digests/notifications | Currently required by code paths | `https://app.walnutmarkets.com` in templates, API fallback in accounts | Yes | app URL | Keep; later consolidate to `PUBLIC_APP_URL` |
| `FRONTEND_APP_URL` | accounts router tests/fallback list | Optional | none | Yes if set | app URL alias | Do not add; prefer current deployed names until cleanup |
| `APP_FRONTEND_URL` | deployed Fly only | Not read | none | No repo use | mistaken alias for `FRONTEND_APP_URL` | Remove after verification |
| `MARKETING_SITE_URL` | deployed Fly only | Not read | none | No repo use | should be `PUBLIC_SITE_URL` | Remove after verification |
| `PUBLIC_SITE_URL` | email templates | Optional | `https://walnutmarkets.com` | Yes | marketing site URL | Add only if overriding default |
| `PUBLIC_APP_URL` | searched | Not read | n/a | No current repo use | desired future canonical | Add code support before setting as canonical |
| `PUBLIC_API_BASE_URL` | accounts router | Optional | `API_BASE` or production API | Yes for webhook URL/admin output | API URL alias | Keep optional |
| `API_BASE` | accounts router, frontend server/client | Optional | production API / localhost fallback | Yes | API URL alias | Keep for current code; future rename to `API_BASE_URL` needs code change |
| `API_BASE_URL` | searched | Not read | n/a | No current repo use | desired future canonical | Add code support before setting |
| `NEXT_PUBLIC_API_BASE` | frontend API lib, middleware, signals page | Optional but current frontend canonical | production API default | Yes | should become `NEXT_PUBLIC_API_BASE_URL` | Keep until frontend code is updated |
| `NEXT_PUBLIC_API_BASE_URL` | `LOCAL_DEV.md` only | Docs only today | none | No code use | desired future canonical | Do not rely on it until code is updated |
| `NEXT_PUBLIC_APP_URL` | frontend landing/legal/admin preview, accounts router fallback list | Optional | `https://app.walnutmarkets.com` | Yes | frontend public app URL | Keep optional in Vercel |
| `NEXT_PUBLIC_APP_BASE_URL` | admin email preview, backend fallbacks | Optional | app URL fallback | Yes | duplicate of `NEXT_PUBLIC_APP_URL` | Deprecate after preview code consolidation |
| `NEXT_PUBLIC_SITE_URL` | member metadata/admin preview | Optional | page default | Yes | marketing site URL | Keep optional |
| `FRONTEND_URL` | CORS startup checks, deployed Fly | Optional | built-in allowlist | Yes for CORS only | duplicate of `FRONTEND_ORIGINS` | Deprecate after CORS verification |
| `FRONTEND_ORIGINS` | startup CORS | Optional | built-in allowlist | Yes | preferred CORS list | Keep |

Remaining `walnut-intel.com` references:

- `frontend/middleware.ts` keeps `walnut-intel.com` and `www.walnut-intel.com` in `publicLandingHosts` for legacy host handling.
- `backend/app/security/startup_checks.py` keeps `app.walnut-intel.com`, `walnut-intel.com`, and `www.walnut-intel.com` in the default production CORS allowlist for compatibility.
- `backend/app/services/email_templates.py` contains legacy replacement rules to rewrite old `walnut-intel.com` senders/domains to Walnut Markets defaults.

These are intentional legacy compatibility references. They should not appear in newly rendered user-facing emails after template refresh.

## 8. Scheduler/Cron/Jobs

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `CRON_TZ` | `backend/crontab` | Required for cron timezone | `America/Los_Angeles` in file | Yes | none | Keep in crontab |
| `INGEST_JOB` | `backend/app/ingest_run.py` | Optional | `core` | Yes | GitHub `JOB_MODE` drives remote command | Keep optional |
| `INGEST_PAGES`, `INGEST_LIMIT`, `INGEST_SLEEP_S`, `INGEST_DRY_RUN` | House/Senate/ingest scripts | Optional | script defaults | Yes | shared ingest knobs | Keep optional |
| `INGEST_DO_HOUSE`, `INGEST_DO_SENATE`, `INGEST_BACKFILL`, `INGEST_DO_INSIDER`, `INGEST_ENRICH_MEMBERS`, `INGEST_DO_INSTITUTIONAL`, `INGEST_DO_SIGNALS_RECOMPUTE`, `INGEST_DO_PRICE_CACHE_WARM`, `INGEST_DO_FUNDAMENTALS_CACHE`, `INGEST_DO_WATCHLIST_CONFIRMATION_MONITORING` | `backend/app/ingest_run.py` | Optional | mostly enabled | Yes | job toggles | Keep optional |
| `INGEST_INSIDER_DAYS`, `INGEST_INSTITUTIONAL_DAYS`, `INGEST_SIGNALS_LOOKBACK_DAYS`, `INGEST_SIGNALS_BENCHMARK`, `INGEST_PRICE_CACHE_LOOKBACK_DAYS`, `INGEST_PRICE_CACHE_SYMBOL_LIMIT` | ingest scripts | Optional | script defaults | Yes | none | Keep optional |
| `CONGRESS_RECENT_DAYS`, `CONGRESS_RECENT_PAGES`, `CONGRESS_RECENT_LIMIT`, `CONGRESS_RECENT_SLEEP_S` | recent congress ingest | Optional | script defaults | Yes | none | Keep optional |
| `OUTCOME_REPAIR_LOOKBACK_DAYS`, `OUTCOME_REPAIR_RETRY_STATUSES`, `OUTCOME_REPAIR_LIMIT` | ingest/daily repair | Optional | script defaults | Yes | none | Keep optional |
| `WATCHLIST_CONFIRMATION_MONITORING_LOOKBACK_DAYS` | `backend/app/ingest_run.py` | Optional | `30` | Yes | none | Keep optional |
| `SCREEN_MONITORING_LIMIT` | `backend/app/ingest_run.py` | Optional | `25` | Yes | none | Keep optional |
| `GOVERNMENT_CONTRACT_*` | `backend/app/ingest_run.py` | Optional | script defaults | Yes for gov contract ingest | none | Keep optional |
| `PRIORITY_TICKER_PREWARM_SYMBOL_LIMIT`, `PRIORITY_TICKER_PREWARM_POPULAR_LIMIT`, `PRIORITY_TICKER_PREWARM_LANDING_SYMBOLS` | ingest/data enrichment | Optional | script defaults | Yes | none | Keep optional |
| `AUTOHEAL_ON_STARTUP`, `AUTO_REPAIR_EVENTS_ON_STARTUP`, `AUTO_BACKFILL_EVENTS_ON_STARTUP` | `backend/app/main.py`, migration docs | Optional | enabled | Yes | startup repair toggles | Keep during migration stabilization |
| `HEAVY_ROUTE_WAIT_SECONDS`, `HEAVY_ROUTE_MAX_CONCURRENCY`, `TICKER_CHART_MAX_CONCURRENCY`, `TICKER_WIDGET_MAX_CONCURRENCY` | `backend/app/main.py` | Optional | low concurrency defaults | Yes | performance knobs | Keep optional |
| `MAX_SYMBOLS_PER_REQUEST`, `TICKER_RESPONSE_CACHE_TTL_SECONDS`, `TICKER_CHART_DEDUPE_WAIT_SECONDS`, `TICKER_CHART_VOLUME_PROVIDER_FALLBACK`, `TICKER_FUNDAMENTALS_CACHE_TTL_SECONDS` | `backend/app/main.py` | Optional | code defaults | Yes | ticker route knobs | Keep optional |

## 9. Admin/Dev/Test-Only

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `NEXT_PUBLIC_API_DEBUG`, `NEXT_PUBLIC_CT_DEBUG_FETCH`, `CT_DEBUG_FETCH` | frontend API lib/docs | Optional | off | Debug only | duplicate debug toggles | Do not enable in production |
| `WALNUT_REQUEST_TRACE` | `backend/app/main.py` | Optional | enabled outside prod | Debug only | none | Keep off unless tracing incident |
| `PROVIDER_DEBUG_LOGS` | FMP news/snapshot services | Optional | false | Debug only | none | Keep off unless diagnosing provider |
| `PYTEST_CURRENT_TEST` | `backend/app/services/screener.py` | Test runtime | pytest-managed | No | test-only | Docs only |
| `TICKER_CONTENT_SQLITE_CACHE`, `TICKER_FINANCIALS_SQLITE_CACHE` | ticker services/tests | Optional | off | No for Postgres production | SQLite local/test fallback | Do not set in production |
| `CONGRESS_METADATA_CACHE_PATH` | congress metadata service | Optional | app default path | Local/cache override | none | Keep optional |
| `INSIDER_OUTCOME_DEBUG_EVENT_ID` | member performance service | Optional | test/debug id | Debug only | none | Do not set in production unless diagnosing |
| `SCREENER_PROVIDER_FALLBACK` | screener service/tests | Optional | off except tests | Debug/fallback | none | Keep off unless needed |
| `APP_TIMEZONE`, `PRICE_LOOKUP_MAX_PRIOR_FALLBACK_DAYS`, `PRICE_LOOKUP_DAILY_SERIES_MIN_DENSITY`, `QUOTE_CACHE_TTL_SECONDS`, `QUOTE_LOOKUP_MAX_FETCH`, `CIK_META_TTL_DAYS`, `CIK_META_MISS_TTL_DAYS`, `TICKER_META_TTL_DAYS`, `TICKER_META_MISS_TTL_DAYS` | services/tests | Optional | code defaults | Yes as tuning knobs | none | Keep optional |

## 10. GitHub Actions

| Var | Source files | Required | Default | Production use | Alias/duplicate | Recommended action |
| --- | --- | --- | --- | --- | --- | --- |
| `FLY_API_TOKEN` | `.github/workflows/daily_ingest.yml` | Required GitHub secret | none | Yes for scheduled remote ingest | none | Keep in GitHub Actions |
| `JOB_MODE` | `.github/workflows/daily_ingest.yml` | Workflow local variable | schedule/input derived | Yes | not a secret | Docs only |
| `GITHUB_PATH` | `.github/workflows/daily_ingest.yml` | GitHub-provided | n/a | Yes | built-in | Docs only |
| `GITHUB_TOKEN` | `.github/workflows/security.yml` | GitHub-provided | n/a | Yes for gitleaks action | built-in | Docs only |
| `GITLEAKS_ENABLE_COMMENTS`, `GITLEAKS_ENABLE_UPLOAD_ARTIFACT` | `.github/workflows/security.yml` | Optional action env | false | Security workflow | action knobs | Keep in workflow |

## 11. Unknown/Legacy Cleanup

### Safe to unset after confirming no external/non-repo consumers

These deployed Fly names are not read anywhere in the repo:

```bash
fly secrets unset APP_FRONTEND_URL MARKETING_SITE_URL STRIPE_CHECKOUT_CANCEL_URL STRIPE_CHECKOUT_SUCCESS_URL -a congress-tracker-api
```

### Remove after checkout smoke verifies canonical Stripe prices

Canonical premium price IDs are already deployed. These legacy names duplicate them and are fallback aliases:

```bash
fly secrets unset STRIPE_PRICE_ID_ANNUAL STRIPE_PRICE_ID_MONTHLY -a congress-tracker-api
```

### Remove after Admin Email template sender fields are verified

Verify every enabled template has nonblank `from_name`, `from_email`, and `reply_to` where intended. Then these fallback-only duplicates can be removed:

```bash
fly secrets unset EMAIL_FROM PASSWORD_RESET_FROM EMAIL_REPLY_TO_SUPPORT -a congress-tracker-api
```

### Keep

Keep deployed secrets that are actively read and production-relevant: `ADMIN_TOKEN`, `APP_BASE_URL`, `APP_SESSION_SECRET`, `DATABASE_URL`, `DATA_ENRICHMENT_QUEUE_*`, `DB_*`, `EMAIL_DELIVERY_ENABLED`, `EMAIL_FROM_ALERTS`, `EMAIL_FROM_BILLING`, `EMAIL_FROM_SUPPORT`, `EMAIL_PROVIDER`, `EMAIL_REPLY_TO`, `FMP_*` canonical guardrails, `FRONTEND_BASE_URL`, `FRONTEND_ORIGINS`, `GOOGLE_*`, `MASSIVE_API_KEY`, `OPTIONS_FLOW_PROVIDER`, `POSTMARK_SERVER_TOKEN`, `QUOTE_LOOKUP_MAX_FETCH`, `STRIPE_CUSTOMER_PORTAL_RETURN_URL`, `STRIPE_PRICE_ID_PREMIUM_*`, `STRIPE_PRICE_ID_PRO_*`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, and `SUPPORT_EMAIL`.

### Unsure / verify first

- `FRONTEND_URL`: read only for CORS; remove only after `FRONTEND_ORIGINS` covers all production origins.
- `FRONTEND_BASE_URL` and `APP_BASE_URL`: both are active. Consolidate in code before removing either.
- `GOOGLE_REDIRECT_URI`: optional but may be intentionally pinned to match Google Console config.
- `EMAIL_FROM_BILLING`: active fallback but only matters if billing templates have blank sender fields.
- `HEAVY_ROUTE_MAX_CONCURRENCY`, `AUTOHEAL_ON_STARTUP`, `AUTO_BACKFILL_EVENTS_ON_STARTUP`, `AUTO_REPAIR_EVENTS_ON_STARTUP`: operational tuning; keep unless runtime behavior is confirmed safe without them.

## Suggested Next Cleanup PR

1. Add backend URL helper functions that resolve `PUBLIC_SITE_URL`, `PUBLIC_APP_URL`, and `API_BASE_URL`, while keeping `APP_BASE_URL`, `FRONTEND_BASE_URL`, `PUBLIC_API_BASE_URL`, and `API_BASE` as one-release fallbacks.
2. Add frontend support for `NEXT_PUBLIC_API_BASE_URL`, falling back to `NEXT_PUBLIC_API_BASE` for one release.
3. Replace `SMTP_HOST` startup warning with provider-aware email readiness based on `EMAIL_PROVIDER` and the selected provider token.
4. Emit startup summaries with category booleans only, for example `email_config_loaded support=true alerts=true billing=true`, `stripe_config_loaded checkout=true webhook=true`, and `provider_config_loaded fmp=true`.
