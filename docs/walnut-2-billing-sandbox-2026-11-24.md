# Walnut 2.0 parallel site and billing sandbox

November 24, 2026. Owner requested a separate Walnut 2.0 build, Stripe product integration and a main-page switch only after testing. The credit proposal is being implemented for preview, not activated as live pricing. Owner deferred signing into Stripe; no catalog objects, credentials, webhooks or subscriptions have been created in the account during this task.

## Parallel delivery

Draft PR846 retains the existing homepage and production checkout. `/ask` is the chat workspace; `/ask/credits` adds a Plans & Credits preview. Company research remains the existing permission-aware ticker renderer. The new page explains the proposed credit allowances, displays an actual sandbox balance only when connected, accepts custom dollar amounts and opens only test Checkout sessions. Without the sandbox backend it explicitly disables purchases. No placeholder balance is represented as a real grant.

The frontend preview currently still reads existing research through its established API path. It is not a fully isolated copy of all production services. Credit requests have their own exact Next.js route and an explicit sandbox backend setting, with no fallback to the production API. Vercel production mode rejects this billing proxy. The backend routes are absent unless explicitly enabled and require a nonproduction application environment plus distinct, explicitly named sandbox account and ledger databases. Database naming is an additional misconfiguration guard; deployment still must verify the actual hosts, account isolation and credentials.

## Stripe catalog

Use an isolated [Stripe sandbox](https://docs.stripe.com/sandboxes). Existing live Premium/Pro products and subscribers stay unchanged. Proposed catalog:

| Product | Price | Included credits |
|---|---:|---:|
| Walnut Premium | $24.95 monthly or $249.50 yearly | 1,500 each month |
| Walnut Pro | $39.95 monthly or $399.95 yearly | 4,000 each month |
| Walnut Research Credits | $0.01 per credit, one-time | Purchased quantity |

Free has no paid Stripe product and proposes 100 monthly credits. The catalog installer creates three products and five prices, using stable product identifiers and price lookup keys. It validates existing objects before reuse. Annual billing does not imply an annual upfront credit grant. Subscription checkout and invoice-qualified monthly grants are not connected yet.

For a $10 purchase, the credit Checkout line item is the one-cent price with quantity 1,000. Custom amounts are parsed as integer cents, avoiding floating point rounding. The sandbox accepts $5 to $1,000; that upper bound is a test-stage purchase limit, not an approved final business policy. No automatic refill or negative balance.

Stripe's [billing credits](https://docs.stripe.com/billing/subscriptions/usage-based/billing-credits) apply to metered subscription invoices. Walnut needs an application ledger that reserves credits before an answer starts, so the integration uses ordinary recurring subscription prices plus a separate one-time credit product and Walnut's own ledger.

## What is implemented

- Separate monthly and purchased grants, integer credit accounting, monthly expiration and purchased carry-forward. Monthly grants have an internal helper only; no public grant endpoint or automatic grant scheduler.
- Atomic reserve, spend and release operations. Monthly grants are consumed first. Failed work restores the original grant without extending expiry. Job identity is bound to account and cost. The Ask model runner is not wired to these operations yet.
- Authenticated, verified-email sandbox checkout; strict origin check, account-bound idempotency, bounded amounts and checkout rate limiting. Existing bound sessions are reused on retry. A verified signed webhook retrieves the paid session and checks price, quantity, currency, totals and the local order before granting.
- Duplicate and concurrent fulfillment is idempotent. Checkout return parameters never grant credits. Asynchronous unpaid sessions wait for payment success. Refunds/disputes are persisted even before fulfillment and place the account on billing hold. Actual refund debits and resolution need reconciliation tooling before live billing; this implementation does not silently reverse already-used credits.
- Explicit sandbox keys only, a pinned Stripe API version, no live credential fallback, no redirected API requests, bounded webhook payload/signature age, and rejection of live or connected-account events.

Stripe requires fulfillment that tolerates repeated and concurrent webhook delivery; see [Checkout fulfillment](https://docs.stripe.com/checkout/fulfillment). A payment-side success with a local timeout can still need reconciliation. Do not manually mint another order to bypass an uncertain payment. There is no automatic live schema migration.

## Connect when the owner is ready

1. Sign into the Walnut Stripe account and create/select an isolated sandbox. Record the account/sandbox identity without copying customer records or secrets into repository files.
2. Configure `WALNUT_STRIPE_SANDBOX_SECRET_KEY` securely in the isolated backend environment. Only `sk_test_` or `rk_test_` credentials are accepted. Do not reuse a live key or paste a secret into chat. Persistent key creation is a separate owner-controlled access step.
3. From `backend`, set `PYTHONPATH=.` and run `python scripts/setup_research_credit_sandbox.py` to print the plan. Run the same command with `--apply` only against the confirmed sandbox. The receipt contains public price IDs, not credentials.
4. Provision a separate backend with fresh sandbox accounts, its own authentication signing secrets, disabled production jobs/delivery/provider collection and separate account/ledger databases. Do not copy live credentials or customer records. The sandbox frontend must authenticate against that same sandbox backend; the current production-backed preview login is not sufficient for a payment test.
5. Configure the variables below through the hosting secret/config mechanism. Explicitly install only the ledger schema in the isolated database using `credit_store(isolated_database_url()).install_schema()` from `app.routers.research_credits`. This does not install account tables or provision infrastructure; those are separate sandbox setup steps.
6. Register the sandbox endpoint `/api/research-credits/stripe-webhook` for `checkout.session.completed`, `checkout.session.async_payment_succeeded`, `charge.refunded` and `charge.dispute.created`. Store its dedicated signing secret securely. Use the pinned `2026-11-18.endive` API version for this integration. Do not redirect existing production webhooks.
7. Enable only the chosen frontend preview environment, test authenticated checkout with Stripe test payment methods, verify the ledger and replay event delivery. Keep its URL separate from the live main page.

| Environment | Required configuration |
|---|---|
| Sandbox backend | `APP_ENV=sandbox`, `WALNUT_CREDITS_SANDBOX_ENABLED=1` |
| Account database | `DATABASE_URL`, database name ending `_sandbox` or local file ending `.sandbox.db` |
| Separate ledger | `WALNUT_CREDITS_DATABASE_URL`, distinct sandbox database |
| Stripe sandbox | `WALNUT_STRIPE_SANDBOX_SECRET_KEY`, `WALNUT_STRIPE_SANDBOX_CREDIT_PRICE_ID`, `WALNUT_STRIPE_SANDBOX_WEBHOOK_SECRET` |
| Allowed browser origin | `WALNUT_CREDITS_SANDBOX_ORIGIN`, exact separate frontend origin |
| Frontend preview only | `WALNUT_CREDITS_SANDBOX_ENABLED=1`, `WALNUT_CREDITS_SANDBOX_API_URL`, explicit sandbox backend origin |

The new balance/checkout API is `/api/ask/credits/{balance,checkout}` on the frontend. It forwards to `/api/research-credits/{balance,checkout}` on the isolated backend. The server rejects account identifiers and entitlement hints in the checkout body. The payment recipient is always the authenticated local order owner. The production-host denylist is defense in depth, not a substitute for inspecting infrastructure settings.

## Remaining before a main-page switch

1. Sandbox infrastructure/auth/catalog/webhook setup and real Stripe test-mode payment evidence. Owner chose to do Stripe sign-in later.
2. Subscription invoice qualification, renewal/cancellation/grace handling, monthly grants for annual customers, upgrade/downgrade rules, refunds/dispute reconciliation and abandoned reservation recovery. No live price migration or current subscriber changes are authorized by this preview.
3. PostgreSQL concurrency and failure qualification in a separate test database. Current transaction tests use actual SQLite connections and threads, not the migration replay database.
4. Bounded, sourced AI answers with access checks before every tool/model call, visible credit quotes, measured cost caps and failure release; conversation ownership, retention and deletion. No model call was made for this task.
5. Free/Premium/Pro/admin acceptance, mobile/desktop layout, billing returns, source freshness, network load and remaining migration consumer gates.
6. Review the complete candidate and then change the main app entry with a tested rollback. Preserve legacy routes and existing subscriber access. Publishing this draft preview does not perform that cutover.

## Validation

Local results and exact preview/CI receipts are appended after the final check. Tests use synthetic payment data and isolated local databases. They cannot establish actual Stripe account configuration, taxes, live delivery or net profitability. Rate limiting currently uses the application's single-process limiter; a shared limiter is required before a multi-instance billing deployment.

Local validation: 48 backend checks pass (32 credit/sandbox plus 16 existing watchlist access checks), 49 frontend cases pass including middleware, and TypeScript passes. Python 3.14.2 locally; Linux Python 3.12 CI is pending. Catalog dry run outputs three products/five prices without network access. No real Stripe payment or PostgreSQL credit-concurrency qualification yet.
