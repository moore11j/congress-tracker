# Subscription credits and account balance

November 24, 2026. Owner requested top-up webhooks, automatic Premium/Pro credits and an immediately updated account balance before configuring Stripe. Implemented in the separate PR846 preview and isolated billing backend. No production Stripe account, credentials, webhooks, payments or schema were configured.

## Payment and subscription flow

Top-ups retain signed webhook fulfillment and now also have an authenticated `/api/research-credits/confirm` route. Checkout returns the test session ID, then the server checks that it belongs to the signed-in account and retrieves it from Stripe. The same idempotent fulfillment operation verifies paid status, exact price/quantity/amount and grants once. A forged return URL, another account's session, unpaid session or duplicate webhook cannot add credits. The balance is returned after the transaction commits, and the client immediately refreshes its shared balance. Webhooks still fulfill when the customer never returns.

The subscription handler retrieves the invoice and subscription from Stripe. It resolves ownership only through the existing verified `UserAccount.stripe_customer_id` binding; the normal subscription checkout persists that binding before redirecting. It never uses webhook email or user metadata as ownership authority. The existing subscription webhook remains responsible for feature entitlements. Both endpoints belong to the same isolated sandbox account environment; this change does not replace the production billing handler.

An accepted `invoice.paid` event must represent a complete, paid, non-prorated initial or renewal period at an explicitly configured catalog price. Premium grants 2,000 credits and Pro 4,000. A durable invoice coverage record and wallet transaction prevent duplicate or overlapping grants. Subscription status events alone do not create paid coverage. Trials, unpaid invoices, unrecognized prices, partial/truncated line sets, discounts, credit notes and prorated changes cannot mint a full allowance.

Annual paid coverage unlocks one month at a time. Calendar boundaries retain the original anchor, including January 31 and February month ends; missed months never accumulate. Balance reads allocate a due month automatically. Already-granted or absent coverage uses read-only checks, so ordinary polling does not update the wallet. The optional `backend/scripts/grant_research_subscription_months.py` job can run hourly with `--apply` in the configured sandbox; by default it prints a plan and touches no database. Each batch checks up to 1,000 account IDs and returns a continuation cursor. No scheduler was installed.

End-of-period cancellation leaves the paid period intact. A terminal cancellation prevents future monthly grants and cannot be reopened by a stale event. Already-issued monthly credits retain their original expiry. Refunds/disputes place the linked wallet on hold, including when delivered before the paid invoice; no further grants or spending occur until reviewed reconciliation. Purchased credits are kept separate from monthly grants. An initial paid allocation replaces unused Free/welcome credits.

Plan changes with proration, discounts, credit notes, refund resolution, reinstatement and existing-subscriber migration remain release gates. Do not enable prorated plan changes in the sandbox portal and interpret a held invoice as success. No duplicate full allowance is granted as a workaround. The original Free onboarding helper is still not connected to automatic account onboarding.

## Balance display and timing

- The Ask Walnut header shows a credit balance linked to Plans & Credits. Account Billing also shows the balance when the isolated billing frontend is configured; the production configuration does not render it.
- Plans & Credits separates available, monthly, purchased and reserved credits. The composer uses the same shared balance and shows the balance after the quoted task.
- All mounted displays share one request loop. Payment confirmation triggers an immediate refresh. Other webhook changes appear on the next two-second refresh, plus network latency. This is automatic near-real-time refresh, not a promise of zero-latency Stripe settlement.
- Hidden tabs pause polling; focus/visibility and account-change events refresh it. Logging out clears the old balance; late responses cannot repopulate another account's balance. Failed requests show unavailable instead of a stale value presented as current.
- AI execution remains unconnected. Its eventual reserve/spend/release client must call the exported refresh helper or dispatch `walnut:credits-changed` after each confirmed mutation, and allocate any due month before spending.

## Stripe configuration when ready

Use the isolated setup described in [billing sandbox](walnut-2-billing-sandbox-2026-11-24.md). Keep production webhooks, customer records, keys and existing subscriber prices untouched.

1. Install the approved `walnut_research_v3` sandbox catalog and an isolated account/ledger schema. The new subscription/coverage tables are additive. Older pre-clarification job tables still need a reviewed additive migration; `create_all` does not alter them.
2. Configure the four new ledger price bindings: `WALNUT_STRIPE_SANDBOX_PREMIUM_MONTHLY_PRICE_ID`, `WALNUT_STRIPE_SANDBOX_PREMIUM_ANNUAL_PRICE_ID`, `WALNUT_STRIPE_SANDBOX_PRO_MONTHLY_PRICE_ID`, `WALNUT_STRIPE_SANDBOX_PRO_ANNUAL_PRICE_ID`. Each is the matching v3 price ID from the installer receipt. Top-ups retain `WALNUT_STRIPE_SANDBOX_CREDIT_PRICE_ID`.
3. In that isolated backend only, configure the existing subscription checkout with the same four prices through `STRIPE_PRICE_ID_PREMIUM_MONTHLY`, `STRIPE_PRICE_ID_PREMIUM_ANNUAL`, `STRIPE_PRICE_ID_PRO_MONTHLY`, `STRIPE_PRICE_ID_PRO_ANNUAL`. Its `STRIPE_SECRET_KEY` must be the sandbox's test key. The new credit adapter independently requires its explicit test-only `WALNUT_STRIPE_SANDBOX_SECRET_KEY`. Do not copy or change live deployment values.
4. Retain the existing sandbox subscription endpoint `/api/billing/stripe/webhook` for account entitlement synchronization. Register a separate destination at `/api/research-credits/stripe-webhook` with its own `WALNUT_STRIPE_SANDBOX_WEBHOOK_SECRET` and API version `2026-11-18.endive`.
5. Send the credit endpoint: `checkout.session.completed`, `checkout.session.async_payment_succeeded`, `invoice.paid`, `invoice.payment_failed`, `customer.subscription.created`, `customer.subscription.updated`, `customer.subscription.deleted`, `customer.subscription.paused`, `customer.subscription.resumed`, `charge.refunded`, `charge.dispute.created`. Configure only sandbox events. Unknown customer bindings return a retryable failure so an invoice cannot be silently credited to the wrong account.
6. Configure the separate frontend's credit proxy and account authentication against the same sandbox backend. Test initial Premium/Pro subscriptions, monthly renewal, annual month rollover, top-ups, webhook-before/after-return races and payment failures using Stripe test methods before live review.

Stripe documents [invoice-based subscription provisioning](https://docs.stripe.com/billing/subscriptions/webhooks) and [combined webhook/return fulfillment](https://docs.stripe.com/checkout/fulfillment.md?ui=full-page). Invoice parsing uses the current [invoice](https://docs.stripe.com/api/invoices/object) and [line-item](https://docs.stripe.com/api/invoice-line-item/object) parent/pricing fields, not legacy top-level subscription fields. The account's actual sandbox configuration is still unverified.

## Validation and limits

Local Python 3.14: 94 combined backend cases passed, then the final 26 subscription cases passed after adding currency and credit-note rejection (96 distinct cases across those runs). The 21 chat/credit/balance frontend cases and generated-route TypeScript pass. Signed webhook tests use a real isolated SQLite account and ledger database; checkout-return tests exercise HTTP origin, ownership, duplicate fulfillment and immediate balance reads. Concurrency uses real SQLite transactions. PostgreSQL qualification remains open.

Browser QA used an explicitly synthetic local account and balance server, with no provider calls. At 390 pixels, the header and composer changed from 2,000 to 2,500 without page navigation or reload; the quote changed from 1,975 to 2,475 remaining. Desktop credit details show 2,000 monthly plus 500 purchased. Mobile has no horizontal overflow. These fixtures prove rendering and refresh behavior, not a real Stripe payment. Screenshots are retained in the owner workspace as `artifacts/walnut-credit-balance-mobile-nov24.jpg` and `artifacts/walnut-credit-balance-desktop-nov24.jpg`. Local servers and temporary viewport settings were stopped/reset.

Exact CI and preview receipts follow in the latest task checkpoint. No production deployment, subscription change, Stripe object creation, payment, AI call or customer send occurred. The institutional replay was not modified or restarted.
