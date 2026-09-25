# Approved signup and reporting fixes — September 21, 2026

> Release update (September 25): this historical implementation report is included in the approved rollout. See `product-growth-release-2026-09-25.md` for current validation and deployment details.


## State

Implemented locally and tested. Not committed, pushed or deployed in this pass. No production account, entitlement, billing or analytics-provider settings were changed. The separately authorized Reddit edits and GSC indexing requests are live; see `distribution-actions-2026-09-21.md`.

## Signup

- Email/password registration no longer requires names or billing address. Optional older-client profile fields remain supported and omitted fields do not erase an existing profile.
- Password rules, rate limiting, verification emails, password reset and verification-before-checkout remain in place. Stripe's existing checkout/tax address collection is unchanged.
- Two-field form, show/hide password, visible requirements and collapsed password-reset controls.
- Explicit stock/follow/return destinations are preserved for email and Google. Generic registration opens `/welcome` with a stock search instead of account settings. The welcome page is noindex.
- Backend returns `is_new_user`; adding email authentication to an existing account does not produce a canonical `signup_completed` event.
- Added consent-dependent `signup_submitted`, `signup_validation_failed` and `signup_failed` events. Form display remains `signup_started`. No entered field values are included in events.

## Reporting

- Product events no longer inflate page views, trends or top pages.
- Totals cover the entire period rather than only the displayed top routes. Least-viewed routes are selected from all tracked routes, not the top-page subset.
- Known account IDs, browser sessions and views lacking a session ID are separate; unidentified views do not create fictitious users. The compatibility `unique_users` field now means known account IDs only.
- Admin IDs, explicitly configured test IDs (`ANALYTICS_EXCLUDED_USER_IDS`, comma-separated numeric IDs), admin routes and sessions identified with excluded accounts are filtered by default. An admin-only include-internal toggle remains available. Anonymous internal activity cannot always be identified.
- Individual URL paths are available in addition to normalized route families.
- Current/new/active/verified-new account counts exclude deleted/suspended records and default internal exclusions. Last-seen activity is not GA4's engagement definition.
- Live-payment counts require a paid invoice with explicit `livemode: true` and positive `amount_paid`. Test, zero-payment, missing-evidence, unmatched and refund-marked invoices are reported separately. Counts are period payment evidence, not current subscriptions or net revenue; no currencies are added together in this new report.
- Product-action reach is explicitly not an ordered cohort funnel. Browser consent and failed session verification can cause differences from authoritative account records.
- Provider configuration-presence indicators expose no secrets and do not claim successful GA4 ingestion.
- Legacy Business Overview labels now disclose raw account inventory, complimentary access, estimated plan value and unreconciled invoice totals.

## Verification

- Frontend TypeScript: passed (`npx tsc --noEmit --incremental false`).
- Existing frontend auth/analytics checks: 26 passed.
- New interaction tests: 2 passed (two-field request, validation, show password, post-signup navigation, email/Google return intent).
- New backend tests: 4 passed (minimal registration/security, existing-profile preservation, report identities/filtering/totals, live-payment evidence).
- Existing account/Stripe/funnel/provider suite: 147 passed, three assertions failed, one temp-directory permission error. The three failures also reproduced after loading the original HEAD account module in a separate test process: old market-pressure persistence expectation, export price expectation, and watchlist feature-gate behavior. They were not changed here. The concurrent Stripe test passed when rerun with a new workspace-local temporary directory.
- Browser: desktop and 390px-wide signup layout, empty-form validation and welcome page verified locally. No real account or payment was created.
- Diff whitespace checks passed. Existing unrelated work was preserved.

## Production follow-up

Deploy the backend before the frontend because the old backend requires the removed billing fields. After rollout, check the report's provider configuration indicators and perform a consented real signup/session check. Confirm a genuine live payment against the ledger and provider delivery when one occurs; do not count or manufacture a test-mode payment as a live conversion. This pass does not certify current production GA4 payment-secret configuration or end-to-end GA4 receipt.
