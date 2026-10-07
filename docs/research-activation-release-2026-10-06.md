# Research writer and signup continuation release — October 6, 2026

## Approved scope

Owner approved the recommended combined release: prepared research topic/style fixes, selected-stock signup continuation, and truthful follow/email behavior. No publication of the two review drafts, source-data reconciliation, email preference mutation, pricing/entitlement changes or new spending is included.

## Changes

- Writer topic detection uses the requested question, angle, keywords and intent rather than incidental additional instructions. Saying “not another ownership brief” no longer switches a fundamentals brief to institutional ownership.
- Style checks exclude table delimiter/URL punctuation from prose dash counts, allow ordinary finance compounds, and recognize named holders from verified SEC comparisons as specific analysis. Source/claim guards remain in place.
- Welcome page makes the selected stock the primary action, retains the full safe return URL including follow intent, attribution and selected tab, and collapses the optional new-stock search. Generic signup retains stock discovery. Continuing a follow intent explains that it completes the save.
- Follow copy and tooltip accurately explain that watchlist emails depend on enabled notification settings, plan and delivery preferences. Existing account defaults, subscriptions and preferences are unchanged. This fixes a false promise, not the delivery engine.

## Verification

- 31 focused backend tests pass on local Python 3.14.2 (editorial, ownership, signup/reporting). Production Docker remains Python 3.12.
- 22 focused frontend tests pass (signup/welcome render, acquisition, follow and funnel analytics). Tests cover both email preference states, deduplication, dotted/hyphenated tickers, external-return rejection, follow/UTM/hash preservation and no automatic follow for a plain ticker return.
- Prior broader research suite has seven failures reproduced against unchanged HEAD; no full-suite pass claimed. See the [sample review](research-and-activation-review-2026-10-05.md).
- Standalone TypeScript check (`tsc --noEmit --incremental false`) passes. Production `npm run build` passes (67 static pages). Only the existing stale Browserslist database warning was emitted. Deployment receipts follow below.

## Deployment receipt

Pending. Shared working-tree edits from other tasks are excluded from this release. ANET and ACN remain review drafts; no public content or emails sent by this release task.
