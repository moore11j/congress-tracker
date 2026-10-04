# Strategy Monitoring, API, and Webhook Roadmap

## Current status — October 4, 2026

Reconciled against `80aaa7f7`. This is a supporting plan under the [product roadmap](roadmap.md), specifically R5 (monitoring) and R8 (developer access). The original August 2 proposal is retained below as history, not an implementation checklist.

| August proposal | Current code evidence | Remaining work |
|---|---|---|
| Admin draft review and publish-only public catalog | [Strategy router](../backend/app/routers/strategies.py) and [strategy service](../backend/app/services/strategies.py) implement public/admin separation and publication controls. | Validate current published data, diagnostics and freshness; code presence is not a live audit. |
| Strategy watches | Implemented as `StrategySubscription`, not the proposed `strategy_watches` table. [Subscription service](../backend/app/services/strategy_subscriptions.py) requires a published strategy with an active prospective version. | Verify follow/downgrade/opt-out and delivery end to end; avoid introducing a duplicate watches system. |
| Canonical strategy event storage | `StrategyEvent` and `StrategyEventDelivery` exist in [models](../backend/app/models.py), including unique event dedupe keys and delivery state. | Add customer read contracts only after access, pagination and retention are specified. |
| Email delivery | Existing queue/worker code has eligibility checks, retries, provider idempotency keys and admin delivery/status routes. | Observe actual delivery/return behavior; queued or attempted is not delivered. |
| Versioned prospective strategies | [Version service](../backend/app/services/strategy_versions.py) and [scheduler](../backend/app/services/strategy_scheduler.py) exist. | Preserve review/approve/activate boundaries, as-of dates and historical versus prospective distinctions. |
| Customer API keys and outbound webhooks | Still future features in [current plan copy](../frontend/lib/planBenefits.ts). Internal application APIs and billing/provider callbacks are separate. | Implement scoped keys, rate limits, entitlement-safe event access, signed webhook delivery and replay controls under R8. |

Current subscription routes in `backend/app/routers/strategies.py` are:

- `GET /api/strategies/{slug}/subscription`
- `PUT /api/strategies/{slug}/subscription`
- `DELETE /api/strategies/{slug}/subscription`

Default subscribed event types are `trade_added`, `trade_exited` and `rebalance_completed`. The `/watch` paths and `strategy_added_position` names below were proposals, not the current contract. Admin operations expose queue/delivery status and version approval/activation; these must not become anonymous developer APIs.

**Next acceptance:** verify follow → eligible prospective event → deduplicated delivery → return to fresh holdings using safe fixtures and authorized opted-in accounts; exercise disabled delivery, retries, downgrade and unsubscribe. Then define the customer event schema and rollout separately. No brokerage execution is introduced by this plan.

## Historical proposal — August 2, 2026

## Near-term review flow

- Persist refreshed strategy runs as `draft`.
- Review drafts in the authenticated admin Strategies panel.
- Publish only curated strategies after methodology, diagnostics, holdings, and disclosures are reviewed.
- Keep public `/api/strategies` limited to `published` definitions.

## User monitoring model

Reuse the existing `monitoring_sources` entitlement concept. A monitored strategy should count like other monitored sources, alongside watchlists and saved screens.

Proposed table:

- `strategy_watches`
  - `id`
  - `user_id`
  - `strategy_id`
  - `status`: `active`, `paused`, `deleted`
  - `notification_channels_json`: email, in-app, future webhook
  - `event_types_json`: additions, removals, weight changes, methodology changes
  - `created_at`
  - `updated_at`
  - unique active watch on `(user_id, strategy_id)`

## Copy-trade event feed

Generate events from strategy holding diffs after each refresh:

- `strategy_added_position`
- `strategy_removed_position`
- `strategy_weight_changed`
- `strategy_methodology_changed`
- `strategy_refresh_failed`

Store derived events before notification delivery so API responses and emails/webhooks share the same source of truth.

Proposed table:

- `strategy_events`
  - `id`
  - `strategy_id`
  - `run_id`
  - `event_type`
  - `symbol`
  - `event_date`
  - `payload_json`
  - `created_at`

## API phases

Phase 1:

- `GET /api/strategies`
- `GET /api/strategies/{slug}`
- `POST /api/strategies/{slug}/watch`
- `DELETE /api/strategies/{slug}/watch`
- `GET /api/account/strategy-watches`

Phase 2:

- `GET /api/strategies/{slug}/events`
- `GET /api/account/strategy-events`
- API keys for Pro users or developer accounts.

Phase 3:

- Webhook endpoint registrations.
- HMAC-signed delivery.
- Retry queue with exponential backoff.
- Delivery logs and replay.

## Trading guardrails

- Walnut should emit strategy signals, not broker orders, until a separate brokerage integration is reviewed.
- Webhooks must include disclaimers and exact methodology/run identifiers.
- Never send transaction-date Congress or insider events as actionable strategy signals unless they are explicitly labeled theoretical.
- Include execution timing, slippage assumptions, and current-holding source in every machine-readable payload.
- Automated trading integrations need per-user acknowledgements, risk limits, and kill switches before launch.
