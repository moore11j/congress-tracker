# Strategy Monitoring, API, and Webhook Roadmap

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
