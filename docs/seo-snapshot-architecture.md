# SEO Snapshot Architecture Note

## Current Request Path

Public entity pages currently render through the normal app routes:

- `/ticker/[symbol]` calls `generateMetadata`, then `TickerPageRenderer`.
- `/member/[slug]` calls member metadata and the member profile page.
- `/insider/[slug]` calls insider metadata and the insider profile page.

Those frontend routes call backend API helpers such as:

- `/api/tickers/{symbol}`
- `/api/tickers/{symbol}/context-bundle`
- `/api/tickers/{symbol}/signals-summary`
- `/api/tickers/{symbol}/government-contracts`
- `/api/members/by-slug/{slug}`
- `/api/members/{member_id}/trades`
- `/api/members/{member_id}/alpha-summary`
- `/api/insiders/{reporting_cik}/summary`
- `/api/insiders/{reporting_cik}/trades`
- `/api/insiders/{reporting_cik}/alpha-summary`
- `/api/insiders/{reporting_cik}/top-tickers`

Middleware currently tries to reduce crawler impact with lightweight anonymous rewrites and terminal shells. That protects the backend, but it can leave Search Console with thin or `noindex` HTML instead of a useful indexable page.

## Expensive Or Mutable Behavior Found

The audited backend paths include several operations that should not be coupled to crawler-facing HTML:

- ticker context bundle cache misses can build a full ticker context bundle;
- ticker quote/profile/fundamental helpers can call FMP provider endpoints or enqueue enrichment jobs on missing/stale data;
- ticker technical, chart, financial, valuation, news, filing, and ownership routes include provider-backed fallback paths;
- member analytics can compute alpha summaries and profile curves from persisted outcomes;
- insider analytics can compute summaries, alpha summaries, trade rows, current price fallbacks, and profile curves;
- event list routes can calculate confirmation metrics and enqueue missing trade outcome work in some flows;
- research brief services call OpenAI, but should remain admin/background only and outside public entity page rendering;
- institutional/profile performance paths can calculate and cache summaries.

Some routes already have bot/prefetch guardrails, but the safe SEO architecture should not rely on user-agent branching. The first HTML response should be a durable snapshot read for both crawlers and logged-out humans.

## Proposed Safe Request Path

The canonical entity routes should serve a baseline from precomputed SEO snapshots:

```text
GET /ticker/NVDA
  -> frontend requests read-only SEO snapshot
  -> backend reads seo_entity_snapshots
  -> frontend renders baseline HTML + metadata
  -> CDN caches response
  -> browser-only enhancement can load richer app data later
```

Equivalent behavior applies to `/member/[slug]` and `/insider/[slug]`.

The read path must not refresh, fetch providers, enqueue jobs, generate AI text, recompute scores, warm caches, or write to the database. Missing or non-indexable snapshots produce safe accessible pages with `noindex, follow`.

## Tables And Services

Introduce a generic snapshot table matching existing SQLAlchemy/startup schema conventions:

- `seo_entity_snapshots`
- entity type: `ticker`, `member`, or `insider`
- entity key: normalized symbol, member slug/Bioguide ID, or reporting CIK
- canonical path
- title and meta description
- indexable flag
- payload JSON for visible baseline sections and internal links
- `data_as_of`, `generated_at`, `updated_at`
- schema version and content hash

Service boundary:

- `get_seo_snapshot(db, entity_type, entity_key)` is read-only.
- `refresh_*_seo_snapshot(...)` writes snapshots from persisted Walnut data only.
- `refresh_seo_snapshot(...)` is used by jobs/admin/background code, never by public page GETs.

## Rollout Guardrail

This implementation prepares the safe architecture but does not unblock production robots rules or broad sitemap inclusion. Robots changes should be reviewed and approved after snapshot-backed pages, metadata, tests, and load checks are complete.
