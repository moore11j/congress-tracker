# Feed controls and plan access, October 24, 2026

Implemented on `codex/feed-controls`, based on `31b2a10f`; merged and deployed as `5480f312` on October 24 after owner approval. See the release receipt below.

## Requested behavior

Ryan's feedback identified page sizes resetting when filters changed, missing exports, and cumbersome sorting. The owner requires sorting and exporting to be Pro only; page sizes above 25 require Premium or Pro. Admin retains operational access. Existing Outcomes rules are separate.

- Page sizes: 25, 50, 100, 250, 500 and 1,000.
- Filter submission, mode changes and Reset preserve the selected allowed size; changing filters starts at page 1.
- Pro can click Date, Ticker, Person / Entity, Source, Action, Amount, % Delta and Signal headers to toggle direction. Mobile users retain the sort dropdowns.
- Sorting applies to the displayed page, matching the existing page-sort scope. It is not a global ordering of the historical database.
- Excel, CSV and PDF export the displayed page in its current order, with separate reported/filing and transaction dates, missing-value labels and disclosure URLs.
- Guests and Free are capped at 25 by the API, including both limit and page_size aliases. Premium can expand rows but cannot sort or export. Pro/admin can do both. Explicit sort requests bypass public response caching so the access check cannot be skipped.
- Export accepts a bounded snapshot of the displayed rows, checks authenticated effective access, and returns private/no-store attachments. It does not run an unbounded data query or claim the client snapshot is an independently audited source record.
- CSV formula prefixes are neutralized; XLSX cells are explicitly text. PDF values are escaped, wrapped and paginated, with repeated headers.

## Validation

- Python 3.12.14, isolated SQLite: all 66 feed-filter, access/export matrix and public-cache tests pass in 53.26 seconds.
- Six focused frontend behavior/API tests pass, covering effective-tier fallback, page-size preservation, disabled controls, sort direction/values/null handling, and export order.
- TypeScript passes with no emit or incremental output.
- Broader feed markup suite: 20 of 22 tests pass after updating the changed table-header assertion. Two unchanged assertions fail against current main too: stale revalidation syntax and an existing watchlist premium-metrics expectation. Neither was changed to mask a product failure.
- Local Chrome with synthetic network fixtures passes Pro, Premium and Free workflows: 100 survives a filter change for paid users; 1,000 works for both paid tiers; Free remains 25; Pro header toggles both directions and exports 100 rows in the displayed order. Desktop and mobile screenshots inspected. Existing global-search hydration and nested mobile watchlist-button console warnings remain outside this change; no uncaught errors in the final fixture run.
- Excel and PDF tests retain all 1,000 rows. PDF pages with long names and a long disclosure URL were rendered with Poppler and visually reviewed after fixing row splitting.
- Whitespace validation passes. Local QA artifacts are under the primary checkout's ignored `artifacts/feed-controls-2026-10-24/` directory.

## Release state

[PR727](https://github.com/moore11j/congress-tracker/pull/727), including application commits `5f2504e6` and `154106d2`, merged as `5480f31225780300f513afb8bf62e0c3c00fcc47` and is deployed. No provider settings, schedules, infrastructure capacity, direct production data edits or emails changed. The paused FMP migration remains separate. Production performance with large mixed-source pages has not been measured.

## October 24 follow-up: tables only

The owner requested removal of the Cards selector and card view. FeedList no longer stores or reads a view preference and always renders FeedTable. The mobile stacked layout is removed; a labeled, keyboard-focusable horizontal scroll region contains the same table at every screen size. Row details remain expandable. Shared detail components used elsewhere are retained.

Seven focused frontend checks pass (the six controls/API checks plus the updated existing layout assertion), as does TypeScript. Local Chrome with synthetic fixtures and a saved `walnut:feed:view=cards` preference confirms the selector is absent, one table remains visible on desktop and mobile, horizontal scrolling and expansion work, and Pro sorting in both directions, export order, filter-size persistence and 1,000 rows still pass. Desktop/mobile screenshots were inspected. No uncaught browser errors; the existing global-search hydration warning remains. Removing the mobile stacked layout also removes its nested watchlist-button markup. No backend changes or new backend test run in this follow-up. Review and deployment remain pending.

## Approved release and production verification, October 24

The owner's instruction to commit and deploy all feed changes authorized this release. The merged frontend/backend trees exactly match the tested PR head. Preview build passed; Vercel production deployment `dpl_FHFsQb5GTWTRNKfDwfycmGTj1zE4` is Ready. Both `walnutmarkets.com/api/app-version` and `app.walnutmarkets.com/api/app-version` returned the full `5480f312` merge SHA.

Guarded [backend workflow40116863444](https://github.com/moore11j/congress-tracker/actions/runs/40116863444) succeeded at17:38:10UTC. Installed normalized SHA256 hashes match the local release for `app/main.py`, `app/routers/events.py`, `app/services/feed_controls.py` and `requirements.txt`. Runtime Python3.12.15 has openpyxl3.1.5 and reportlab4.4.10; actual installed renderers produce readable CSV, XLSX and PDF bytes. Isolated in-process rank checks cover0/10/20/30 without database writes.

Public checks after deployment:

- Readiness/database returned healthy in0.354s.
- Requests for1,000 rows through `limit` and `page_size` each returned25rows/limit25 in0.628s and0.362s.
- Explicit sorting through either parameter and all three export formats returned403 anonymously, in0.213–0.226s.
- Fresh Chrome on the production app passed desktop/mobile checks with a saved Cards preference: no selector, one table,25rows, disabled sort headers, no export buttons, horizontally scrollable mobile table and expandable row details. No uncaught browser errors; screenshots inspected. The browser harness waits for hydration after consent dismissal.

These timings are bounded spot checks, not a capacity benchmark. Authenticated paid production sessions and1,000-row production performance were not exercised. Prior paid-tier local browser fixtures and66backend/sevenfrontend checks remain the evidence for those branches. Local-only verification scripts/screenshots are in `artifacts/feed-controls-2026-10-24/` in the primary checkout. This tracked receipt preserves the results.

## October 24 follow-up: static rows and exports on the right

The owner clarified that rows should no longer expand. Commit `2fc4bdb9` removes row expansion state, focus/click/keyboard handlers, card details and obsolete overlay props. Ticker/entity/disclosure links and watchlist buttons remain. The export toolbar now aligns to the right edge, including its narrow-screen wrapping.

Seven focused frontend checks and TypeScript pass. Local Pro Chrome fixtures confirm the PDF button's right edge matches the table, clicking a row produces no expansion, and existing sorting, export order, filter persistence and1,000-row behavior still pass. Screenshots inspected. Vercel production and both public versions verify `2fc4bdb9`; live desktop/mobile rows stay closed with no uncaught errors. Backend unchanged.
