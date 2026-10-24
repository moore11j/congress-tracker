# Feed controls and plan access, October 24, 2026

Implemented on `codex/feed-controls`, based on current `origin/main` at `31b2a10f`. Production is unchanged.

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

Draft [PR727](https://github.com/moore11j/congress-tracker/pull/727), application commit `5f2504e6`, is ready for review. Review and deployment remain pending. No production data, provider settings, schedules, infrastructure or emails changed. The paused FMP migration remains separate. Production performance with large mixed-source pages has not been measured by these isolated checks.
