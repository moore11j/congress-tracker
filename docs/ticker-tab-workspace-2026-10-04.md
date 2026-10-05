# Ticker tab workspace — October 4, 2026

## Scope and state

Implemented against `7681f15f`; the owner subsequently approved commit and deployment. The release preserves newer strategy changes through `ec10100f`. Deployment verification is recorded below when complete. The owner requested moving the chart and activity sections into tabs to shorten the ticker page, retaining the sidebar and prioritizing mobile navigation and price context. This supersedes the earlier chart-above-tabs layout direction without changing chart functionality, source interpretation or entitlements.

## Changes

- Overview remains the default. Chart sits immediately beside it, before News and Financials. The chart mounts on first selection and stays mounted but hidden on other tabs so marker/range settings survive switching.
- Congress activity and Top Congress traders share a tab; Insider activity and Top insiders share another. Each leaderboard requests only its relevant source. Signals and Government contracts have dedicated tabs.
- Institutional activity follows the existing Ownership panel, which includes institutional holders. Its shared table retains reported value, source, details, filing/report dates and action; the action now uses plain colored text instead of a pill. Pro enforcement remains in place.
- Events and Filings are separate tabs. SEC requests run only when Filings is selected; press releases and disclosure events remain under Events. Other research, analyst, macro and valuation features remain available.
- Removed the invisible Overview height placeholder and absolute panel positioning. Each tab uses its own content height. The desktop evidence sidebar remains intact; mobile has tabs, a compact latest-close/1D-change/relative-volume strip, selected content, then the full sidebar.
- Added accessible tab semantics, arrow/Home/End focus navigation, Enter/Space activation through native buttons, active-tab horizontal reveal and overflow indicators. Legacy activity/leaderboard hashes select the right tab; sidebar links dispatch that selection, and pagination preserves the section hash.

## Validation

- TypeScript check passed; the final production build also passed, including type checking, all 67 static pages and the ticker routes. This was the shared working tree with concurrent strategy edits preserved.
- Final focused run: **38 passed, two existing assertions failed** across eight ticker test files. Updated only tab-list/Filings activation assertions to match the new navigation. Remaining failures concern the already-existing conditional signal-band label and old analyst-unavailable copy; checked both against `HEAD`. No full-suite pass is claimed.
- Browser checked the actual local `/ticker/DEMO` route against an isolated local fixture API. Rendered context uses a renamed archived context sample, with synthetic activity and chart data; these are layout checks, not current financial evidence or production tests.
- Confirmed Overview excludes the chart/activity sections, Congress includes its own leaderboard, the sidebar link selects Insider activity with only Top insiders, and a disabled Congress chart marker remains disabled after Overview → Chart. Pagination retains the Congress selection and section URL. Exact range-count behavior was not established because the initial fixture omitted the response offset; existing pagination tests pass.
- Effective mobile width was measured as **390px**, with document width **374px**, and tab-menu → compact quote → selected-panel order confirmed from DOM bounds. Also checked table containment at 520px. Browser zoom required compensating viewport dimensions; the measurement is the effective CSS viewport, not the requested physical window size.
- Screenshot capture repeatedly timed out; no new raster preview was produced. Automated approval review rejected opening a temporary synthetic sign-in route because it set authentication cookies. That route was removed without use, and no alternative sign-in bypass was attempted. Authenticated visual coverage, populated Pro Ownership/Signals and a fresh image preview remain unchecked.
- Temporary local fixture services were stopped and the development cache moved under ignored `artifacts/ticker-tabs-20261004/`. Unrelated strategy changes and existing generated-file edits were preserved. No production data, account preferences, emails or billing changed.

## Release acceptance

## Approved deployment receipt

- Committed/pushed `9d3ea81d6d737374f7fa3e7b076d65f11f1043ff`. [Vercel production](https://vercel.com/moore11js-projects/congress-tracker/H8Lk1Pe9hXhK6jMhMUTyiaJPUPQZ) succeeded; both public app-version endpoints returned that exact revision. API readiness/database returned `ok`. Frontend-only release; Fly was not redeployed.
- Rebuilt after the newer strategy release; production build/type checking passed. Signed-in live AAPL verification used the existing browser session. Overview, chart controls, populated Signals and Congress panels, institutional holders/activity and institutional pagination (1–20 → 21–40 with the Ownership tab retained) were checked.
- Live mobile viewport measured 390px, document/scroll width 375px. Navigation → compact price/volume → selected content is visible. Screenshot saved locally under ignored `artifacts/ticker-tabs-20261004/production-mobile.jpg`; viewport override reset.
- Live checking exposed an existing lazy-request cleanup race: Ownership retained its loading skeleton after receiving data until a tab switch. Follow-up clears loading state when cached data arrives in Ownership, Financials, Macro, Valuation and Analysts. Production build passed; focused rerun had 26 passes and the same two baseline assertion failures. Follow-up deployment verification is recorded below when complete.
- Concurrent navigation work appeared during verification and is preserved separately; only the loading-state fix is included in the follow-up. No account preferences, source data or delivery settings changed. Broader role coverage and source completeness remain separate.
