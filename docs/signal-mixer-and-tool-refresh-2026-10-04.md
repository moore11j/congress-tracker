# Signal Mixer repair and research-tool refresh — October 4, 2026

## Status and scope

The owner approved commit and deployment after reviewing the implementation and previews on October 4. Release verification is recorded below when complete. The request covers source-limit failures even on September 1–October 4, a standalone Signal Mixer, and Backtesting, Compare and Screener formatting consistent with the retirement/options calculators. Separately deployed strategy release `9fbd61c1` is preserved.

## Behavior

- `/signal-mixer` is a dedicated authenticated tool with its own title, explanation, form and results. Navigation and ticker links point there; `/backtesting?strategy=mixer` redirects there. Existing Premium API enforcement remains.
- Signal Mixer is a historical event study: match an earlier company event with a later purchase disclosure and evaluate subsequent stock returns versus SPY. It does not allocate capital or monitor live rules. Portfolio Backtesting remains the separate capital-constrained simulation.
- The old 20,000 raw-record and 500-company rejections are removed. Sources stream in 2,000-row batches. Confirmations are restricted to the relevant companies; event matching happens before price loading. Price histories load in batches of at most 50 companies plus SPY. Results aggregate the complete eligible population, including global medians; the 50-example display limit does not limit the calculation.
- The selected dates define the disclosure window. Outcomes can mature after that window, through the stated current observation date. Future/incomplete horizons are not shortened. A horizon waiting for its allowed next daily close remains pending before the seven-day exit allowance expires.
- The initial recipe is **analyst upgrade → insider purchase filing within 30 days**, with a three-year disclosure window and unchanged cost inputs. This was selected for available historical coverage, not positive returns. Government contracts and the other existing sources remain selectable.
- Empty results distinguish no qualifying event pairs from matches awaiting maturity or prices. Source timing, missing history, cost assumptions and sample warnings remain visible.
- Shared research-tool headers, navigation, emerald input accents, blue result accents, rounded panels, focus states and responsive spacing apply to Mixer, Backtesting, Compare (including loading), and Screener. Existing controls, filters, result tables and entitlement gates are retained. No scoring, pricing or general portfolio-engine methodology change is part of this task.

## Read-only production-data checks

The candidate service was compiled in memory on an existing API machine and executed using a PostgreSQL **read-only transaction**, 45-second statement timeouts and a 180-second process deadline. No production source, rows, provider hydration, account settings or deployment was changed. These are candidate-service measurements, not evidence that the public HTTP endpoint has been repaired.

| Recipe / disclosure dates | Runtime | Qualifying trigger records | Matching setups | Completed 30 / 90 / 365-day outcomes |
|---|---:|---:|---:|---:|
| Original insider + contracts, Sep 1–Oct 4, 2026 | 2.64 s | 1,388 | 0 | 0 / 0 / 0 |
| Original insider + contracts, Oct 4, 2023–Oct 4, 2026 | 18.02 s | 7,691 | 0 | 0 / 0 / 0 |
| New insider + analyst-upgrade recipe, Sep 1–Oct 4, 2026 | 2.33 s | 1,388 | 7 | 0 / 0 / 0 |
| New insider + analyst-upgrade recipe, Oct 4, 2023–Oct 4, 2026 | 15.71 s | 7,691 | 55 | 38 / 26 / 10 |

All four windows completed without either old capacity error. The three-year analyst recipe's median net returns were -1.6894%, -5.46%, and 23.937% respectively; this is coverage verification, not predictive validation. Recent-window outcomes were pending, overlapping, or lacked an exit close. The subsequent exit-grace correction was verified by regression test; it changes pending versus missing-exit classification, not these completed counts. Contract observation timing restricts its historical matches and is not replaced with a backdated announcement date.

## Validation and review evidence

- Python 3.14.2 / isolated SQLite: **49 passed** across `test_signal_mixer.py` and `test_backtesting.py`. Includes 20,010 irrelevant sales before purchase qualification; 505 companies across 11 price batches without sample truncation; unrelated contract filtering; returns after the disclosure end; costs/benchmark alignment; filing-date ordering; SMA history; overlap; pending horizons; anonymous denial and Premium gating.
- Focused frontend: **22 passed** across backtesting result links, compare routes/recovery/access, and screener contracts, columns, filter persistence and export locks. Layout expectations were updated; a pre-existing compare-copy assertion was reconciled with the already-correct current options-flow-coming-soon copy.
- TypeScript and the final production build passed (including `/signal-mixer`). The first build picked up stale generated preview types; moving the temporary preview cache outside the frontend and rebuilding resolved that local verification issue. This is not a full-suite or exhaustive coverage claim.
- Actual components/routes reviewed in the local browser at 1440px desktop and 390px mobile, with no page-width overflow. Ran Mixer, switched Backtesting to custom inputs, viewed populated Compare, and ran Screener with populated rows. UI data was synthetic, separate from the production-data service checks. The temporary local fixture login route was removed and fixture services stopped.
- Local-only screenshots: `artifacts/tools-refresh-20261004/signal-mixer-desktop.png`, `signal-mixer-mobile.png`, `backtesting-desktop.png`, `compare-desktop.png`, and `screener-desktop.png`. They are ignored review artifacts, not investment evidence or production screenshots.

## Remaining acceptance

After separately authorized deployment: verify the new route/navigation and old-link redirect, anonymous and Free/Premium access, the actual default and short-window HTTP requests, and pending/no-match states. Observe concurrency and longest-window latency before promising instant arbitrary studies. Stored-price completeness, survivorship/corporate-action coverage, and broad five-year/source combinations remain unproven. Saved recipes/live alerts and general portfolio execution realism remain open; R6 is not complete.
