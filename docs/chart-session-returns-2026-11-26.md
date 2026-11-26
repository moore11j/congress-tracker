# Chart session returns

Prepared November 26, 2026 on deployed baseline e4aea368. Not deployed.

The 1D and 5D paths passed minute points to a quote helper that compared the final two rows, then labeled the observation as a daily close. This reproduced the cause reported by the owner and extends [PR805's metadata finding](https://github.com/moore11j/congress-tracker/pull/805#discussion_r4638542729) to the numerical comparison.

The chart retains its latest minute price and timestamp. During the regular session, Day Change and Day % compare that minute with the exact preceding trading session's cached close. After the regular close, they compare the current regular close with the preceding regular close; the latest after-hours movement is separately measured against the current regular close. Premarket movement is separate and Day % stays unavailable until the regular session. Missing, invalid, older-than-required or mismatched-provider/basis references cannot become a fabricated return. No provider request is added by the comparison helper.

Minute quotes retain minute_close, the selected Massive plan's 15-minute delay and the full UTC observation timestamp. The UI identifies delayed after-hours/premarket prices, adds separate extended-hours statistics and a regular-close value after hours, and explains both comparison bases. Daily chart behavior is retained.

US cash-equity session boundaries use America/New_York and existing trading-day handling. The focused helper includes early closes for the trading day of July3, Thanksgiving Friday and Christmas Eve, based on [NYSE's published calendar](https://www.nyse.com/trade/hours-calendars). This does not change shared alert calendars or other market logic.

A read-only production cache inspection at15:44UTC found NBIS87.31 to84.99 (-2.65719849%, displayed -2.66%) and NVDA155.38 to149.30 (-3.91298751%, displayed -3.91%) on November24/25. The four cache records and their provider/basis metadata are retained in backend/tests/fixtures/chart_regular_closes_nov25.json. Minute prices in regressions are explicitly synthetic; no live intraday source capture or production browser acceptance is claimed.

Validation:58local backend checks pass across new minute/session regressions, Massive public callers and the existing chart suite on Python3.14.34distinct frontend checks pass, including rendered1D/5D assertions for separate -2.66% Day and +0.08% after-hours values, the regular close and the full delayed timestamp. TypeScript passes on the final UI. LinuxCI remains pending. Tests cover both symbols/ranges, UTC date rollover, missing closes, source/basis mismatch, zero/invalid references, premarket, regular hours, holidays, early closes and DST. No production mutation or market-provider request was used for cache inspection.

Migration remains a separate open objective. The reference collector completed1741captures with eight uncertain keys retained; first/repeat replay and cutover checks remain. No replay input or worker was changed by this chart fix. This branch does not authorize deployment or source activation.
