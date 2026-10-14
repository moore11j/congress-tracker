# Arm SEC financial coverage, October 14, 2026

## Source gap and implementation

A bounded read-only production audit at16:15 UTC finds Arm among current financial preparation failures. The existing core reader accepts domestic10-K/10-Q forms only. Captured SEC companyfacts includes USD US GAAP annual20-F and quarterly6-K facts for exact issuer ARM/CIK0001973239. No source selection is changed.

The reviewed reporting policy expands accepted forms only for that symbol/CIK. Shared statement and current-fundamental readers retain USD units, entity identity, completed New York filing-day availability, conflicts, aligned periods and latest-period guards. Inline recovery permits the reviewed Arm quarter-end filename convention; event-only6-K filings cannot trigger financial downloads. Other issuers retain their prior forms. The policy includes both original source hashes and accounting basis.

The public statement contract now supplies five annual periods and twelve quarters, plus verified current trailing amounts and ratios. Direct diluted EPS retains its reported ordinary-share basis; no ADS conversion or forecast is inferred. Free cash flow is the existing arithmetic operating-cash-flow minus property/equipment purchases, explicitly excluding separately reported intangible purchases and financing payments. This is not issuer-adjusted free cash flow. Only ARM cache versions change; ordinary demand/rotation refreshes them. Confirmed404/directory absence retains negative caching; transport denials remain errors.

## Source reconciliation

| Original | SHA256 |
| --- | --- |
| [FY2026 annual20-F](https://www.sec.gov/Archives/edgar/data/1973239/000197323926000097/arm-20260331.htm) |95ea3f0d24a85deeda32984663de9dafebe29659e98344194a9d752ed2d2c601|
| [June2026 quarterly6-K](https://www.sec.gov/Archives/edgar/data/1973239/000197323926000114/arm-20260630.htm) |0d280b51d3bc153599ff69979b582f5042d727caab9700b4613f72f1c1e1c1ad|

Both originals explicitly supply the reported table amounts. For trailingJune30,2026, annual plus current quarter minus prior-year quarter yields USD millions: revenue4920+1289-1053=5156; netincome904+270-130=1044; operatingcash1524+902-332=2094; PP&E545+197-154=588; grossprofit4799+1253-1023=5029; operatingincome900+91-114=877. Arithmetic freecashflow is1506million. All18 underlying input values match independently parsed inline facts in the two originals, including exact periods and accessions. Annual/quarterly direct diluted EPS0.85/0.25 match their original tables.

Actual source replay succeeds with sockets forbidden on WindowsPython3.14.2 and LinuxPython3.12.3. Removing all facts afterMarch31 forces the reviewed6-K recovery path and restores the same current period/cashflow. Real preparation and statement-cache readers repeat with identical whole cached state and no repeated source requests. Canonical fundamentals/events remain empty; no production writes, provider/model calls or email occur during replay. The source captures themselves made bounded identified public SEC requests. Artifacts: foreign-coverage-oct14 manifest, originals, extractedtables and arm-replay.json.

Initial155 affected Linux checks pass;23 focused negative-cache/absence checks also pass. Final exact-source Linux run passes161 affected tests and the full original-source/cache replay with reported ordinary-share EPS preserved. An initial test invocation named a nonexistent test file and collected nothing; the corrected suite is the reported pass. No full-suite claim.

## Current release gates

Implementation is local pending final validation/release. Production remains9ec6b4d7. Ordinary Arm cache recovery, deployed hashes and public repeat remain required. Core fundamentals is still FMP, institutions publication remainsfalse, and other foreign currencies/forms remain separate coverage work. Sustained CPU/latency, all alert/ranking consumers, transcripts, final Massive/globalFMP/billing remain open. No new schedules, hosting costs, subscription, customer email or cancellation.

The16:16 aggregate two-hour FMP inventory confirms provider_call entries for prices, core fundamentals and operational transcripts. A narrower earlier query excluded provider_call cache-status rows and returned empty; it must not be cited as zero traffic. Authorization telemetry does not independently prove completed HTTP requests. Source ownership remains Houseofficial, Form4SEC, SECpress and Senatecommunity generation5. This audit does not change ownership.
