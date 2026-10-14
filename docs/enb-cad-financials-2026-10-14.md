# Enbridge CAD financial ratios, October 14, 2026

## Problem and scope

Production Enbridge preparation was unavailable at 22:23 UTC because the general reader selected only USD US GAAP facts. The issuer reports CAD US GAAP statements. This repair reads explicitly reviewed CAD concepts for ENB/CIK0000895728 and publishes dimensionless ratios through the existing SEC preparation and consumer contracts. It does not select public core fundamentals or prices, convert CAD amounts to USD, substitute parent earnings for common earnings, or publish a leverage candidate.

The new basis pins total `Revenues`, rather than the narrower contract-revenue component. Reported `NetIncomeLoss` is income attributable to controlling interests, matched to parent equity including preference shares for ROE. The original statements separately identify earnings available to common shareholders; no USD market ratio is inferred from those CAD figures. Missing gross profit remains missing. Free cash flow amounts stay in currency-labeled calculation evidence; the existing USD-facing cash field stays absent.

Files: sec_currency_ratios.py, sec_fundamentals.py, sec_inline_financials.py, sec_fundamentals_preparation.py, test_sec_currency_ratios.py, fixtures/sec_enb/observations.json. The older TSM synthetic currency test now provides a separately verified common-income mock rather than assuming parent earnings qualify. No dependency, schedule, database schema or resource changes.

## Original evidence

Four complete SEC originals were captured with an identified, paced client. The retained compact test fixture contains standard observations and parser output, with URLs and hashes of full originals. Full originals remain in local artifacts/coverage-gaps-oct14; they are not committed as large files.

| Report | SEC original | SHA256 |
|---|---|---|
| June 2026 | [10-Q](https://www.sec.gov/Archives/edgar/data/895728/000119312526326752/enb-20260630.htm) | b187cf2fa5591d4801a01bd5807c2deb57872e536f65491446730102c4d7de00 |
| December 2025 | [10-K](https://www.sec.gov/Archives/edgar/data/895728/000119312526049810/enb-20251231.htm) | acee978a4ece8718962043f658ba4edeb3e0a7395ad0bfcc74a17e8b3464cdef |
| June 2025 | [10-Q](https://www.sec.gov/Archives/edgar/data/895728/000095017025101102/enb-20250630.htm) | ad0a422223c3417a698bd9ac836577b58086cbc552bb26c34043924b836e4d3e |
| December 2024 | [10-K](https://www.sec.gov/Archives/edgar/data/895728/000089572825000006/enb-20241231.htm) | 059eebb5ef38f304f5201710f78631ee1009c546b1e4848a8c7fd76cba637eb9 |

All 34 selected current/prior/instant input observations match the appropriate complete original's exact currency, accession, filing date, period, concept and amount. June 2026 trailing revenue is CAD83.491b (65.194+51.675-33.378), operating income CAD11.132b, parent earnings CAD6.127b, operating cash CAD12.432b and capital expenditures CAD10.764b. Prior trailing revenue is CAD64.477b. Current and beginning parent equity are CAD65.349b and CAD65.448b.

The eight ratios are operating margin13.3332%, net margin7.3385%, revenue growth29.4896%, operating margin change minus3.1471percentage points, current ratio0.7210, ROE9.3687%, FCF margin1.9978% and FCF growth minus67.4537%. These are the existing Walnut definitions using reported CAD amounts, not promises of exact vendor-definition equivalence.

## Pre-release validation

Before deployment, final363 selected checks pass on Linux Python3.12.3 after correcting the obsolete synthetic TSM common-income fixture. The complete original/cache/alert/control replay passes on both Windows3.14.2 and Linux3.12.3. One earlier command referenced a nonexistent test filename and ran no tests; it is not counted.

Complete-original replay on Windows Python3.14.2 passes: four documents,34 matched inputs, simulated companyfacts lag filled from the actual current inline original, isolated SQLite exact persistence/cache repeat and zero canonical cache writes during preparation. The real summary and confirmation-source conversion obtains three scored inputs and available classification from SEC alone. The actual watchlist evaluator creates zero alerts when the optional metrics become available, zero repeat duplicates, zero rollback alerts and exactly one later synthetic threshold alert. No email or production writes. Twenty dated full-source controls preserve complete output against972fdb22 with identical repeats. Receipt: artifacts/coverage-gaps-oct14/ENB-full-replay.json. Historical controls and simulated lag are not whole-universe or future scheduled validation.

The final test group also covers identity, wrong-unit exclusion, mixed currencies, missing total revenue, equal-date conflicts, completed New York filing-day boundaries, latest incomplete periods, stale facts, selective cache version, acquisition arguments and USD market ratios. The production reader changes only ENB's preparation version, retaining the existing schedule and bounded newer-filing acquisition.

## Broader readiness and next gates

Read-only full audit22:10:45UTC:1,752scopes,1,104 usable SEC and1,245 combined scopes;1,361 baseline classifications versus1,070 replacements,362 lost and71 gained. The249 unsupported SEC scopes all lacked a current complete trailing revenue period. This audit predates the local ENB repair and must not be presented as post-release coverage. Receipt financial-consumer-readiness-1792015848.json. Current leverage audit22:12 has only107 v6 preparations,23 component-complete and83 incomplete plus one negative-expense case;1,639 are missing or older. Older candidate counts do not justify activating leverage.

Ten leading coverage-gap source pairs and the official directory are retained. Many foreign issuers expose annual IFRS facts without current interim companyfacts; they need actual quarterly exhibits. SPCX identity is verified as Space Exploration Technologies, but current companyfacts has only interim history; its registration originals need separate accounting-continuity review. No broad foreign coverage, inferred zero, dropped symbol or public activation follows from this ENB repair.

Pre-release public samples22:22UTC all200:Feed0.360s,Insights0.661s with research,news0.290s,research0.460s,AAPL2.549s,NVDA2.199s. Current CPU throttle0 and balance11,533; earlier20:22 throttling remains in two-hour history, and balance has declined since the prior release. Sustained capacity remains unproven. Receipts insights-incident-1792016550.json and cpu-quota-1792016549.json under artifacts/direct-feeds/free-replacements-2026-10-09. These samples do not establish all data coverage.

Next: final checks/review, exact release and ordinary ENB refresh, then remaining financial coverage/definitions, institutions/transcripts, every ranking/digest/alert consumer and sustained capacity. Massive subscription/price validation remains the final paid step. Global zero-FMP traffic and cancellation/access-end confirmation remain required. No subscription, customer email, video, landing change or cancellation.


## Production release, 22:32 UTC

PR648 source91211240d0482c1128edb92fc4da3d91aa45423f merged as b2c3dd5bd8f4da2013e5c222e8b49c035d14d1e2. Backend workflow38648048917/job116139732746 is terminal success. Fourteen normalized runtime hashes and exact compact original current/prior/instant points, all eight ratios and identical repeats verify on production Python3.12.15 with network prohibited and no production writes. Receipt artifacts/coverage-gaps-oct14/ENB-runtime-1792017109.json. Full original parsing remains the separately verified offline replay.

The exact backend image and unchanged API shared2/2048MB and DB performance1/2048MB are verified by artifacts/preload-receipts-1792017138.json. FMP core/prices and institutional publicationfalse remain unchanged. Stored news23/23 and earnings5/5 succeed; institutional reference950scopes remains truncated/partial, research17partial/3unavailable. Stored run times are not proof every receipt ran after deployment.

Postrelease public reads all200: Feed0.512s, Insights0.868s with research, news0.256s/12items, research0.478s/44items, AAPL2.899s/NVDA2.544s. Receipt insights-incident-1792017136.json. These are samples, not sustained-capacity or complete-data proof. The old hardcoded release comparison in that probe is ignored; image and hashes are separately verified.

First two ordinary ENB checks at22:31:56 and22:32:42 retain the prior unavailable02:38 preparation, so ordinary propagation remains pending. The scheduler database reports the22:31 fundamentals job terminated with exit minus15 during the deployment; it is terminal, not a live process. The existing boot/process-aware lease logic permits the next ordinary run to recover without manually clearing state. No forced refresh or duplicate deployment was started.

CI38647890665 secret scan succeeds. Inspected frontend audit still reports five unchanged production dependency groups (four high/one critical); backend Pillow10.4.0 still lists33 advisory rows. Dependency manifests are unchanged. No clean full-suite or dependency-audit claim. A job-log read returned502; the same deployment's run-level diagnostics subsequently confirmed success. The PR attachment attempt hit the chat's100-identity limit; existing attachments were preserved.

## Next original-source investigation

The Space Exploration Technologies SPCX prospectus is retained as one bounded public read: [June12 424B4](https://www.sec.gov/Archives/edgar/data/1181412/000162828026042639/spaceexplorationtechnologi.htm),11,954,086bytes,sha256782a77c750517fde51c274def5753456d63e4aba186248e7341bc6970c3dce01. It contains audited2025/2024/2023 financial tables but no selected inline US GAAP income tags. The prospectus explicitly combines xAI and X as common-control entities and describes a five-for-one share split. Current companyfacts comparative interim amounts reconcile to prospectus March revenue (2026:12.508 minus7.814 equals4.694b;2025:8.138 minus4.071 equals4.067b). This is a promising source, not completed accounting-continuity acceptance. Full current original/annual/quarter matching, complete statement extraction and source-bound calculations remain necessary before any SPCX repair. No registration-form facts or share metrics were silently mixed into the current reader.

Current SPCX June30 original is also retained: https://www.sec.gov/Archives/edgar/data/1181412/000162828026052535/spcx-20260630.htm, sha256 b21adff93ed92113baf1c1d7a454aa9f1bf2448de5e2fe2db59c808711010280,2,272,033bytes. It independently describes the same common-control mergers and split. Two prospectus table candidates include the annual income statement; the audited table reports revenue18.674/14.015/10.387b and operating income minus2.589/0.466/minus3.505b for2025/2024/2023. This remains investigative evidence, not adapter acceptance.


At22:36:55 the next ordinary batch is terminal and its lease is cleared. It completed10 SEC scopes and8 metric scopes, reusing eight valid preparations from the interrupted run and finishing two more; the partial exit1 corresponds to explicit unsupported-source results, not a scheduler crash. There were21 due scopes and the batch advanced through ECF to EHAB. ENB remains due with the old attempted version and its original22:38:19 freshness deadline; it was outside this batch's first ten. Receipt artifacts/coverage-gaps-oct14/ENB-rotation-1792017415.json. Ordinary recovery is proven; ENB's eight stored values are not yet verified. No additional batch was manually triggered. All new local tool handles are terminal.
