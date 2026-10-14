# HCA SEC operating-income recovery

October 14, 2026. Deployed through PR640/1752fe1d with exact runtime verification; ordinary HCA refresh pending. Public core fundamentals remain FMP.

HCA reports individual operating expenses without a tagged operating-income subtotal. The free replacement previously supplied only two of the five inputs used by Walnut's fundamentals classification, so that classification was unavailable. Its current-period margin was rounded Finnhub data and its year-over-year margin change was absent.

## Source and definition

The reviewed [June 30, 2026 Form 10-Q](https://www.sec.gov/Archives/edgar/data/860730/000119312526321077/hca-20260630.htm) and [2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/860730/000119312526044769/hca-20251231.htm) support revenue minus salaries, supplies, other operating expenses and depreciation/amortization. This excludes affiliate earnings, gains on disposals and financing. Independently, pretax income plus interest minus affiliate income and disposal gains must reconcile to the same result.

Quarterly source SHA256: `71e56da9c844f15467842d30c40ebb7110a2f9733e05d78cd8a502825eb0ef5f`. Annual source: `cc4733afa5ba5a7fa97b3faaddafee3e7c1d376bf482e49ecba6461a5e26ac34`. Complete raw originals and manifests are retained in local `artifacts/yahoo-gaps-oct14/`. These are observed source hashes, not a promise that future downloads are byte-identical.

The HCA-only rule requires all nine observations, matching period, filing, accession, currency and issuer, nonnegative expenses and independent arithmetic reconciliation. Explicit zero is valid; missing/conflicting inputs remain held. Reviewed history begins January 2024. Any reported operating-income series takes precedence over derivation. Provenance retains the complete component facts and identifies the subtotal as derived. Current inline parsing retains the additional HCA concepts, and only HCA's preparation version changes. No extra fetch path, schedule or public provider is introduced.

## Validation

The actual complete companyfacts, submissions and original quarterly filing replay produces trailing operating income of $12.073 billion, operating margin 15.4756258572%, and annual margin change +0.2372424079 percentage points for June 30, 2026. Each current/prior quarterly and half-year component is checked against the original statement. All six previously available SEC metrics and leverage preparation remain identical. Repeated output is identical and source inputs are immutable.

The actual current Finnhub cache combines with these SEC values through the existing reader. Classification gains a third available input and changes from unavailable to bullish; this is an isolated consumer calculation, not a published score or validated return forecast. The new SEC margin takes precedence over Finnhub's rounded 15.48%. Negative-equity ROE and unselected net debt/EBITDA remain unavailable. Market-data and wider ranking/alert acceptance are separate gates.

All 20 saved control-company source pairs preserve complete results and repeat equality, including six existing revenue-coverage holds. Source hashes are verified and network access is prohibited. Replays run on Linux Python 3.12.3 with zero production writes or emails. Receipts: `hca-operating-replay.json`, `hca-control-replay.json`, and `HCA-operating-fixtures.json` in the local evidence directory. The final affected regression result and release status are recorded below when complete.

## Yahoo findings

Four bounded, one-minute-spaced public income-statement requests (TSM, ASML, BRK-B, HCA) returned 379 observations. Exact draft PR619 parser/bundle replays verify hashes, immutable inputs, identical repeats and duplicate-capture deduplication. This does not establish sustained access, broad coverage or a supported production API.

ASML's reported trailing operating income disagrees with the sum of the four retrieved quarters by EUR963.3 million; trailing EBITDA differs by EUR1.2921 billion. BRK-B lacks operating income and EBITDA in the capture. TSM quarterly operating income differs from its [original SEC statement](https://www.sec.gov/Archives/edgar/data/1046179/000104617926000541/a2026q2consolidatedreport-.htm) by TWD34.46 million even though Yahoo's own quarter/trailing arithmetic agrees. HCA's value does reconcile to the direct SEC calculation above. Yahoo remains unselected and PR619 remains draft. These findings support recovering issuer data from reviewed originals instead of silently filling gaps from incompatible aggregates.

## Release verification

PR640 source `a99ccac5fc6be38cc6f16a1fb9677f7bc6459aed` merged as `1752fe1d20212be3aa22c4401c82ab9cfe2779d7`. Backend workflow38622068207/job116051831296 completed successfully, including readiness and Premium privacy checks. Thirteen runtime hashes and all five actual source-period derivations match on deployed Python3.12.15, including identical repeats and immutable inputs. Receipt: `artifacts/hca-operating-runtime-1792005367.json`.

The isolated SQLite preparation replay executes the actual source-to-cache pipeline from the three saved files, persists all eight expected SEC fields and returns an identical cached repeat with transport forbidden. No canonical rows are created; no production data or email is touched. Receipt: `artifacts/yahoo-gaps-oct14/hca-cache-replay.json`.

The ordinary19:16 batch completes10 scopes, but HCA still has its prior version in `artifacts/hca-rollout-1792005404.json`. Its normal version refresh remains pending; do not call it live prepared or publicly selected. Public postrelease samples return200: Feed0.542s, Insights1.216s with research present, news0.261s, research0.436s, AAPL2.793s and NVDA2.186s. These are immediate samples, not sustained capacity evidence. Receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/insights-incident-1792005383.json`.

Secret scanning passed. Unchanged frontend dependencies and Pillow still fail audits; no all-CI/full-suite claim. One CI diagnostic request returned502, then the dedicated diagnostics service confirmed terminal success. PR attachment failed because this chat already has100 attachments; the PR itself is created and merged. No source selector, schedule, subscription or host configuration changed.

## Ordinary follow-up, October14

The existing19:31 scheduled refresh now prepares HCA on the new version. All eight persisted fields and the complete current/prior/operating-income reconciliation match the original-source replay. This supersedes the pending ordinary refresh note above; public core remainsFMP. Captured at19:41 in `artifacts/tsm-interim-oct14/rollout-1792006890.json`, with repeat verification in `artifacts/tsm-interim-oct14/ordinary-verification.json`.

## Remaining gates

Broad financial coverage, actual score/ranking transitions, all alert/digest consumers, institutions, transcripts, sustained workload capacity, final Massive prices and global FMP retirement remain open. No subscription, host-size, video, landing-page or cancellation change.

Final affected regression suite: 145 passed on Linux Python 3.12.3. Includes integer overflow, missing/zero, identity, currency, prior disposal losses, reported conflicts and cache-version isolation.
