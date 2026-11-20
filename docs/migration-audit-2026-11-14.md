# Migration continuation and Walnut 2.0 audit checkpoint

November 14, 2026, America/Denver. The owner requests a comprehensive Walnut 2.0 proposal while continuing the approved FMP migration, with no Walnut 2.0 implementation before review. [Proposal](walnut-2-proposal-2026-11-14.md).

## Source baseline and work performed

The primary workspace is `b79891156d5200dfda1b56a9d5596d9f989a898d`, with substantial existing modifications, and is 613 commits behind its locally recorded origin/main. The current common-equity worktree is based on `4e0b68c2d1786df55ca68b20fcd32c6daf752a8c` with seven modified tracked files and the existing untracked common-equity service, fixture verifier, tests and fixtures. No application edits were made in this turn. No fetch or fresh deployment inspection was performed; local refs and dated receipts do not prove today's deployed version.

Ran the existing AST-only FMP dependency utility against that release worktree. It reports 72 distinct literal endpoint paths across 32 referencing modules, no unclassified paths, and two old catalog paths without references: `balance-sheet-statement-growth` and `cash-flow-statement-growth`. This is consistent with the previously documented valuation request reduction. The full inventory is retained in `artifacts/fmp-dependencies-2026-11-14.json` and grouped in the proposal. It includes optional code and registry defaults, does not resolve every dynamic expression/database override, and is not a count of active production calls.

## Common-equity validation

The strengthened `backend/tests/test_sec_common_equity.py` completes **34 passed in 79.39 seconds** using the existing `backend/.research-ops-venv` Python 3.14.2 environment. The process explicitly sets `DATABASE_URL=sqlite://` and denies socket connection attempts. Tests cover complete retained originals, reported-equity precedence, invalid/misowned/preferred/nil/hidden evidence, proof geometry/liability categories, copied cache evidence, unchanged existing metrics, source display, score/rule recovery, repeat/crossing behavior and rollback.

The first default-Python attempt failed because pytest was absent. Both older `.venv` and `.venv312` launchers point to a missing Python 3.12 installation. The existing research environment was used successfully without installing dependencies. An asynchronous tool runtime timed out while awaiting an earlier attempt; only the subsequent terminal 34-pass execution is accepted. WSL enumeration returned `Wsl/E_ACCESSDENIED`; no Linux result is claimed.

The previously unrecorded `artifacts/common-equity-parity-nov9/receipt.json` is present. Independently compared all three retained JSON datasets: baseline, candidate and repeat contain the same complete 49-scope outputs/request lists. The replay runner SHA256 matches its receipt, `7835ceea5c6d6140994dee653914459ddbd93da2d93e3b83d17db20e718e9654`. This recovers retained evidence that the November 9 narrative still described as pending. A fresh current-worktree replay also completed in this turn: all 49 complete outputs and source-request lists match the retained candidate and repeat exactly. There are 46 successful projections and the same three holds: B and WNW (`document_set_root_or_base`), RZLV (`document_set_statement_ambiguous`). No changed output or new source request was introduced. These offline comparisons do not prove an installed production build. Fresh result: `artifacts/common-equity-current-parity-2026-11-14.json`; combined receipt: `artifacts/migration-audit-2026-11-14-receipt.json`.

Remaining gates: broad exact-release regression suite, Linux Python 3.12 fixture under the unchanged CPU/wall limits, commit/review/release, installed-source verification and ordinary cache/consumer recovery. No production limit was relaxed. The common-equity package is still uncommitted and undeployed.

## Institutional replay: completed local evidence, narrower acceptance

Read the retained `complete-peer-reference-replay-nov6` state and consumer receipt. Active run `2ed99d4db45c429ab03442491ed71e8b` now reports phase `repeated`, first and repeat each **6,472**, identical complete canonical fingerprint `dc9786df559b440b08efcb2dedcd4d79e9e05e7720bc0810b37731d9f2021efd`, and `local_server_stopped=true`.

The consumer receipt has status `complete_peer_captured_snapshot_consumers_only`, with **1,826** eligible/captured/declared events. Pro receives 1,826 alerts and digest counts 12/8/1,826 in the no-send rehearsal, with identical repeat. Free/Premium receive zero for these protected institutional contexts. The canonical rollback equals the repeated state; synthetic accounts/alerts are rolled back. It reports zero external requests, production writes or emails. Consumer receipt SHA256: `9144a2ffa963483c7c7decf2d7d5ec3fc102158cd1fe6be6c695401f63b84ed8`.

These are inspected saved receipts, not a fresh database replay in this turn. The receipt explicitly sets `production_consumer_acceptance=false` and `full_cutover_ready=false`. Exclusions include current-quarter live publication/sustained capacity, ranking generation and daily/weekly Top Ideas needing financial/price evidence, and calendar fetching. Preserve these limitations.

The next continuation needs release reconciliation. `check_current_release_peer_consumers_nov9.py` currently pins `ffcc1e35a9e48901d97b4c4bd6aea69f7b1b8981` and a tagged-IFRS worktree, whereas the last memory entry describes pin `45115e7e`. No acceptance file for the intended current release was found in this directory. The older `consumer-continuation-resume-nov9.json` still says waiting and pins run `cd67dc0f2a2b45b1a814eb8c1fcdd21f`, which differs from the completed active run. Do not blindly restart it or overwrite its receipt. Frozen replay helpers and institutional databases were left untouched.

## Product and cost implications

The proposal reuses existing research evidence, thesis matching, divergence, scores, screener, caching and delivery. It preserves one 30-day Confirmation Score, Ledger horizons 7/30/90/180/365, historical identities and all existing feature routes/entitlements. New primary navigation, intelligence facade, Ask, materiality presentation and continuous thesis delivery are proposed work, not completed features.

Massive remains the selected price target. Its current public Starter price remains $29 with delayed data/five-year history, but current public terms distinguish personal non-business from business use. Walnut customer display/redistribution rights remain to be resolved before promising that tier as the final production budget. No commercial-tier shortlist was reopened or subscription purchased. Current invoices and private account entitlement were not inspected. The approximately $600 FMP and $61.94 Fly figures are historical reference amounts, not a reconciled November total. [Massive Stocks](https://massive.com/stocks), [terms](https://massive.com/legal/terms).

The current proposal supersedes the older “smaller core product” suggestion only as product planning: all existing functions must remain reachable. FMP migration retains immediate priority and its technical/authorization gates. No cancellation, customer sends, new schedule, model request, video or deployment occurred.

The [November 6 provider hold](confirmation-forward-provider-hold-2026-11-06.md) records an explicit FMP account suspension. Restored access was not verified here; source selection is not proof of a functioning fallback. The concurrent [November 14 study checkpoint](confirmation-forward-study-progress-2026-11-14.md) records successful bounded Massive historical collection, 208/225 observations across 28 requests. It remains separate from public-price cutover, commercial rights and certified Ledger acceptance. No study files or schedules were changed by this task.

## Next work

Finish current common-equity validation and Linux capacity proof; reconcile exact-release institutional consumers; verify ordinary financial rotation and broader coverage; then finish all consumer/delivery/ranking/schedule gates, final Massive entitlement/rights and prices, global zero-FMP evidence and billing. Walnut 2.0 implementation awaits the owner's requested architecture/UX/phase review.
