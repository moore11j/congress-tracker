# Complete statement corroboration and retained source evidence

October 23 Pacific / October 24, 2026 UTC. This continues the [saved checkpoint](interim-statement-corroboration-2026-10-22.md); status below distinguishes local validation from release acceptance.

## Behavior

Qualified financial documents filed on the same date for the same period previously remained ambiguous even when one complete document contained every normalized observation from its peers, with exact agreement. The shared reader now selects that complete document. It never combines complementary fragments, takes values from different originals, relaxes accounting or currency requirements, or adds an issuer eligibility list. Conflicts and unqualified newer statements remain held. The existing three-candidate discovery bound is unchanged.

The shared heading rules also recognize “consolidated interim” balance, income and cash-flow statements. Complete VTEX originals demonstrate the real trigger: the selected press release contains the formal financial filing's normalized observations and two additional operating-cash-flow observations. This yields two reported revenue/margin quarters and current ratio 2.9120450704225354. Unsupported net income, EPS, FCF and TTM remain absent. Partial financial coverage does not establish a recovered Confirmation classification.

Acquisition retains complete peer originals. Every consumer revalidates them before projection, then preserves the recomputed selection proof, both original hashes and normalized observation digests through public history and core-cache evidence. Cached proof validation rejects malformed identities, sources, hashes, counts, selection and method metadata. Structural cache checks do not claim to reparse originals; source normalization and consumer validation perform that separate verification.

## Validation and boundaries

The prior paused implementation passed 250 Linux checks and a retained 49-company comparison, but omitted corroboration from returned consumer evidence. Those passes did not cover that omission. The new tests explicitly assert complete proof retention in public/core projections and actual cached payloads, no-refetch repeats, and rejection by both cache readers after proof corruption. Existing source-to-score/watchlist tests cover recovery, repeat, a later crossing and rollback without sending customer messages.

The initial updated run had 73 passes and one test failure: deepcopy retained the shared selected-source object, so the corruption test changed both copies of the hash. The test now round-trips through JSON to model persisted cache data and corrupts only one copy. No production guard was weakened. Final regression and source-cohort results follow when terminal; earlier overlapping counts are not summed.

An exact installed verifier is prepared at `artifacts/verify-interim-corroboration-runtime-oct23.py`. It checks hashes of relevant installed modules and all complete fixtures, replays source/core/public/cache equality with SQLite memory storage, and forbids network access. A local rehearsal is not installed-runtime acceptance.

Local rehearsal passes18 module/fixture hashes and five complete originals on Python3.14.2, taking23.390625CPU seconds. Full public-panel equality, both corroborating hashes and no-refetch cache repeats pass, with zero network requests, production reads/writes or emails. Production Python3.12 verification remains pending. A fresh bounded fetch of the five retained public VTEX URLs returns five200responses; current-original consumer replay remains pending. No repeated fetches or source-access fallback was used.

## DOX current-original reconciliation

Bounded read-only receipt `artifacts/dox-source-proof-1792823048.json` shows ordinary DOX core refresh at October24 05:06UTC, with the expected current ratio but a changed primary-filing hash. A single public SEC fetch matches the exact production hash `b2390ecbe598a112b249adec15f5e3d16f25b5f1b57fc4f06924313d5cfb1f72`. Requalification of the complete current and retained originals under identical identity/availability verifies all 28 normalized observations unchanged; current-date requalification also matches. Receipt: `artifacts/dox-current-oct23/receipt.json`.

This resolves the particular core statement-hash discrepancy without waiving it or inferring that the entire HTML document was unchanged. The old public panel still has its older capture. This is not proof of fresh whole-universe coverage or every current metadata hash.

## Deployment status

Fresh configuration receipt1792823048 confirms production574ca736, coreFMP/historySEC, institutional publicationfalse and the existing shared2CPU/2GB API/data host. Initial release preflight1792823249 finds zero protected unfinished/live jobs, zero pending jobs and the latest protected portfolio request succeeded. Recheck before an actual deployment.

PR726 is open as a draft at07876013. No deployment has occurred. No provider switch, cache deletion, new recurring schedule, host, subscription, customer delivery, video activation or FMP cancellation occurred. The full financial/consumer/institutional/transcript/capacity/Massive/zero-FMP/billing gates remain open.

## Final source/consumer regression and current-source acceptance

The final expanded Linux Python3.12 run passes262checks in610.49seconds. The complete49-company replay against installed574ca736 has48identical results and only VTEX changed, with exact repeat and unchanged source hashes during the comparison. CPU234.53125seconds is an offline measurement, not production capacity proof. Receipt `artifacts/interim-statements-oct22/cohort-comparison.json` now contains the final proof-preserving implementation hashes.

Current-source acceptance uses six public originals fetched once each, all200, including the newly disclosed director-appointment filing. The first replay exposed that missing retained source; after capturing it, normal discovery makes three distinct reads and selects the same complete financial document. Full annual/quarterly/semiannual arrays, summary and health match the older accepted capture; both current original hashes survive public/core evidence and exact repeated projection. Receipt `artifacts/interim-current-oct23/replay.json`. The earlier capture is compared at its original availability date, while current qualification uses October24. An intermediate comparison mistakenly advanced the old capture's date and requested a different unretained filing; that failed helper run is not acceptance evidence.

## Required PDF/image security update

Exact PR-head CI40070606732 reports48 known vulnerabilities in the existing pypdf6.19.0 and Pillow12.3.0 requirements. Secret scan, frontend dependency audit and Vercel preview pass. The backend failure is not bypassed: only pypdf6.21.2 and Pillow13.0.0 are selected as fixes. [pypdf release notes](https://pypdf.readthedocs.io/en/stable/meta/CHANGELOG.html) and [Pillow13 release notes](https://pillow.readthedocs.io/en/stable/releasenotes/13.0.0.html) confirm the relevant security releases. Production uses supported Python3.12. A repository search finds none of the listed removed Pillow helpers or removed I-mode PNG conversion behavior in application consumers.

The two old versions and two fixed versions are installed in separate local target directories, leaving production and the financial-regression environment unchanged. Existing House/Senate, issuer PDF/cache, financial PDF and image/layout consumers are under isolated regression; complete public House PDF/OCR method/text/page/error parity is also required. No automatic or manual social-video publication is enabled by these tests. Final dependency compatibility, exact CI and installed-version verification remain pending at this checkpoint.
