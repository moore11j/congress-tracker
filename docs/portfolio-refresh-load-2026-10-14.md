# Portfolio refresh CPU and timeout repair, October 14, 2026

## Incident and release state

At diagnosis production was ae4931ff (PR650). The portfolio repair is now deployed as8241a907 (PR652), as verified below. Financial precision PR651/source9c230e8a is held unmerged and undeployed while reliability is addressed. Core fundamentals/prices/transcripts remain FMP; institutional publication remains false. No host, subscription, trigger schedule, publication or delivery policy changes.

At 23:32 UTC the API credit balance was at its floor; first near-zero sample was 23:23. Feed took 6.508 seconds, Insights 4.846 with an unavailable research fallback, and both AAPL/NVDA context requests timed out after 20 seconds. The database was about 89% idle in the sampled metric. Health also failed at 23:37:53. Earlier fast post-deployment samples do not prove sustained capacity.

The ordinary portfolio-simulation-refresh started at 23:18 and timed out at 23:38:01: exit -15, 1201.57 seconds. Fifteen data jobs were pending in the 23:34 process observation. The completion receipt's 0.0024 CPU seconds omits unreaped descendant work after group termination and must not be treated as the job's actual CPU usage. Process sampling observed the portfolio subprocesses running. A separate unidentified SSH command helper also consumed CPU in that sample; it was absent on the subsequent read. This is workload evidence, not complete attribution of every exhausted credit.

Read-only persisted-run counts since 23:18 show 227 rows each for 30/90/180/365 days, but only 20 for 1095 days. Counts are not a full success/coverage audit. Subsequent news/metadata/earnings warmers succeeded; fundamentals/research/reference nonzero exits carried explicit partial/unavailable coverage, not demonstrated Python crashes.

## Repair

- Execute all warmup trades, sales, rebalances and opening-holding reconstruction, but skip daily display/quality snapshots that are discarded outside the visible window. Rebalance snapshots remain necessary and are retained.
- Reuse the already loaded trade events and loader skips only within the same entity/window invocation. Price histories and annual holdings still reload during preflight retries. A separate invocation reloads events; no process-global data cache is added.
- Sort the unchanged benchmark date list once per simulation rather than once per visible day.

All financial methodology, lookbacks, candidates, price-quality gates, provenance, output fields and persistence paths remain unchanged.

## Verification

Linux Python 3.12.3: 133 existing portfolio/strategy tests pass; two new regression tests pass. The latter prove changed prices are observed during reused-input simulation, complete independent-output equality, and new trade input visibility across separate invocations. No full-suite pass is claimed.

A bounded public-member snapshot contains 93 event rows, 37 annual holdings and 16,634 cached prices, with input history starting in 2022. Compressed SHA256 e9dcf2f61fb9e3c0e7f03a0ee2d289506bf2ed56b3b0aa6f2e502711fd17799a. The initial two-member export exceeded its 30 MB guard and failed before returning data; the smaller export succeeded. This sample does not establish the full production cohort or full historical coverage.

All ten complete simulation dataclass outputs match the deployed ae4931ff code exactly: five lookbacks (30/90/180/365/1095) and both transaction/disclosure date modes. The five-scope batch report repeats identically with SHA256 2279e733a05cabce2e34e2b29f14e647e0129c5201bc27adb2abc6bdddee3b29. Queries decrease from 169 to 164, event loads from ten to five and snapshot calls from 3459 to 1258 in the captured profile; profiled function calls fall from 1,013,057 to 644,648.

CPU measurements are noisy. An initial separate-run comparison was slower after the patch. Eight alternating paired runs in the same process produced median CPU 0.6734071 versus 0.63738605 seconds (5.35% lower), with broad overlapping samples. Do not extrapolate this to production job savings or claim sustained recovery. The removed work and exact output parity are established; production effect remains unverified.

Primary local artifacts: portfolio-profile-1792021424.json.gz, portfolio-local-baseline-oct14.json, portfolio-local-optimized-oct14.json, portfolio-differential-oct14.json, throttle-attribution-1792020861.json, throttle-processes-1792021151.json, preload-receipts-1792021236.json; direct-feeds/free-replacements-2026-10-09/{cpu-quota-1792020745.json,insights-incident-1792020753.json,scheduled-job-cpu-1792021174.json}. These ignored artifacts are local evidence; the conclusions and limits are retained here.

## Remaining gates

Review/release the performance repair and verify exact runtime plus public consumers. Ordinary full portfolio completion, all lookback coverage, queued-source freshness and sustained capacity remain open. Financial precision PR651 is separate and held. Full free-feed financial definitions/coverage, institutions, transcripts, all alerts/rankings/scheduled consumers, final Massive subscription/validation, zero FMP requests and cancellation/access dates remain required before FMP shutdown.


## Release verified, 23:54 UTC

PR652 (source666c9d3564d5011cbda2147258ea17a12aafa46e) merged as8241a90724a7e09f1a68cd3ce7d8a75f8bcb967f. Backend workflow38656670796/job116167964126 finished successfully, including readiness and anonymous Premium privacy checks. Six source/schedule hashes match. An isolated in-memory SQLite check on deployed Python3.12.15 proves reused trade inputs still see repaired prices and produce complete output equal to an independent reload. No production database writes, network requests or email in that fixture.

Immediate public checks: Feed0.549s, Insights0.911s with research present and no unavailable fallback, news0.245s/12 items, research0.616s/44 cards, AAPL3.628s and NVDA2.899s. These samples are after a restart and do not establish sustained capacity or acceptable broad ticker latency. The old probe's hard-coded release-string field is unrelated to this backend release and is not used as deployment evidence.

Ordinary post-release strategy evaluation exits0 in32.44 wall/21.6785CPU seconds; news and metadata warmers also exit0. Reference/research retain explicit partial coverage. Full portfolio completion remains unverified; it was not manually restarted or falsely marked complete. Data-job fairness and three-year coverage remain next reliability checks.

CI secret scan passed. Logs verify unchanged frontend production audit five vulnerability groups (four high, one critical) and Pillow10.4.0's33 advisory rows; no dependency files changed, no full CI pass claimed. PR attachment failed because this chat reached the app's100-attachment limit; the normal PR URL remains available.

Evidence: primary artifacts/portfolio-runtime-1792022050.json; direct-feeds/free-replacements-2026-10-09/insights-incident-1792022048.json and scheduled-job-cpu-1792022072.json. All new local command handles are terminal. Financial precision PR651 stays held. No FMP shutdown or other source switch.
