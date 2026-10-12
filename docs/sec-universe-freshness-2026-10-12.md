# SEC universe freshness repair, October 12, 2026

Deployed as PR604/79a30e49; exact runtime and first ordinary batch verified below. Public core fundamentals remain FMP. No schedule, batch maximum, source selector, canonical event, alert identity or host size changes.

## Reproduced defect and repair

The broad warmer ordered nonpriority symbols by their latest attempt. A cache hit near its 24-hour expiry updated that attempt without updating the original observation. A subsequent rotation could arrive after expiry. The existing priority-only observation deadline did not protect the rest of the retained universe.

All broad scopes now use their actual observation plus 20 hours as the refresh deadline. Initial rollout seeds missing deadlines from compact SEC cache timestamps, without reading large payloads. Changed preparation versions override deadlines. Fresh scopes are skipped while the entire universe remains available to the independently bounded Finnhub metric warmer. Source failures preserve cached evidence and retain a bounded retry delay. The legacy priority deadline projection remains for operational readers. No preparation-version bump or forced full rebuild is introduced.

## Evidence

The regression fails against deployed baseline 0f64a9dd by selecting fresh AAA instead of due ABC. Tests cover observation deadlines, cached hits, changed versions, corrupt timestamps, failed source preservation and Finnhub progress when all SEC scopes are fresh. Fresh exact-worktree run on Python 3.14.2 passes 108 selected fundamentals, freshness, Finnhub metrics and leverage tests in 10.92 seconds. The earlier expanded selection passed 166 tests. Diff whitespace checks pass; no full-suite claim.

Actual job/cache logic ran over 1,751 isolated synthetic scopes and 481 five-minute ticks per implementation, covering 40 simulated hours. Real requests were blocked. Baseline first expires at minute 565 and reaches 612 simultaneously expired caches; 279 sampled ticks contain expired data. Candidate has zero expired caches across all ticks. Both retain the maximum ten SEC scopes per run, original FMP baseline rows, and zero events or emails. Baseline refreshes 1,751 source observations; candidate refreshes 3,502. Thus this restores necessary refresh work, not a claim of lower source cost. Local CPU is 39.719 versus 54.844 seconds for the simulation, not production throughput. Receipt: sec-universe-clock-oct12.json; process62593 terminal/exit0.

This assumes healthy source responses and the current 1,751-scope universe. It does not prove capacity at the 5,000-symbol admission bound, under failures, or during weekday workload. Full ordinary production rotation and CPU observations remain acceptance gates.

## Current production coverage and remaining work

Read-only audit at 05:53 UTC covers 1,751 retained FMP-baseline attempts: 410 usable current SEC scopes, 111 verified issuers without supported SEC ratios and 288 combined free scopes. There are 738 expired caches, 322 identity/version conditions, 165 directory absences, three missing caches and two companyfacts404s. These are dated preparation conditions, not all permanent subscription gaps. Receipt: paired-free-coverage-1791784427.json. Period comparisons include 358 newer, 18 same and two older (ACN/AZO), whose earlier-earnings coverage difference remains documented separately. No numeric equivalence claim.

Before public core selection: observe this release and full rotation, resolve retained financial definitions/coverage, validate scores/rankings/all monitoring and delivery consumers, and retain safe rollback. Institutional capacity/publication, transcripts, scheduled Senate and SEC quarantine observations remain separate. Massive personal Stocks stays last; no purchase, FMP shutdown or cancellation.


## Release and first ordinary production run, 06:12 UTC

[PR604](https://github.com/moore11j/congress-tracker/pull/604), head2cdac140, merged as79a30e496ff4a619e2deae606d7079391ca48a09. Backend workflow38210934004 succeeds. All55 expected runtime hashes match; the one shared2CPU/2GB API and one performance1CPU/2GB database remain unchanged. Institutions publication remains false, core remains FMP, and the already selected Senate flags remain true. No source activation or infrastructure change. Secret scan and Vercel pass. Existing unchanged Pillow and frontend dependency audits fail; this is not an all-CI-pass release. Direct diff review completed; no new automated reviewer receipt. PR attachment attempt encountered the100-item task limit.

At06:08 runtime verification, queue39has no pending/running work; available memory1462.6MB. Eight public responses succeed: feeds0.387/0.412s and freshAAPL2.536/NVDA1.278s. Postrestart samples do not prove sustained capacity. Predeployment metrics at06:05 show APIcredits2301 and CPU above the0.125baseline, without sampled throttling. Sustained load remains a gate.

Ordinary06:11:03 batch now seeds1748scope deadlines across1751attempted symbols; three scopes have no cache deadline and remain covered by attempt/version scheduling. It identifies1033due scopes, processes exactly10SEC and8Finnhub, and records zero canonical writes/model calls/emails. SEC results include seven partial financial scopes, two directory absences and one companyfacts absence; Finnhub retains one identity mismatch rather than substituting data. The overall partial status and process exit1 are expected truthful coverage reporting, not full coverage success. Recorded job cost9.61wall/2.8864CPU seconds. At06:11:38,818deadlines remain past due; this is the existing backlog, not completed rotation. Do not increase batch sizes or claim sustained freshness from one run.

Receipts: universe-freshness-runtime-1791785329.json, universe-warming-1791785498.json, scheduled-job-cpu-1791785507.json and cpu-quota-1791785148.json. All observation sessions terminal; no local PostgreSQL started. First ordinary selected Senate06:27 and due SEC quarantine06:47 observations remain pending. Keep full FMP exit goal active.
