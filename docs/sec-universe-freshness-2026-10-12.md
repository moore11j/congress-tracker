# SEC universe freshness repair, October 12, 2026

Implementation and local validation complete; deployment pending. Public core fundamentals remain FMP. No schedule, batch maximum, source selector, canonical event, alert identity or host size changes.

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
