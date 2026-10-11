# EBITDA percentage contract repair, October 11, 2026

The FMP exit comparison found an existing normalization defect: the income-statement-growth field growthEBITDA is a fractional change even when its absolute value exceeds one. The magnitude heuristic incorrectly displayed SGRP -7.101256 as -7.1% instead of -710.1%. Raw statements verify EBITDA fell from2.627M to-16.028M. This is a unit correction, separate from Finnhub/FMP EBITDA-definition differences.

## Change

Only the known growthEBITDA input uses an explicit finite fraction-to-percent conversion. Existing screener/legacy alias precedence remains, including zero; percent-suffixed strings, booleans, non-finite values and arithmetic overflow are unavailable. No current cache bulk rewrite or historical backfill is bundled. Existing provider/source identities and all other metric mappings remain unchanged. Subsequent ordinary fresh FMP normalization uses the correct unit while migration continues.

## Evidence and consumer checks

85 checks pass on Python3.14.2: new unit/filter/history cases, existing fundamentals cache/snapshots and the replacement Finnhub contracts. The first new filter assertion mistakenly called the core-filter helper rather than the fundamental-filter helper; corrected before passing. One follow-up edit used the wrong relative cwd and failed before changing the test; corrected with an absolute path. These were test-harness mistakes, not hidden baseline failures.

Actual saved-source replay ebitda-percent-replay-oct11.json compares current code with3998196d using the captured raw response SHA25642157525c16bc9ab860c976fbbb2a6a7824ac0eb07134678656ceae750bc96ea. SGRP changes onlyebitda_growth from-7.101256185763228 to-710.1256185763228, matching both statement inputs. NVDA stays67.81638552538398 and every other normalized field remains identical (observation timestamps excluded). No network or production writes during replay.

The field feeds screener display/range filters and stored snapshots. It is absent from the five-metric scored fundamentals summary. Actual cache SQL range filters and in-memory fundamental filters include/exclude the corrected value consistently. The summary is byte-for-byte equivalent with five populated scoring inputs; repeat upsert is idempotent, and an earlier snapshot's value and hash remain unchanged. Broader provider switching still requires the full score/monitoring/watchlist/daily/weekly rebaseline gates; this narrow repair does not establish their completion.

## Release state

Implemented and tested locally; review, deployment and exact production interpretation are pending. Public core remains FMP, institutions publication false. No landing changes, subscriptions, schedule/resource changes, customer email or FMP cancellation.
