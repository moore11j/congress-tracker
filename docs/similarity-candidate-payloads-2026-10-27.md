# Historical matching payload cost

## Measured problem

On October 27, a normal AAPL context build logged 2,273.8 ms internally: profile 147.5, quote 535.4, signals 61.7, source context 170.5, confirmation 1,340.8 and build-slot wait zero. Other public samples varied, so this is a measured slow component rather than proof of universal latency.

A bounded read-only profile isolated historical setup matching at 1.093 and 1.105 seconds for AAPL and NVDA. Its candidate query took 716 and 727 ms; score history took only 7.8 and 3.6 ms. The existing methodology index was valid. A nonexecuting plan showed an estimated 5,915 qualifying rows sorted before the 5,000-row limit.

The actual 5,000-row cohort contained 34,812,283 bytes of contribution JSON and 1,932,306 bytes of freshness JSON. Existing episode selection retained only 1,189 headers across both directions. Reading the five metadata fields needed for selection took 0.029 seconds. This supports reducing transferred evidence rather than changing cohort size or adding hardware.

Receipts in primary artifacts: `ticker-timing-logs-1793087930.json`, `confirmation-profile-1793088011.json`, `similarity-query-1793088086.json`. All diagnostic database transactions were read-only with three-second statement limits. No snapshot contents or customer records were exported.

## Change

Keep the exact existing 5,000-row window, ordered by market date and ID. Load only ID, ticker, direction, market date and calculation timestamp first. Apply the unchanged 30-day episode algorithm, then the existing direction restriction. Fetch complete evidence in one query for retained IDs, preserving their order. Deferred evidence raises on accidental access, preventing per-row lazy queries.

Confirmation snapshots are immutable through the ORM. If an out-of-band change removes or alters selected headers between queries, fall back to the original full-window read. Existing methodology, live-only eligibility, date cutoffs, episode rules, source evidence, outcome eligibility, ranking and paid projection remain unchanged. No new cache, schedule, provider, index or service is added. Worst case still fetches the full bounded cohort plus one header query; savings depend on episode/direction duplication.

## Validation

All 26 selected Linux Python 3.12 checks pass. New cases compare IDs and complete results against the original full-payload query for bullish and bearish setups with a bounded window, repeated episodes, mixed-case tickers, excluded same-day/reconstructed/different-methodology rows, and full/limited projections. Header-only reads contain no evidence JSON; retained evidence uses one bulk read with no lazy-query fanout. Empty direction selections avoid the second query. Simulated out-of-band deletion and direction changes reproduce the coherent fallback.

Read-only production-data parity uses one repeatable-read transaction, the unchanged installed implementation and the candidate in a separate in-memory module. No application file, cache, canonical record or provider was changed. Both AAPL and NVDA preserve all 250 matches, complete output hashes, public/paid projections and identical repeats:

| Symbol | Original matching | Candidate | Candidate repeat |
|---|---:|---:|---:|
| AAPL | 1.0790 s | 0.4313 s | 0.4277 s |
| NVDA | 1.0073 s | 0.4322 s | 0.4206 s |

Receipt `similarity-parity-1793088352.json`, candidate source SHA256 `1c91e0840d70a77e894538b04462c187a6755a5b26d2f5841f8e7645bd4aa7e2`. CPU also decreased in these samples, but order/cache effects prevent a sustained CPU savings claim. Current source fields and price provenance were retained; this is not FMP cutover proof.

## Release gate

Review and deploy the exact candidate through the guarded backend workflow, verify the installed source, then observe normal public contexts and stage timings. Current backend remains `ee89105c` until that succeeds. Institutional capture approval, broad financial coverage, all-consumer/scheduled reliability and final Massive/FMP shutdown remain separate gates. The preceding metrics capacity release is live, with full-rotation freshness still pending.
