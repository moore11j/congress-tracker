# House scan economic review

October 12, 2026. Offline original-source review completed for the economic fields of House filing 9116376. This is preparation evidence, not a canonical publication or a completed Congress cutover.

## Source and method

The [official 31-page Khanna filing](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/9116376.pdf) has SHA256 `9112dbf52d805f97500c314cbfacf58b114c9448d0f9a81091bafdff63da1a95`. All pages have now been rendered and visually inspected. Page 1 is a cover with no transaction rows. The remaining 30 pages contain 459 rows.

An existing snapshot of the public austin-starks congressional-stock-trades dataset provided candidate transcriptions. Its 2026 filing/trade parquet sizes and hashes were checked against its saved manifest before reading; all four scanned House filing hashes match the downloaded official originals. This does not approve the secondary ticker resolutions or establish ongoing House coverage/freshness. Snapshot generation is October 11 at11:32:37.357 UTC.

The separately encoded page/checkbox ledger checks owner code, purchase/sale, transaction date, notification date, amount lower bound, amount upper bound, partial-sale checkbox and capital-gains checkbox. There are 3,672 comparisons across 459 rows. Names and security/ticker resolutions remain separate work; a secondary resolved ticker is never represented as printed source data.

Page transaction counts, in source order for pages1 through31:

`0,7,11,19,19,19,19,19,12,18,18,18,18,18,18,18,16,19,16,16,16,16,16,16,16,5,11,19,7,15,4`

## Findings that change implementation

- Page17 row13, Lululemon: the original notification column reads October1,2026; secondary candidate index263 has null. The original-based date is retained in the review bundle alongside that discrepancy. No production row has been corrected.
- The other 3,671 compared economic fields agree with the independently encoded page ledger. The repeat produces exactly the same review SHA256.
- Four pairs have identical candidate descriptions/economics but distinct source positions and trust headings: Berkshire ClassB, McDonald's, Micron and Procter & Gamble on page30 rows8-11 versus page31 rows1-4. All eight lots are retained. Deduplicating on owner/date/amount/ticker would lose legitimate rows.
- Common stock, ADRs, options, ETF, index-linked instruments, hybrid securities and municipal/corporate debt occur in this filing. The secondary ticker for an option's underlying cannot be imported as an ordinary stock trade. The registry/publisher currently only supports the earlier reviewed stock-only form and holds mixed unsupported instruments; do not silently drop their rows to publish the rest.
- Fifty-five candidates have no secondary resolved ticker, including instruments that should have no stock ticker and names needing further identity review. This is not a count of missing common stocks. Descriptions including BlackRock Funding, Kimco Realty OP and foreign ADRs require their actual security identity; do not use parent-company substitutions.
- The three Harshbarger forms have blank owner columns. The [House instructions](https://ethics.house.gov/wp-content/uploads/2026/07/7-8-2026-2025-Published-Instruction-Guide.pdf) make the ownership indicator optional. Blank ownership remains undisclosed; it must not be invented as self.

## Evidence and checks

Primary artifact folder: `artifacts/direct-feeds/free-replacements-2026-10-09/house-scans-oct12/`.

- `secondary-crosscheck.json`: exact candidate input hash `a43064b1dd2f0db9c37c26338ae3c3871b82837bdabcae783c1d4174448377f5`.
- `khanna-economic-review.json`: all page/row references, candidate descriptions clearly marked as candidates, independently reviewed economics, discrepancy and eligibility=false. Hash `1e649b65694c18845ec936ecab2827c2cd1911ae8c800ac74865b5ed10dd1c58`.
- `review_khanna_economics_oct12.py`: offline ledger comparison, original hash/row index/page population checks, unique visual row identities, all-lot retention and exact repeated output; Python3.14.2. No provider requests, canonical writes or emails. This is not an application regression suite.

## Live state and other checked holds

Read-only `house-scan-holds-1791834034.json` at19:40:33 UTC still shows the four documents failed at their19:20 run with39/52 attempts and zero revisions. This is before their next ordinary due collection, not proof that deployed PR621 failed. The retention release remains d2793db6; no forced collection or deployment was performed this turn.

`role-cpu-1791834042.json`: API two-hour mean0.191506 CPU exceeds the0.125 shared allowance; latest five-minute0.196335, current throttle0 and7381.69 credits. Database two-hour mean0.106772. Sustained capacity is still not proved; no resource change.

Four bounded free Finnhub requests through the existing shared limiter at19:41 establish that recommendation holds are genuine identity differences: ASML returns ASML.AS; both BRK-B and BRK.B return BRK.A; CX returns CEMEXCPO.MX. All returned periods are June-September2026. `recommendation-identities-1791834070.json` preserves the nonsecret responses. No identity guard was weakened or canonical data published; limiter accounting is the only expected production write from the probe.

## Remaining work

Finish source descriptions/security mappings and explicit mixed-instrument/undisclosed-owner handling, preserve every source lot, rehearse against complete existing canonical populations and consumers, then publish only after the original-source contract is satisfied. Observe the next ordinary retention run. Economic review alone is not permission to bypass publication gates.

Public core financials, institutional publication, transcripts, full scheduled/watchlist/monitoring/daily/weekly/ranking verification and sustained capacity remain incomplete. Massive personal Stocks remains the final subscription step, followed by complete zero-FMP traffic evidence and billing cancellation. No subscription, schedule, host, customer email, video, landing change or FMP cancellation occurred.
