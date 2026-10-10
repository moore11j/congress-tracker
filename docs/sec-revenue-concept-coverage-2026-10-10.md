# SEC reported revenue concept coverage

October10,2026: read-only production preparation audit finds403 fresh panels:225 with metrics and178 unavailable. Of the unavailable panels,77 are absent from the SEC directory,100 have unsupported current trailing revenue coverage and one has no companyfacts response. This is preparation coverage, not public core-provider activation.

Four public SEC responses captured for AAON and Akamai show both report current revenue with RevenueFromContractWithCustomerIncludingAssessedTax. The existing parser recognizes only the excluding-tax concept and other revenue concepts. AAON therefore has no revenue records; Akamai incorrectly reaches only its old pre-2018 revenue series and returns unavailable.

Add the standard including-tax concept as the lowest-priority candidate. Existing net/total revenue retains priority when both concepts have the same latest reporting period. A single concept must supply the entire trailing and comparison period; no tax-inclusive/net mixture is allowed. Exact source tag, dates, accessions, input values and source hashes remain in calculation evidence. This uses reported revenue including assessed tax, not an assertion of net revenue or provider parity.

Preparation version advances to sec_ratio_preparation_v2 so an old cached absence does not survive its next normal scheduled preparation. Cache identity, request budgets, scheduler, canonical fundamentals and provider flags remain unchanged. Full rotation/market inputs and ranking/consumer gates remain before SEC core selection.

Python3.14.2:37 focused calculation/preparation/warming checks pass. Four regression checks cover actual concept math/provenance, same-period net-concept priority, rejection of mixed-concept annual/YTD history, and old-absence cache refresh with stable repeats. Offline22-company replay verifies44 hash-bound source responses, six AAON metrics and nine Akamai metrics, identical cache state on repeat, zero additional source reads on repeat, and zero provider requests/canonical writes/events/emails. Seven of22 retain explicit coverage gaps; this is not universal financial coverage.

Source captures and results are retained locally under artifacts/direct-feeds/free-replacements-2026-10-09/core-gap-sources-oct10 and sec-revenue-concept-22-replay.json. Deployment and normal scheduled v2 observation remain pending at this checkpoint. FMP remains on; Massive prices last.
