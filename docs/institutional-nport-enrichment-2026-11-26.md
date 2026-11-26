# SEC identifier enrichment for waiting institutional filings

November26,2026. Candidate on deployed151c4d71. Not deployed; exact CI and release review pending.

An existing source-owned filing with missing symbols could acquire a dated Massive mapping, but could not acquire a newly supplied, independently verified SEC N-PORT identifier. The ordinary new-filing path already accepts those SEC identifiers. This discrepancy leaves existing waiting filings blocked even when equivalent qualified evidence becomes available.

The shared projection now allows missing-symbol enrichment from exact N-PORT rows that the existing complete-original parser accepts and that were filed by the target filing date. It still evaluates all dated legacy and SEC candidates through the existing resolver. Conflicting or ambiguous candidates remain held. Unnormalized symbol drift, incomplete or changed sources, published pairs and priors supporting published pairs retain their existing protection.

Only source-owned missing positions can be enriched. Reported CUSIPs, shares, values, dates and position identities remain unchanged. The receipt retains the exact N-PORT source hash, URL, row, symbol and filing date used for recovery. When N-PORT evidence enables disambiguation of a Massive candidate, both proofs are retained. Repeating the same evidence leaves the full tested state unchanged and sends no email.

The new complete-original fixture is the SEC N-PORT submission0002066578-26-003341, filedSeptember28 for July31. The full8,148,718-byte source, compressed to248,377bytes, passes the existing parser and contains823accepted identifiers. Actual-source regressions demonstrate GTES enrichment after the disclosure date, rejection for an earlier prior filing, and GTM disambiguation while retaining unrelated historical ZI/ZOM rows. Holder filings in those tests are synthetic; these are not production recovery claims.

Validation:106focused checks passed before the last additional source-backed regression, then all13final new cases passed;112additional publication/source/preparation/security-transition cases passed. This is219distinct local checks. One initial final-test failure was an overbroad assertion that included unrelated fixture rows; narrowing it to the target CUSIP made the intended historical-preservation assertion pass. Exact Linux CI remains pending.

The running frozen replay, provider selectors, prices, entitlements and source manifests are unchanged. The new SEC original is included as a test fixture only. Actual full-cohort recovery, any reviewed production identifier-manifest addition, production runtime/capacity checks and source activation remain separate gates.
