# Reviewed TEVA and MRX ordinary-share references

November 26, 2026. Candidate on deployed `151c4d71`, not deployed or activated.

Massive's retained September references for TEVA and MRX have no share-class or composite FIGI. The existing adapter therefore holds both, despite returning the expected ordinary-share CUSIP, CIK, symbol and US exchange. This affects 173 TEVA and 62 MRX Q3 filings in the complete first-pass mapping inventory. Those counts overlap and do not establish publication recovery.

Two complete retained primary sources independently establish the exact new ordinary-share classes. [NYSE's September 14 Form 25 notice](https://www.sec.gov/Archives/edgar/data/818686/0000876661-26-000744.txt) confirms TEVA's ADS conversion and continued NYSE ordinary-share listing under M8769Q102. [Marex's July 1 Form 6-K](https://www.sec.gov/Archives/edgar/data/1997464/0001193125-26-291648.txt) confirms Nasdaq trading under MRX and G5T40M104 from that date. The fixture directory retains both whole originals with checksum verification.

The candidate admits only those two exact CUSIP, CIK, symbol, exchange and ordinary-share tuples for September 2026 or later reporting quarters, with both FIGIs null. Existing active, US locale, USD, stocks-market, single-result, checksum, source URL and observation-time checks still apply. Earlier periods, former ADS/ordinary classes, unrelated issuers, malformed FIGIs and conflicting candidates remain held. Existing TotalEnergies handling is unchanged.

Each accepted identity retains its original Massive observation and response hash, plus the independent SEC source URL, complete-original hash, effective date and exact identity tuple. It does not rewrite an original reference, imply the reference was known on the filing date, bridge a corporate action or change historical holdings.

Validation: all 77 focused cases pass, including 30 new complete-original, negative-boundary and immutable-staging cases. A separate read-only audit independently hashes and reparses all 18,884 frozen references plus 1,767 separately retained references. Of 20,651 unique identities, exactly the intended TEVA and MRX September references change from held to verified; the other 20,649 complete identity outputs remain equal. Exact Linux CI, actual publication replay and any production release remain pending.

Artifacts: `reviewed-ordinary-tests-nov26.xml`, `reviewed-ordinary-fixture-proof-nov26.json` and `reviewed-ordinary-complete-parity-nov26.json`. No provider request, canonical write, source selection, pricing change or customer send was made by this qualification. The active frozen replay and approved reference collector keep their original code and inputs.
