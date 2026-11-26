# Reviewed TEVA, MRX and DPC ordinary-share references

November 26, 2026. Draft PR854 on deployed `151c4d71`. Implemented and locally tested; revised exact CI and deployment remain pending.

Massive's retained references for these three ordinary-share classes lack both FIGIs. The shared adapter therefore holds them despite returning the expected CUSIP, CIK, symbol and US exchange. The complete first-pass inventory contains 173 TEVA September scopes, 62 MRX September scopes, and 52 June plus 43 September DPC scopes. These filing/security counts overlap and do not establish publication recovery.

Four complete SEC originals independently establish the classes and listing dates:

- [NYSE's September 14 TEVA conversion notice](https://www.sec.gov/Archives/edgar/data/818686/0000876661-26-000744.txt) confirms the ADS conversion and continued ordinary-share listing under M8769Q102.
- [Marex's July 1 Form 6-K](https://www.sec.gov/Archives/edgar/data/1997464/0001193125-26-291648.txt) confirms Nasdaq trading under MRX and G5T40M104 from that date.
- [DPC's June ownership statement](https://www.sec.gov/Archives/edgar/data/2107018/0001193125-26-345016.txt) identifies ordinary shares, CUSIP G2R11M108, CIK2107018 and a June30 reporting event.
- [DPC's June-quarter Form10-Q](https://www.sec.gov/Archives/edgar/data/2107018/0001104659-26-093772.txt) identifies DPC ordinary shares on NYSE and the June26 IPO completion. The entire8,866,194-byte submission is retained; the ownership submission is125,518bytes.

The candidate accepts only these exact CUSIP, issuer, symbol, exchange and common-share tuples, with both FIGIs null, for quarter ends on or after each documented effective date. Replacing the former shared September floor with the tuple's effective date preserves the TEVA/MRX quarter boundaries and permits DPC June/September. Existing active, US locale, USD, stocks-market, single-result, checksum, URL and observation-time checks remain. Other issuers/classes, malformed FIGIs, earlier quarters and conflicting candidates remain held. TotalEnergies behavior is unchanged.

Each identity retains the actual Massive observation and response hash plus complete SEC hashes and the exact reviewed identity tuple. DPC retains both its class and listing sources. Later SEC evidence does not become a historical observation: availability still uses the real retained reference observation, and the candidate never rewrites holdings or bridges a corporate action.

All93focused checks pass, including16new DPC cases. The initial full-original assertions exposed zero-width separators in the SEC cover table; normalizing those in the test permits the exact class/ticker/exchange assertion. Application behavior was unchanged by that test correction. Actual staging/reinterpretation/repeat preserves original bytes and timestamps without new revisions.

Independent read-only parity hashes and reparses18,884frozen references plus1,915retained references,20,799unique records. Exactly four intended identities change from held to verified: TEVA/MRX September and DPC June/September. The other20,795complete outputs are identical. These tests preserve the active frozen replay and its inputs.

The1,915-source overlay alone supplies all missing identifiers for1,301of3,256mapping-wait filings. Exact candidate resolution with these reviewed listings reaches1,416; adding the separately prepared N-PORT source reaches1,471. No existing resolution changes in the measured missing-position scope. These are identifier results, not completed publication, repeat or consumer acceptance.

Evidence: artifacts/reviewed-dpc-tests-green-nov26.xml, reviewed-dpc-complete-parity-nov26.json, reviewed-dpc-full-first-qualification-nov26.json and dpc-identity-originals-nov26/manifest.json. Whole originals and retained responses are in backend/tests/fixtures/reviewed_ordinary_identities/.

The earlier8478b793CI and prepared853/854release chain are historical and must not be used to approve this revised head. Production stays852/151c4d71 while the bounded collector and frozen second validation run. Fresh exact-head CI, revised combined-source/release checks, full overlay publication/repeat/consumer acceptance and source activation remain separate gates. No deployment, canonical writes, source activation, pricing changes or sends occurred.
