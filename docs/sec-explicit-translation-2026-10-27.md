# Explicit currency translations and split statement headings

## Confirmed problem and shared repair

The complete Baozun August 27, 2026 SEC release explicitly identifies RMB to US-dollar translations as reader convenience amounts. The existing parser recognized translations *from* a currency, but missed translations *of* a currency. Its income-statement title also ends with a separate `INCOME` paragraph, and empty layout paragraphs pushed the balance-sheet issuer outside the previous eight-node context. Together these produced translation-basis and current-statement-alignment holds despite explicit source evidence.

The repair is generic, with no ticker-specific rule. It accepts explicit `translation of RMB into US$` and `translations of certain Renminbi (RMB) amounts into U.S. dollars (US$)` declarations using the existing currency aliases, direction and nearby convenience declaration checks. It joins a split comprehensive statement title only with the exact adjacent word `INCOME` or `LOSS`. A failed issuer-context check can retry with the last eight nonempty blocks within a maximum of 64 eligible layout nodes. The closest statement owner still controls the table; unrelated companies, adjusted statements and unproved currencies remain rejected.

Discovery qualifications advance to version 2 so old held or qualified receipts require original-source revalidation when ordinary collection next runs. This does not force a refresh, alter schedules or select the discovery reader. The table methodology remains unchanged for already supported sources. Core fundamentals remain FMP; discovery collection is enabled and its public reader remains disabled.

## Complete source evidence

The source is accession `0001104659-26-102080`, CIK `0001625414`, filed August 27, 2026:

- [SEC cover](https://www.sec.gov/Archives/edgar/data/1625414/000110465926102080/tm2624188d1_6k.htm), SHA256 `93421853b02dd7b3ed4e44f3bd6c928054e4a28bca4b962bdfa5d9407cf40956`.
- [Financial results exhibit](https://www.sec.gov/Archives/edgar/data/1625414/000110465926102080/tm2624188d1_ex99-1.htm), SHA256 `02d6f835dfd5803f7a16d8ea9f386c7cd6831f0969f1a915bd55590fc4565cc0`.

The two already identified exhibit links were fetched once, with no redirect or access-denial retry. The second exhibit is retained for research but is not part of this qualification proof. Compressed fixtures retain the complete cover, financial exhibit and original company-directory/companyfacts bytes; every test verifies their lengths and hashes. The metadata snapshot is from October 17. This is an explicitly identified filing replay, not a fresh full-directory coverage claim.

The complete replay qualifies 14 native observations and preserves CNY values. June 30 current assets are 6,751,999,000 and current liabilities 3,554,552,000, giving current ratio 1.8995358627472605. Q2 revenue is 2,742,973,000 for 2026 and 2,552,661,000 for 2025. Core and panel outputs repeat exactly. No convenience USD amount is substituted, no FX conversion or annualization occurs, and EPS/free cash flow remain absent. This recovers partial statements and one core ratio, not full financial classification or publication readiness.

Receipts: `artifacts/bzun-financial-originals-oct27/qualification-baseline.json` (translation hold), `qualification-translation-only.json` (alignment hold), and `qualification.json` (qualified partial output). All replays prohibit network access and make no production writes.

## Validation and release status

The initial Linux Python 3.12 run passed 249 checks and exposed one existing ASC raw-HTML date-header failure. Exact deployed `f7ef4573` and proposed parser replays both produce the same date/issuer holds (`artifacts/asc-existing-date-hold-oct27.json`). That test is not marked passing or weakened. ASC's existing tagged financial path is unchanged; the separate raw-table date repair remains open.

The first 49-original comparison found unchanged financial values but additional equivalent declaration text in YI's evidence. The repair now checks all explicit currency directions for conflict before retaining established equivalent evidence. New tests cover both conflicting direction orders and exact legacy-proof stability. The final affected Linux run passes 99 checks. There are 252 distinct passing identifiers across the two Linux runs, not a single fully passing suite of that size. Initial receipt: `artifacts/translation-linux-first-oct27.xml`; final receipt: `artifacts/translation-linux-final-oct27.xml`.

The final frozen comparison preserves full acquisition, panel, core, hold, provenance and request output for all 49 retained cases against `f7ef4573`. Receipt: `artifacts/translation-release-parity-oct27.json`; process CPU 244.969 seconds, no provider requests or production writes. The complete Baozun fixture separately verifies the intended new partial recovery. `artifacts/translation-test-sources-oct27.json` pins all changed application files, the regression test and four complete fixtures/manifest to the reviewed release.

Release, installed hash verification and ordinary scheduled observation remain pending at this checkpoint. No public-reader or FMP switch is authorized by these local receipts.

At 10:45 UTC, the actual installed worker planner had 105 due scopes, with PAGS at position 105. Its incomplete checkpoint is pending behind ordinary work, not proven stuck. No scheduler change was made. NBIS retains its ordinary 21:41 UTC deadline; Meta's index retry remains due October 28 at 06:13 UTC. Neither was forced.

Broad financial coverage/definitions, selected-reader and scheduled-consumer checks, institutional capture approval/publication, transcript coverage, sustained capacity, Massive last and zero-FMP/billing closure remain open. This repair does not change hosting, subscriptions, customer sends, landing pages or paused social publishing.
