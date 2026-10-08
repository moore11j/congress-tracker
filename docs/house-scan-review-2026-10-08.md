# Reviewed House scanned PTR fallback

October 8, 2026. Part of the owner's approved FMP replacement. This checkpoint resolves extraction of one exact scanned source, not House cutover or general OCR coverage.

## Verified source and mapping

All seven pages of the four currently failed House scans were rendered and visually inspected. Tony Wied's [one-page initial PTR 9116361](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/9116361.pdf) contains four printed rows. The form is stamped received October 2 and the Clerk's directory identifies October 2, 2026, WI08. Its SHA-256 is `438b1c89ec6f78c9ec39f30799b499a8e2fb734cd53dd5b2787f71a6d3b01385`.

All four rows show joint ownership, partial sale, transaction date September 17 and notification October 2. Row 1 checks amount C ($50,001–$100,000); rows 2–4 check B ($15,001–$50,000). The form has no printed tickers. The reviewed mappings below are an inference from the disclosed issuer/share-class names and independent SEC listing evidence, not a claim that tickers were printed.

| Printed asset name | Mapped ticker | Independent listing evidence |
|---|---|---|
| SERVICENOW INC | NOW | [June 2026 SEC cover](https://www.sec.gov/Archives/edgar/data/1373715/000137371526000076/R1.htm): ServiceNow common stock, NYSE |
| THE TRADE DESK INC CLASS A | TTD | [June 2026 10-Q cover](https://www.sec.gov/Archives/edgar/data/1671933/000167193326000086/ttd-20260630.htm): Class A common stock, Nasdaq |
| UPSTART HLDGS INC | UPST | [June 2026 10-Q cover](https://www.sec.gov/Archives/edgar/data/1647639/000164763926000063/upst-20260630.htm): Upstart common stock, Nasdaq |
| CHIPOTLE MEXICAN GRILL I | CMG | [2025 10-K cover](https://www.sec.gov/Archives/edgar/data/1058090/000105809026000009/cmg-20251231.htm): Chipotle common stock, NYSE; truncated original description retained |

## Implementation and checks

`backend/config/house_ptr_reviews.json` is a reviewed transcription registry. The fallback in `house_ptr_review.py` requires exact source bytes, full discovery identity, page and row counts, unique printed row references, complete checkbox/date fields and SEC security evidence. It only applies when the PDF has no text layer. Unknown or changed scans remain held. It does not download or infer a review from external content.

Raw ticker remains absent; normalized ticker is enriched with `reviewed_sec_listing` provenance. Raw checkbox values, original descriptions, source page, SEC URLs and review/registry hashes remain in the source report. Canonical row hashes remain unchanged by ticker enrichment. This is a visually reviewed transcription, not automated OCR.

102 focused checks pass on Python 3.14.2, including real scanned/digital PDF fixtures, changed bytes, metadata mismatch, incomplete/ambiguous review, date and amount guards, Congress reconciliation/publication and collector schedules. An initial run had nine temporary-directory permission errors; rerunning with an isolated writable artifact directory passed. No full-suite claim.

## Existing-record conflict remains held

Offline reconciliation against the read-only production baseline captured earlier October 8 finds all four existing stock events (471345, 471346, 471347, 471670) and four transactions under filing 19816. Their stock economics match, but FMP records the filing/report date as October 7. The source directory and received stamp say October 2. The guarded publisher holds the entire filing; it will not insert four duplicates.

No existing dates or historical performance have been rewritten. A follow-up correction must distinguish official filing date from observed availability and preserve original event identities, ingestion times, historical strategies and delivery state. The three other scanned forms contain 33 mostly debt/private-fund rows, with degraded names; they remain held. This is not a claim that all scanned House coverage is complete.

Local evidence: `artifacts/direct-feeds/house-scan-review-2026-10-08/canonical-review.json`, original PDFs/discovery in `artifacts/direct-feeds/congress-collection-2026-10-08/`, rendered pages in `tmp/pdfs/house-scan-review/`. These artifacts are local-only; the public fixture and registry are included in the release.

Deployment, live staging retry and subsequent Senate schedule observation are pending at this preparation checkpoint. House ownership remains FMP; global FMP remains active. No emails or subscription changes.
