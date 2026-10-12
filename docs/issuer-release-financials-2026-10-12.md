# Issuer financial release preparation, October 12, 2026

The owner approved expanding SEC and company-source coverage before another subscription. This preparation targets the previously identified AutoZone period gap: an earnings release can precede the annual financial statements available through companyfacts. It does not select a new public provider or establish marketwide coverage.

## Source evidence

The [AutoZone issuer release](https://investors.autozone.com/news-releases/news-release-details/autozone-4th-quarter-total-company-same-store-sales-increase-15) was visible through the web reader, but the identified direct download returned403. Its [SEC-filed Exhibit99.1](https://www.sec.gov/Archives/edgar/data/866787/000117184326006159/exh_991.htm) succeeded:187952bytes, SHA256 `eb434101a0243d150a95d9acc07d88dae36dd2083c447b0b4a887127583f588c`. These are alternate hosts for the same release, not two distinct corporate events. Only the captured SEC original is accepted by this preparation adapter.

The release date is September22. The selected GAAP annual table covers52weeks ended August29,2026 and52weeks ended August30,2025. Amounts are in USD thousands except EPS; raw inputs preserve the distinction. The16-week quarter, share-count rows and adjusted debt/EBITDAR table are not substituted for annual GAAP inputs or net debt/EBITDA. New fiscal releases require review. No intraday historical availability is claimed; the parser requires the New York publication day to be complete.

## Implemented preparation

`backend/app/services/issuer_release_financials.py` validates the reviewed URL, issuer/ticker, publication date, unique annual table, period headings, units, required components and four accounting equalities at reported precision. It produces a deterministic source-bound receipt, explicit formula/input evidence and `publication_eligible=false`. It has no network or database calls and does not modify canonical caches, scores, events or alerts.

`backend/scripts/prepare_issuer_financial_release.py` accepts a saved original and writes a new receipt without overwriting an existing receipt or the source. Run from backend with `python -m scripts.prepare_issuer_financial_release ORIGINAL --output RECEIPT`.

| Prepared metric | Result |
|---|---:|
| Annual revenue growth |7.3914088267%|
| Annual diluted EPS growth |5.3013046179%|
| Annual gross margin |52.3388903489%|
| Annual operating margin |18.3066791127%|
| Annual net margin |12.6497531413%|
| Annual operating margin change |-0.7556258999 percentage points|

Reported annual revenue20.338555billion versus18.938717billion, operating income3.723314billion and diluted EPS152.55 versus144.87 are separately asserted in the original replay. No balance-sheet, cash-flow, valuation, analyst-consensus or net-debt metric is filled by this adapter.

## Validation and remaining work

Python3.14.2:71 financial preparation, SEC, Finnhub and financial-alert checks pass, including19 new issuer cases. The first test invocation used the repository root and collected no tests; the correct backend invocation and final combined run pass. The actual captured original produces identical full receipts twice, equal to the saved CLI receipt. Receipt SHA256 `bd96e92c992ab728db0fbaf7463677f72ed00712136dc6d1d04fe09c3237b17e`; two replays use0.046875CPU seconds locally. No database module, source request, email or production write occurs in the replay. This is not a production capacity benchmark or all-suite claim.

Local originals and receipts: `artifacts/direct-feeds/free-replacements-2026-10-09/issuer-financials-oct12/`. Broad collection/discovery, persistent preparation integration, parity with current public values, cross-period cache rules, score/consumer acceptance and activation remain open. The existing reviewed SEC earnings pipeline should supply identity and availability evidence when this preparation is integrated; do not add a duplicate event or request-time scrape.

Accenture also publishes an [FY2026 earnings release](https://www.investor.accenture.com/~/media/Files/A/accenture-v4/investors/earnings-reports/2026/accentures-fourth-quarter-fiscal-2026-earnings-release.pdf). It is a discovered candidate only: no original replay or adapter is complete. Yahoo/Seeking Alpha remain unverified feed candidates, Quiver stays excluded and Massive stays the final price subscription. No purchases, new recurring schedules, landing changes or FMP cancellation.
