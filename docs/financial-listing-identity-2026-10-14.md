# Financial listing identity repair, October 14, 2026

## Reproduced gap and source review

The full-universe financial consumer audit found missing Finnhub preparations for ASML, SAP, NVS, FOX and FWONK. A bounded six-symbol diagnostic at17:47:47UTC used the existing authenticated client and shared provider limiter. Each of these five requests returned financial data but normalization rejected its reported symbol: ASML.AS, SAP.DE, NOVN.SW, FOXA and FWONA respectively. No price, subscription or paid endpoint was requested. The sixth symbol, NUE, has dated ratios ending April4; the existing150-day freshness guard correctly holds them. Its current scalar snapshots do not establish that the underlying dated observations are fresh. That guard remains unchanged.

The five exact associations are now reviewed within the existing consolidated issuer policy. The SEC directory and submissions independently verify their CIKs and U.S. tickers. Issuer pages establish the specific listing/share-class relationships:

| U.S. symbol | Returned source | SEC CIK | Primary identity evidence |
| --- | --- | --- | --- |
| ASML | ASML.AS | 0000937966 | [ASML share information](https://investor.asml.com/share-information), Nasdaq and Euronext listings |
| SAP | SAP.DE | 0001000184 | [SAP basic stock data](https://www.sap.com/investors/en/stock/basic-data.html), Deutsche Boerse and NYSE ADR |
| NVS | NOVN.SW | 0001114448 | [Novartis shareholder FAQ](https://www.novartis.com/investors/shareholder-information/frequently-asked-questions), NOVN and NVS |
| FOX | FOXA | 0001754301 | [Fox issuer announcement](https://investor.foxcorporation.com/establishment-of-fox/), Class B and Class A |
| FWONK | FWONA | 0001560385 | [Liberty Media FAQ](https://www.libertymedia.com/investors/stock-data/faq), Series C and A of the same Formula One group |

Each registry entry also retains the specific current SEC report URL from independently fetched submissions. Formula One permission is only for the exact two classes of that group; other Liberty tracking stocks, spun-off companies and even unreviewed share classes remain rejected. These associations do not prove accounting-definition equivalence to FMP or permit historical group/company scopes to be combined arbitrarily.

Only the existing nine company-level fields can transfer: current ratio, debt/equity, ROE, ROIC, gross/operating/net margins, revenue growth and annual EBITDA growth. Prices, beta, valuations, EPS and other per-share values stay excluded. Existing SEC values retain precedence, older dated metrics remain held against newer SEC periods, and independent matching CIK and field provenance are mandatory at combination. No fuzzy aliases, suffix stripping, share conversions or currency amounts are added. Existing exact-security payloads and prior reviewed pairs are unchanged.

## Actual-source replay

Captured payloads normalize to45 prepared fields across five companies. Combining with read-only production SEC preparations adds39 fields:9 each for ASML/SAP/NVS,7 for FOX and5 for FWONK. All existing SEC values remain identical. The real cache adapter, screener projection and financial summary run in isolated SQLite; repeat produces identical full rows and exactly five cache records, zero events and monitoring records, no network or email.

FWONK now has three scored inputs and a mixed financial classification. ASML/SAP/NVS/FOX each have two scored inputs and remain unavailable under the unchanged three-input requirement. Do not describe recovery of descriptive financial ratios as completed core coverage or a full Confirmation Score validation.

Local receipts: artifacts/metric-identity-probe-oct14.json; listing-financial-baseline-1792000267.json; listing-identity-replay-oct14.json; metric-identity-sources-oct14/sources.json. The SEC source manifest SHA256 is d3a9033d12497a2ddb6b3e4fc421b9537cec66feffda2e60b4dbfca319a0ad88. Source observation times, full values and hashes remain in receipts. The replay uses Python3.14.2. No customer data was exported.

An additional six-public-file SEC review of HCA/NUE/BRK-B confirms absent standard current operating-income series. Pretax income and total costs are not silently relabeled as operating income. No application derivation was added from that investigation.

## Validation and release state

Before implementation, expanded identity tests fail10/pass39 on Python3.12.3, reproducing all five rejected associations. An initial invocation lacked isolated database configuration and failed collection; corrected SQLite invocation reproduced the defect. A later expanded invocation included a nonexistent test filename and ran no tests; it was corrected. Final affected regression result and release verification follow below.

Implementation is limited to the reviewed identity registry and regression tests. Public core remains FMP, institutional publication disabled, transcript and other gates unchanged. No schedule, capacity, subscription, customer email or cancellation changes. Automatic daily metric preparation can acquire the new valid pairs on its existing rotation; current denied attempts are not erased or represented as successful observations. Exact deployment and ordinary propagation remain pending.

Final affected Linux Python3.12.3 regression selection passes136 checks: identity, metric normalization, annual metric growth, financial alert basis and selected-provider consumers. This is not a full-suite pass.
