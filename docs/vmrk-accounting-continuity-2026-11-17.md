# VMRK accounting predecessor continuity

VMRK's November 9, 2026 filing describes an August 17 reverse acquisition. Equity Residential is the legal acquirer; AvalonBay is the accounting acquirer. Pre-merger comparative financial statements follow AvalonBay. Old Equity Residential companyfacts under the surviving CIK therefore cannot provide the annual anchor for these trailing calculations.

The implementation uses three complete, hash-pinned originals: VMRK's September 2026 10-Q, AvalonBay's December 2025 10-K and AvalonBay's September 2025 10-Q. Seven required overlapping revenue, parent-income, operating-cash-flow and equity observations agree. Every additional shared observation must also agree. Each output retains its original filing, source CIK, source hash and accounting-basis CIK.

| Current trailing input | Reconciled value, USD |
| --- | ---: |
| Revenue | 3,511,386,000 |
| Parent net income | -88,966,000 |
| Common-shareholder net income | -89,321,000 |
| Operating cash flow | 1,442,305,000 |

The existing formulas now yield net margin -2.5336434103%, revenue growth 16.5250224164% and ROE -0.3722803186%. The earlier ROE -0.0859695522% used Equity Residential's annual income and is rejected. Common earnings remain distinct from parent earnings by the explicitly reported 355,000 preferred distribution.

AvalonBay's annual parent income is reconciled from reported consolidated profit minus reported noncontrolling income and independently equals reported common-shareholder income. This does not generalize the successor's partial minority allocation into a complete allocation. Missing capex, unsupported operating/gross margins, per-share conversion and REIT leverage scoring remain unavailable. The overall fundamentals classification remains unavailable.

Original acquisition makes three unique document reads. A cache retains only these three validated original byte strings (16,269,280 bytes) and small parsed fact dictionaries, never HTML trees. Every use verifies original size/hash and current filing identity. Future periodic filings and amendments require another review; an unavailable or changed original cannot fall back to mixed legal-entity history. Ordinary preparation rejects legacy accounting-basis caches and repeats a valid prepared result without additional source reads. Historical recorded snapshots are not rewritten.

The financial panel follows the accounting predecessor and labels that basis explicitly. Common-income valuation uses the same bridge. Vendor metrics cannot fill unsupported ratios until their accounting history is verified, since matching a legal CIK does not establish the accounting basis.

Validation includes complete-original source and common-income reconciliation, statement panels, cache reuse and legacy rejection, provider isolation, changed/missing originals, conflicting facts, wrong identities, future filings/amendments and fallback holds. The bounded offline script `backend/scripts/verify_reverse_acquisition_fixture.py` verifies five retained sources, exact result/request repeat and unchanged unavailable classification under a 10-second CPU soft limit, 12-second hard limit and 22-second wall limit. No network is permitted during replay.

Sources: [VMRK current original](https://www.sec.gov/Archives/edgar/data/906107/000119312526453735/vmrk-20260930.htm), [AvalonBay annual original](https://www.sec.gov/Archives/edgar/data/915912/000091591226000004/avb-20251231.htm), [AvalonBay comparative original](https://www.sec.gov/Archives/edgar/data/915912/000091591225000025/avb-20250930.htm). Exact identities and hashes are in `backend/app/services/sec_reverse_acquisition_review.json`; complete compressed originals are in `backend/tests/fixtures/sec_reverse_acquisition/`.
