# House publication holds and reviewed member name

October 15, 2026. Local implementation; release and live publication not established at this checkpoint.

## Current complete held cohort

A bounded read-only audit captures all eight current House publication holds. All eight original PDFs are independently fetched from the official House source and their full hashes match the production source receipts. Parsing retains all137 rows. No source payloads or customer records were exported from production; independently downloaded public originals and extraction receipts are in artifacts/house-holds-oct15/. The original Allen and Meuser pages were rendered with Poppler and inspected visually.

| Filing | Rows | Current finding |
|---|---:|---|
| 20035562, Salazar | 42 | Original parses; staging is quarantined because existing canonical records differ. |
| 20035464, Fields | 5 | Original parses; existing canonical reconciliation remains required. |
| 20035549, Taylor | 5 | Legacy disclosure date mismatch and ambiguous lots/instrument/member/date identity. |
| 20035622, Cisneros | 57 | Includes an explicit THR exchange action; current publisher accepts only purchases/sales. Non-stock bonds also remain source-specific. |
| 20035588, Hern | 24 | AIO is stored as Other Investment Fund while the disclosed row is parsed as stock; retain classification hold. |
| 20035487, Meuser | 1 | Original explicitly discloses NVIDIA common stock, but the shared NVDA security row is Stock Option. Do not relabel shared security without preserving historical instrument semantics. |
| 20035648, Allen | 2 | Exact official Richard W. Allen name differs from the current directory's Rick W. Allen. Repaired locally below. |
| 20035651, McCormick | 1 | Original says Richard Dean Dr McCormick and GA06; current term is GA07. Name matching alone cannot resolve the district discrepancy; remains held. |

Audit receipts: artifacts/house-holds-1792107148.json, house-holds-1792107246.json; public original/hash/row manifest artifacts/house-holds-oct15/replay.json. These eight publication receipts do not represent every staging-only scanned or unresolved House document; broader coverage remains open.

## Reviewed identity repair

The [official Allen filing](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035648.pdf), SHA25669ae5a2b6ab70b7fa3d68adb3c852343f8963599c43738a3cee6e01b5c5b918e, identifies Hon. Richard W. Allen as Member for GA12. The [House Clerk member record A000372](https://clerk.house.gov/Members/A000372) identifies Rick W. Allen as the representative for GA12, with the current oath date January3,2025. The reviewed directory identifies that same member/term. This supports an explicit reviewed name alias, not general Richard/Rick nickname inference.

Only the exact normalized name Richard W. Allen is mapped to A000372, and only in House resolution. Current district, chamber, term dates and uniqueness checks remain mandatory. Wrong district, expired terms, other middle initials, unreviewed abbreviated names and duplicate directory candidates still hold. Source names, original bytes, metadata, trade/disclosure/arrival dates, row hashes, deduplication and all other publication checks are unchanged. McCormick's contradictory district is not overridden; [his official member website](https://mccormick.house.gov/) identifies the current GA07 office.

The complete original contains two spouse sales: AZO on September22,2026 in the15001-50000 bracket, and COST on September18,2026 in the1001-15000 bracket. The source filing date remains October14. Source-backed tests preserve both lots and their published IDs/timestamps/payloads on repeat, with zero EmailDelivery rows and network blocked.

## Validation and state

Before repair, two source/identity checks fail and seven negative guards pass. The repaired run passes138 focused tests on LinuxPython3.12.3 across reviewed identity, Congress reconciliation/publication, non-stock repairs, dates and legacy repairs. An earlier expanded invocation named a nonexistent test file and ran no tests; the corrected existing paths produced the138 result. This is not a full-suite claim.

The installed-code verifier passes local rehearsal on WindowsPython3.14.2, binding six module/directory hashes and replaying the complete original, canonical repeat and four negative identity guards in isolated SQLite with autoflush disabled. Installed-runtime proof follows the reviewed release. No schedule/provider flag/subscription/hardware change, no private-export retry, no source-classification rewrite or customer email. FMP remains enabled. Exact release, ordinary publication and all wider coverage/consumer/capacity/final-provider/zero-FMP/billing gates remain.
