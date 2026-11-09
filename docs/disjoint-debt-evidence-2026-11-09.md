# Original borrowing and finance lease reconciliation

## Problem and scope

The replacement SEC financials held AutoZone's leverage because its gross borrowing total does not explicitly include finance leases. A generic borrowing tag cannot establish whether adding leases would double count debt. This change requires the complete original borrowing instrument sum and separately classified current/noncurrent finance lease liabilities before allowing the existing ratio candidate.

The shared parser has no ticker-specific branches. It accepts only a complete USD gross principal table with supported standard borrowing instruments and an explicit finance lease classification outside borrowing accounts. It rejects incomplete sums, contradictory references, unknown classifications, wrong issuer/period/units, duplicate instruments, nil components and unavailable maturity components. An omitted disclosure never implies zero.

## Normalization and consumers

- Existing optional inline acquisition may fetch one current original within its unchanged six-document bound, only when the necessary companyfacts components already exist and no finance-inclusive aggregate/maturity pair supplies the answer. Source failures preserve normal collection backoff.
- The original proof is bound to the exact selected period, accession, filed date, values, tags, source identity and source hash. It does not replace companyfacts observations or repair unrelated fields.
- Existing debt conflict guards retain precedence. The final net debt policy revalidates the candidate against the original-derived proof.
- The measurement is explicitly disclosed as gross borrowing principal before discounts and debt issuance costs plus reported finance lease liabilities. A measurement transition changes the alert baseline; ordinary dates, source hashes and numeric changes do not hide real later changes.
- Core provider selection, pricing, institutional publication, schedules, historical snapshots and customer sends are unchanged. Current preparation caches retain their existing expiry; no global invalidation is introduced.

## Retained original evidence

The complete AZO 2026-08-29 annual filing, filed 2026-10-26, has SHA256 `f69d9fe80fa329d3d096fa18927d55b970a8689708a089b80d336e254649b796`. Fifteen notes/commercial paper observations sum to USD9,125,000,000. Separately classified finance leases are USD114,873,000 current and USD302,455,000 noncurrent. Combined debt is USD9,542,328,000; cash is USD326,115,000. Retained source projection yields net debt/EBITDA 2.082666809485135 with other projected values unchanged.

Complete FOX and FWONK filings are negative controls, not additional recoveries. Their absent qualifying lease disclosures stay held.

## Validation and release state

The original-document mutation tests include an unchanged serialization control. A first UTF-8 HTML round-trip corrupted whitespace because the filing declares another encoding; ASCII character references preserve semantics. The corrected 23 original-document cases pass. The first broad integrated run exposed test assumptions about absent dictionary fields; those were corrected without changing the application contract. The next 302 focused debt/source/consumer cases pass. A subsequent exact-source switch/repeat/rollback test passes with SQLite synthetic records, no network, zero alerts and no customer sends. The final measurement-baseline addition is under validation.

The 49 retained whole-acquisition comparison against deployed `8c842f04` and its identical repeat are running. These frozen sources cannot establish current production coverage. Linux exact-release checks, installed runtime proof and ordinary refresh remain pending. No deployment or financial source activation is claimed by this report.
