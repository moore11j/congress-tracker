# Magna SEC transcript discovery, October 15, 2026

## Scope and source chain

Magna files earnings webcast transcripts as exhibits to SEC 6-K reports. The adapter discovers the exact official company submissions directory, verifies each cover's issuer and SEC commission identity, then follows its explicitly labelled transcript exhibit. It retains all original bytes, checksums, fiscal identity and the directory/cover/transcript evidence chain. The filing date establishes date-level source availability; the call date and fiscal quarter come independently from the exhibit description and transcript heading. No quarter is inferred from a filing date.

The reviewed directory currently contains 31 relevant covers. The adapter keeps every current/prior-year cover within its explicit 64-cover bound. A new uninspected filing dated at or after the selected transcript holds older output. Missing, failed, future or more than 48-hour-old directory verification also holds output. A same-day filing is discovered immediately but its cover cannot publish until the New York filing day has completed. Identical directory bytes can therefore mature without losing their original proof.

Two transcript layouts are verified: the current webcast heading and the older numeric-date TRANSCRIPT heading, each with the exact fiscal period/call date, actual first analyst question, operator closing and issuer transcription disclaimer. SEC's SGML envelope is accepted only with the linked exhibit filename. Changed links for one fiscal period quarantine the existing record; ambiguous same-period filings require reconciliation. No arbitrary website or sibling-file permission is granted.

The existing shared two-request collection budget and minimum daily per-page retry are preserved. An identified current transcript precedes older pending covers. Directory and cover proofs are loaded in batches, then reparsed from immutable bytes; the actual 31-cover reader uses six SELECTs rather than per-cover queries. Source preparation remains distinct from research publication. No provider selector or schedule change.

## Retained public originals

- Current cover: https://www.sec.gov/Archives/edgar/data/749098/000110465926089231/tm2618646d2_6k.htm
- Current transcript: https://www.sec.gov/Archives/edgar/data/749098/000110465926089231/tm2618646d2_ex99-3.htm, SHA256 0a09690ea4ba6c32daf766f7a584157a218fce28bca15b3693006042ee4f8eb5, 63,667 normalized characters.
- Prior cover: https://www.sec.gov/Archives/edgar/data/749098/000110465925073101/tm2519795d2_6k.htm, SHA256 e5b0b4bbe64eac9da72a4bca61f4c714634980bda4de0bdb6daaa38b3a2f0d9c.
- Prior transcript: https://www.sec.gov/Archives/edgar/data/749098/000110465925073101/tm2519795d2_ex99-3.htm, SHA256 f4722abfe4e055d2838d98a96753bfa20a2d68d0fb0b29fe1b531c34c4ac375b, 58,829 normalized characters.

The current non-transcript financial cover and complete company directory also participate in the saved-source replay. Source originals are retained under artifacts/mga-financials-oct15 and artifacts/coverage-gaps-oct14. Retrieval uses the existing identified, rate-limited SEC client. Initial sandbox networking was denied; reviewed public-source retrieval succeeded. No production-derived payload export was attempted.

## Validation

Final Linux Python 3.12.3 selection: 367 passed, including 49 new SEC policy/collector/reader tests and existing issuer, registry, transcript and free research consumer tests. Earlier Windows selection had 356 passes and three setup errors in existing tests with oversized parametrized IDs; the Linux suite passes. Final new 49 also pass on Windows Python 3.14.2.

The full saved-original replay passes on both runtimes. Two collection runs consume exactly two requests each: directory, transcript cover, current transcript and same-day financial cover. Both complete transcript texts match the retained originals. The current source produces one research document, zero research evidence events and an exactly unchanged complete database on repeat. Actual current reader query count is six. Synthetic tests also cover next-quarter rollover, stale/failed/future directories, immutable tampering at every hop, changed links, cross-provider exact reuse/conflicting-period holds and unchanged existing FMP records. These are isolated consumer checks, not live scheduled delivery evidence.

Receipts: artifacts/sec-transcript-final-linux-oct15.log; artifacts/sec-transcript-replay-linux-oct15.json; artifacts/sec-transcript-replay-win32-oct15.json. The installed-code synthetic probe has passed its local rehearsal with all external connections blocked; it has not yet run in production. Its initial exception handler was corrected to catch the actual DirectSourceError type.

## Release and remaining gates

Implemented and tested locally; exact release, installed runtime and ordinary scheduled discovery remain pending. Broader issuer coverage, institutional activation, core financial coverage/definitions, paid analyst/valuation decisions, all remaining consumers and sustained reliability, final Massive and global zero-FMP/billing remain. Pending production-derived audit/export approvals remain unchanged. FMP stays enabled. No subscription, hardware change, customer email, video, landing-page change or cancellation.
