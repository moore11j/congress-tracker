# SEC transcript cover and retained-metadata compatibility, October 15, 2026

## Problem and repair

The ordinary issuer run after PR683 held Magna accession 0001171843-26-002990. Its cover uses mixed case, a colon after Commission File Number, adjacent HTML text nodes and a May 4 signature preceding its official May 5 filing date. The source has no transcript; rejecting it prevents the collector from confirming inspected coverage. A complete public-source replay found nine such cover failures and two unsupported year-end transcript labels in 31 reviewed covers.

The repair reads separated HTML text nodes, retains issuer/form/file-number checks, and distinguishes the signature, call, fiscal and filing clocks. Availability still uses the official filing date and completed-day guard. Reviewed annual labels distinguish the prior fiscal year from the following outlook year. Q1/Q3 titles and explicit operator transitions match the captured complete originals. Two transcripts have no standalone call date; their linked SEC cover explicitly dates the exhibit. These carry transcript_date_source=sec_cover and still require the exact fiscal heading, disclaimer, Q&A and closing.

Existing parsed cover/transcript receipts may retain the exact previous heading/Q&A policy. Compatibility accepts only those known policy fields and the explicit cover-date annotation; URLs, original hashes, parent chain, source clocks, fiscal identity and every other field remain exact. Never-parsed pending transcripts may adopt the reviewed expanded policy. Existing originals and canonical identities are not rewritten. The prepared reader independently reparses the immutable transcript using the specific SEC policy, keeping the six-query bound.

## Source evidence and checks

Captured official directory: https://data.sec.gov/submissions/CIK0000749098.json, SHA256 a17dedcb78e733bf2f52277ec5e9988ea79a907d4dcd41720fa5646327d592db at 2026-10-15T20:18:30.760900Z. All 31 exact cover URLs/hashes/bytes and seven full transcript originals are inventoried in artifacts/mga-transcript-cover-repair-oct15/cohort.json and transcripts.json. No production-derived payload export was used.

Final focused and affected issuer/free-research regression selection: **445 passed on Linux Python 3.12.3**. New cases cover signature versus filing date, annual/outlook identity, missing dates, source/date tampering, exact legacy receipt preservation and repeat publication. Early failures were invalid synthetic HTML/absent interparagraph whitespace; fixture boundaries were corrected to match the complete originals, without weakening the parser.

Complete saved-source collection/reader/research replay passes on Windows Python 3.14.2 and Linux Python 3.12.3: all 31 covers and seven transcripts parsed, 39 unique simulated requests over 20 nonempty batches of at most two requests, six reader queries, one canonical research document, zero evidence events, and exactly unchanged complete database on repeat. Network is blocked and FMP disabled. Receipts: artifacts/mga-transcript-cover-repair-oct15/cohort-replay-win32.json and cohort-replay-linux.json.

Current Q2 2026 text is 63,667 characters; Q1 2026 56,168; Q4 2025 50,379; Q3 2025 54,781; Q2 2025 58,829; Q1 2025 53,110. Q4 2024 is 59,400 characters on Windows/libxml2 2.11.9 and 59,386 on Linux/libxml2 2.14.6: exactly two literal &puncsp; entities versus decoded spaces account for the difference. All other text is identical. Extraction remains the existing runtime-specific parser; immutable source bytes/hashes are identical and no stored text is rewritten. Full-text parity and repeat stability are established within each runtime, not falsely claimed across XML library versions.

The installed-code probe rehearses the new annual cover-date path, prepared reader, repeat, request bound and checksum rejection using only synthetic in-memory data. Six expected source/config hashes are recorded. Production execution remains pending.

## Release and remaining gates

Implemented and tested locally; exact reviewed release, installed verification and ordinary collection recovery are pending. The previous failed source retains its daily recheck boundary. No timestamp resets, forced collection, publication selection, paid subscription, new schedule, host, customer email, video or FMP cancellation. The earlier Unilever release a1488ead is deployed, but its ordinary acquisition remained pending in the latest observed run. Broad transcript/financial coverage, institutions, all scheduled consumers and sustained reliability, paid Finnhub and final Massive validation, zero global FMP traffic and billing retirement remain open.
