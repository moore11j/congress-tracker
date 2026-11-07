# Alphabet transcript archive rollover

The ordinary November 7 00:13 UTC collection rejected Alphabet's 2025 transcript index because its older URLs differ from the 2026 routes. The fourth-quarter 2025 call also occurred in February 2026. This prevented completing the reviewed archive; it does not explain or resolve every transcript coverage gap.

The adapter now normalizes the issuer's relative archive links and the observed canonical trailing slash, distinguishes fiscal year from event year for Q4, and accepts the explicit `America/Los Angeles` date label used by the Q2 page. Host, route, fiscal title, independent publication date, transcript body, Q&A, language, document type, duplicate and pagination checks remain enforced. No generic redirect following is added.

Five complete originals, 553,549 uncompressed bytes, are retained with SHA256 manifests under `backend/tests/fixtures/issuer_alphabet_archive_nov7`. The four calls resolve to April 24, July 23 and October 29, 2025, and February 4, 2026. Existing 2026 transcript text hashes remain unchanged.

Validation: 70 local checks pass, including all four archive originals, rejected identity/URL/timezone mutations, current-year parity, freshness and fair collection scheduling. A complete two-year collection uses at most two requests per run, prepares the latest 2026 Q3 call, and repeats with identical document/revision rows and no additional requests.

Release and installed verification are pending. The failed archive index remains subject to its existing daily retry boundary; this change does not force a retry, add a schedule, publish research, select a provider or send alerts. Core fundamentals, transcripts and prices remain gated separately from this collection repair.
