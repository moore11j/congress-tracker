# Moderna transcript punctuation recovery

2026-11-25. Candidate based on production commit 8023b767. Not deployed or activated.

Moderna's complete November 6 Q3 2026 transcript identifies the company as `Moderna, Inc.`. The existing discovery policy required `Moderna Inc`, so both collection and prepared research rejected the latest quarter. A bounded production audit confirms `reviewed_period_not_prepared` and the fiscal-pattern error. The six previous quarters remain readable.

The candidate permits only the optional comma and period in this exact reviewed Moderna fiscal heading. It verifies the symbol, discovery policy, integer fiscal year/quarter and unchanged original pattern before selecting the parser expression. Publisher, ticker, anchored heading, event date, fiscal identity, source host and Q&A checks remain required. Existing metadata, canonical identities and stored output shapes do not change. Other issuer policies retain their original expressions.

## Source and consumer verification

The official [quarterly results index](https://investors.modernatx.com/quarterly-results) and [complete Q3 transcript](https://irp.cdn-website.com/89dd0539/files/uploaded/MRNA-USQ_Transcript_2026-11-06.pdf) were captured in two bounded requests. Six complete older PDFs were already retained. The manifest records all eight hashes, lengths, URLs and expected complete parser-output hashes, without redistributing the transcripts in Git.

- Baseline replay: six older originals accepted; latest Q3 rejected with the production error.
- Candidate replay: seven originals accepted, exact full metadata/text/parsed-output equality for all six older originals, and exact repeat equality for all seven. New Q3 text contains 60,854 characters.
- Complete collector replay: index plus six discovery PDFs requires seven requests; the existing static Q2 source is intentionally excluded by discovery. The separate seven-original parser replay covers that Q2 original too.
- Actual-source research replay covers new-document preparation and retained FMP history. Full database snapshots repeat identically, with one research document, zero evidence events and zero deliveries. Subsequent discovery makes zero requests.
- Wrong company, ticker, publisher, year, quarter, date, missing Q&A and tampered metadata remain rejected. A correct title later in the body cannot excuse a wrong heading.
- All 589 local issuer regression cases pass with no skips. Linux CI must independently fetch the eight hash-pinned complete sources and run the same suite before release review.

Local evidence: `artifacts/moderna-rollover-nov25/manifest.json`, `baseline-seven-originals.json`, `candidate-seven-originals.json`, `parity.json`, and the isolated worktree's `backend/moderna-all-issuer.xml`. CI fetches are bounded to eight exact allowlisted URLs, 2 MB per input, no redirect or retry; a changed hash fails for review.

## Wider migration status and release plan

The read-only audit of ten configured issuer readers finds eight prepared, Moderna held by this defect and Unilever held by its index access check. NVIDIA remains a fixed older Q2 FY2027 source; prepared does not prove latest-quarter coverage. Reddit Q3 ordinary preparation is now verified. Six of 22 companies with recent unavailable transcript research coverage are configured direct issuers. Last-six-hour telemetry contains 240 FMP transcript call events and 240 throttles; the events have no symbol, so they cannot be individually assigned to those companies.

This parser repair does not switch `TRANSCRIPT_PROVIDER`, change financial/scoring rules, create customer events or modify institutional jobs. After CI and exact release approval, coordinate the existing reference collector between requests, run guarded deployment, verify changed and consumer source hashes plus unchanged effective provider settings/reference hashes, then resume the same collector with its original deadline and counters. Observe ordinary Moderna collection and prepared-reader recovery before considering separate transcript activation. Existing institutional replay and both finite market observers must retain their bound inputs and processes.

The full FMP migration remains incomplete: financial consumer coverage, transcript breadth/freshness, institutional first/repeat/consumer acceptance and publication, scheduled observations and final zero-FMP verification are still open.
