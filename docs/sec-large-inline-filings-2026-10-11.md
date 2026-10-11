# Complete larger SEC inline filings

October 11 UTC / October 10 local, 2026. Free fundamentals preparation; public core selection remains FMP.

The ordinary SEC ratio rotation reports `InlineFactsError` for PFG at04:21UTC. Four identified official SEC requests reproduce `filing_size`: the actual June30 10-Q is16,055,207bytes, above the US GAAP parser's8MB ceiling. It is an accepted transport size but never reached financial validation. This is an implementation coverage gap, not an unavailable source or paid-provider requirement.

The repair accepts complete US GAAP/IFRS inline originals up to20,000,000bytes and rejects larger originals before DOM allocation. It retains issuer, accession, filing/report date, namespace, dimensional-context, currency, transformation and conflicting-fact validation. No slicing, guessed values, relaxed financial checks or new provider is added. Each original's SHA256 is computed once and reused for every field's provenance and the document receipt, replacing repeated hashing of the same large bytes.

## Source and replay

[PFG June30 10-Q](https://www.sec.gov/Archives/edgar/data/1126328/000110465926088071/pfg-20260630x10q.htm): accession0001104659-26-088071, filedJuly29 per the captured submissions directory. SHA256 `c33ee7d0419372d7e299c4d50b22d792a4277a7a648c81b4b320778efb72eddc`. Companyfacts SHA256 `4e877284132e01a1220db032ec6b1dee886b5e88920b4a04cba058047f0bfae8`; submissions `8d4a53650fa32039d1646631dce8be31ef364232891cf95233935c0e3182cd09`. Captured04:45:55UTC, all originals retained with exact hashes under `artifacts/direct-feeds/free-replacements-2026-10-09/pfg-inline-oct11/`.

The saved-source replay uses actual `acquire` and `project_fundamentals`, selecting the filing from the original directory without hand-entered dates. It recovers16 standard facts and three current ratios: net margin9.932964596%, revenue growth3.716921775%, ROE13.233664854%; periodJune30. Missing ratios remain missing. Local Python3.14.2 used2.656CPU seconds with176.33MB peak working set. This is one isolated process, not production capacity evidence. No provider requests, database writes or emails during replay.

An initial exploratory parser-only probe supplied a guessed filing date; that diagnostic is not accepted provenance and is superseded by the directory-driven replay above. The initial memory-measurement harness required unavailable psutil and exited before parsing; it now uses native Windows process counters without installing packages.

## Validation and release gate

103 focused inline, foreign statement, fundamentals, preparation and continuity checks pass. New tests exercise a17MB complete source under both standard and explicit accounting-basis modes, identical values/source hashes, one hashing pass, and rejection above20MB before DOM allocation. Runtime/source deployment and ordinary PFG preparation remain unverified. No schedule, quota, source selector, market inputs, subscription or FMP cancellation changes.
