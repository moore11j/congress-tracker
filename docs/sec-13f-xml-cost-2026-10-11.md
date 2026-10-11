# SEC 13F XML processing cost

October 11, 2026. Institutional publication remains disabled.

The actual 100-filing publication batch used 141.297 CPU seconds locally. A separate profile of that batch confirms 918 original parses, about 98.7 cumulative seconds in submission parsing and 47.5 seconds in repeated XML field lookup under profiling. Hashing and SQLite reads are other substantial costs. Profile timings include instrumentation and are not ordinary production runtime.

The change indexes direct information-table fields once per row and reuses the XML tree already parsed by the submission validator. It preserves the first matching leaf across namespaces and repeated parent elements, including empty first values. Existing cover totals, finite/nonnegative numbers, filing identity, periods, completeness, checksums and publication holds remain enforced. The public bytes parser and saved-source result format are unchanged.

The accepted snapshot SHA256 is `5762d171fbf53a617b9b0681cb488b376c89950065fadd95bb002c5f38ee066b`. Parsing all 718 captured originals, comprising 157,503 positions, produces exactly the same serialized source text, complete parsed payload and hold reasons before and after. Local Python 3.14.2 CPU time for this parser-and-fingerprint comparison falls from 53.500 to 32.266 seconds, approximately 40%. This is not a production latency or overall batch improvement claim.

All 146 focused checks pass: 74 parser, evidence, prepared-evidence, publication and batch tests, plus 72 collection, worker and legacy snapshot checks. The first run passed 71 tests and hit three temporary-directory permission errors before those cases ran; rerunning with an isolated writable temporary directory passed all 74. The separate unprofiled actual batch comparison is in progress.

Evidence under `artifacts/direct-feeds/free-replacements-2026-10-09`: `institutional-actual-batch-profile-oct11.json` and `.profile`, `institutional-xml-before-oct11.json`, `institutional-xml-after-oct11.json`, and `benchmark_13f_xml_oct11.py`. The batch comparison writes a distinct `institutional-xml-batch-oct11.json`, retaining the original measurement.

No new source selection, schedule, purchase, storage allocation or customer email is part of this change. Complete source growth bounds, current consumer validation, other free replacements and final Massive verification still gate FMP shutdown.
