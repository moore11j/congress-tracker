# Reddit issuer transcript, October 10

Reddit's official investor quarterly-results page loads its financial reports from the same-host public Q4 API. The captured API response links the exact Q2 2026 transcript PDF on s203.q4cdn.com. The reviewed registry permits only that document, with the API URL and response SHA256 as provenance. ReportDate June30 is the quarter end; the transcript heading establishes the call date July30. Neither a webcast nor prepared remarks is treated as a full transcript.

The original PDF contains24 pages and57647 normalized characters, including operator introduction and analyst questions. Reddit uses an operator transition without the generic question-and-answer heading recognized by the existing parser. An optional reviewed qa_pattern now requires that exact transition when supplied. Missing company, fiscal period, publication date or Q&A holds the document. Existing registry entries keep their existing parser behavior.

## Evidence and validation

Original PDF SHA256 b817c1508a458fa7228ea0fb5cdb089d32b5987bd21aa4769b4221a5725c49c6; normalized text SHA256 7ca7064388f562f21dc51ba04c48b4fc8f7ca705b5bc9a285fa81f64db8c01b3. Official API response SHA256 d28b8820b24e50aba02f3cf8e3312d0cf728ca2e5cf972c2689e7488c1b9378a. Captured originals remain in local issuer-candidates-oct10 artifacts.

96 registry/PDF/transcript/direct-source tests and28 operational/alert-parity checks pass on Python3.14.2. Production is Python3.12.15. Actual captured-PDF replay parses the full Q&A and verifies API provenance, one canonical document, identical whole-state repeat, and a harmless transport revision without duplicate research. Zero model/source requests, emails or production writes. Whole-state SHA256 e93c1489e48a88f1f153a3d060623b3559843479a720984bd917d93a68fe0af3; artifact issuer-reddit-provenance-replay-oct10.json.

Deployment and bounded scheduled collection remain pending. This adds an eighth reviewed issuer, potentially the sixth covered ticker in the captured22-ticker active workload. Future-quarter automatic discovery is not implemented. Existing historical publication guards remain; public transcripts stay FMP pending broader coverage and consumer gates. Hourly collection remains at minute13, at most two reviewed documents per run and daily recheck per document. No subscription, public provider activation or FMP shutdown.
