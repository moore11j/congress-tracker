# Share verified institutional parses within one batch

October 11 UTC / October 10 local, 2026. Preparation follow-up only; institutional publication remains disabled.

The complete-quarter selector parsed each peer original to identify its quarter, then Prepared13FEvidence parsed the same original again. The two-quarter synthetic batch regression observed six calls through those entry points for two originals. A bounded batch-local cache now binds each parse to its verified SHA256 and full source identity and shares immutable JSON strings, tuples and Decimal values between grouping and preparation. The repaired regression observes two calls. Independent target validation, source checks on reuse, fiscal/as-of comparisons, complete-source bounds and invalid-source holds remain. No global cache or persisted prepared data is added; the cache dies with the batch.

The cache enforces the existing complete-scan ceilings of10,000 originals/100MB. Individual evidence-object limits remain500 peers and100MB including identifiers. Source status, dates, request budgets, selection, public events and scheduled commands do not change. PR567's separate per-quarter budget correction is not included in this branch.

77 focused evidence/publication/worker/batch checks pass on Python3.14.2. Tests preserve original hashes, identity drift rejection, exact boundary refusal, immutable results, malformed-source holds, event receipts and repeat safety. The first revised regression incorrectly expected four calls; after inspecting the actual entry-point scope, its original two-call requirement is retained, and the implementation passes it.

Actual718-original baseline cost on local Python3.14.2: load/classification39.641CPU seconds plus prior preparation23.734/current22.547,85.922total. PeakRSS674.28MB includes loading the199MB saved snapshot and is not production batch RSS. Saved-source comparison of shared preparation against independently rebuilt records and actual PostgreSQL complete-reader/value-hold replay are running; no improvement or new parity result is claimed until those complete. No live provider requests, production writes, emails, infrastructure change or activation.
