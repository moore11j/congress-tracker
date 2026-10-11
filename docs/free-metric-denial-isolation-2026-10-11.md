# Per-symbol denial isolation for free financial metrics

October11 UTC / October10 local,2026. Public core fundamentals remain on FMP; the independent free preparation job is enabled with at most eight requests per batch and the shared Finnhub budget.

Read-only receipt metrics-live-1791682936.json at01:42:14UTC shows exactly53 of1751 metric scopes attempted. Each captured01:31,01:36 and01:41 scheduled run plans eight but attempts only AAPL.TO, receives access_denied and stops without recording a per-symbol retry timestamp. The same earliest symbol therefore blocks the whole universe indefinitely. Eight source hashes match; this is a deployed behavior, not a hypothetical capacity estimate.

Treat access_denied as an explicit unavailable scope, retain its normal24-hour attempt timestamp and continue the remainder of the already bounded batch. Never substitute the U.S. AAPL security for AAPL.TO or claim the denied data is available. Shared quota/cooldown/budget failure and authentication failure still stop; missing_api_key and shared_budget_required explicitly stop without deferring untouched scopes. The prior implementation also incorrectly deferred all symbols for a missing key; its new regression fails before the repair.

No rate increase, new schedule, provider endpoint, paid subscription, public source selection or canonical record change. The eight-request cap, elapsed metric budget, request limiter, cached-observation timestamps, source provenance and separate delivery lane remain unchanged.

Two regressions are reproduced before repair: a denied first symbol starves the next one, and missing credentials incorrectly defer both symbols. After repair89 focused metrics/preparation/shared-budget/news tests pass on Python3.14.2. The first combined test command named two nonexistent test files and ran no tests; the corrected explicit command is the89-pass result. Release and ordinary scheduled production progression remain required before calling the deployed behavior repaired. The broader institutional cohort replay remains running independently; this change does not alter its source interpreter or publication code.
