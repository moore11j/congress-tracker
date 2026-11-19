# Exact ticker search before unrelated prefix matches

The November19 production directory contains ANT, but the search index has no ANT entity. Searching ANT returned ANTX prefix matches before the exact prepared-directory fallback could run. Searching Anthropic did reach ANT. This is a shared ordering defect for verified listings missing from the index.

The candidate moves the index-symbol check into the unverified fallback. Existing exact ticker results still return immediately. Otherwise the existing selected-source identity resolver can prepend a verified exact ticker, with normal limits and duplicate handling. If no verified identity exists, unrelated prefix results cannot invent a requested ticker or enqueue its enrichment. Existing symbol-related fallback behavior is preserved. There is no hardcoded ANT mapping, index mutation, provider change, or external data request in this repair.

Validation: four regression cases fail on the prior code and pass after the repair. All52 local search/universal-search/SEC-metadata tests pass. Cases include lowercase and share-class aliases, one-result limits, cached repeats, no provider/enrichment requests, missing/stale/wrong-source/absent identities, and a real search index built before a fresh directory listing (including null exchange) becomes available. Existing insider, member, company-name and typo searches retain their tests. Linux CI is configured in Search Regressions; final CI/release review remains pending.

This does not repair quote access: the November19 read-only incident probes found FMP429 and Massive snapshot403. PR830 is separately deployed1deb7327 and its approved finite institutional preparation is active. No search deployment is authorized by the prior PR830-specific approval.
