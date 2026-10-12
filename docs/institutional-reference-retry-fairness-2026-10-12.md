# Institutional reference retry fairness, October 12, 2026

## Reproduced coverage failure

The reference warmer removed attempt timestamps after 24 hours and then ranked eligible scopes solely by quarter and filing frequency. When a backlog exceeds the daily request budget, frequently held unavailable identities can repeatedly outrank unattempted identities. Three new regressions fail against release16cc3282: a four-day, eight-request scenario visits only two unique scopes, expired failed scopes precede unseen work, and history disappears across source windows. This is a reproduced scheduling failure, not a claim that every live scope has already starved.

## Repair

Keep valid attempt history for both current comparison quarters. Continue excluding attempts less than24hours old. Within each quarter, unattempted scopes lead, then the oldest expired attempts, with filing frequency and CUSIP as deterministic tie-breakers. Existing interleaving preserves one slot per quarter when both have work; either quarter can use spare capacity. Verified identities are removed on cached or full planning, and obsolete quarters are discarded. Plan version6 rebuilds the bounded candidate window without resetting actual attempt timestamps.

The two-request batch size,500-source scan,5000-scope admission cap,15-minute cached-plan lifetime, source-window cursor,60-second cooldown, provider-denial cooldown, actual collection times and publication gates remain unchanged. This fixes ordering within admitted windows; it does not claim unlimited source coverage or change the existing bounded admission rules. Older history already discarded by the previous release cannot be reconstructed by this repair. No new provider request budget or paid subscription.

## Validation

87 focused tests pass on Python3.14.2 across warming, lightweight preflight, reference identity and reference-backed publication. The multi-day unavailable case now reaches all eight scopes before retrying the oldest pair, with unchanged two-per-run limits and zero public events/emails. Cached and fresh windows, old-quarter cleanup, completed-identity cleanup, quarter balance, daily cooldown, provider refusal and publication guards pass. An initial test append used the wrong relative path and changed no file; the corrected regressions reproduce three failures. An incomplete string replacement initially omitted history assignment and caused five local failures; the completed edit passes all87 checks.

A guarded repeatable-read/read-only production comparison at03:25:09 uses the actual377-scope cached window and811saved attempt entries. Both versions correctly select no scopes at the current time because all377 are deferred. At the explicit simulated two-day expiry, the old planner discards all history and leads with000000000 for September; the candidate retains811entries and leads withN0280G100, the older attempted scope. June selection remains30233Q108. This is planner evidence, not an actual future collection or proof that the selected identity is available. No provider calls, database writes or emails occur. Receipt reference-fairness-1791775509.json; candidate source SHA256 c49d151af817cd56123a5d2474e0459ca5e30b9c6ccbd0a4e851b0b8d7ec3e2e. Timing order warms the first database read, so these numbers are not a performance-improvement benchmark.

## Release state and remaining work

Implementation is local on codex/reference-retry-fairness; production remains16cc3282. Review/deployment and an ordinary version6 collection receipt remain pending. Institutional publication staysfalse and FMP remains selected. The full647-reference institutional replay87489 is still live in first pass; do not restart for silence. Its publisher code is unchanged by this planner-only repair. Complete final repeat/consumer checks, actual writer capacity, remaining source/financial/transcript coverage, all affected scheduled consumers, final Massive personal Stocks and zero-FMP/billing cancellation remain required.

A single fresh identified request for OCC final notice59843 at03:20 returned403HTML, not a PDF. No other OCC request was attempted after denial. Existing Dimensional continuity holds remain intact. A wildcard-path search and one guessed manifest path failed read-only; the actual registry was then inspected. No transition registry or evidence timestamp was changed.
