# Replacement-news response completeness, October 11, 2026

Read-only production audit take-response-audit-1791752372.json shows39 of40 recent Walnut Take requests consumed exactly their output-token limit. Corresponding scheduled logs repeatedly report unparseable/missing items despite HTTP200. Eight-article batches have720output tokens, but output IDs previously repeated provider URLs up to500characters each. The single-article256-token request used141tokens. This is observed truncation evidence; request success alone is not semantic success.

## Change

Use short request-local string IDs in the model input/output, then map validated results back to unchanged durable article/cache IDs. Article URLs, provider summaries, thumbnails, source timestamps, output order, cache reuse and the public payload contract stay intact. Reordered batches are accepted; unknown, duplicate or missing IDs, empty takes/invalid biases and explicit incomplete Responses statuses fail closed to the existing clearly labelled fallback. No retry loop, extra model, larger token budget or new schedule is introduced. Successful complete batches log item/token counts without content or credentials.

Usage accounting now records zero web-search calls because this request supplies no search tool. This corrects internal accounting; it is not a claim about a provider invoice. Existing generated-cache prompt versions remain reusable because the editorial task has not changed.

[Official OpenAI documentation](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses) documents incomplete max-output-token responses and checking completion before consuming JSON. Used the OpenAI Docs skill; no new API-key acquisition tool is available or needed for mocked tests, and live credentials remain server-side.

## Validation

Python3.14.2:34 selected tests pass across the new response-ID checks, insights snapshots, free research consumers and Finnhub news rollout. Tests cover long source URLs, reversed eleven-article/two-batch mapping, exact provenance-field retention, cache reuse without a new request, and malformed/incomplete/duplicate/missing-ID rejection. Eight-article budget remains720tokens and the three-article remainder270. Representative mocked output JSON is less than one-third its former size, not a measured live token-saving claim. Initial test invocation used the repository root instead of backend and collected no tests; corrected invocation passes. git diff --check passes.

Release and actual model/consumer verification are pending. Senate scheduled recovery/access, institutional growth/capacity, core/transcript coverage, all alerts/rankings and final Massive/globalFMP/billing remain open. No frontend or landing-page change.
