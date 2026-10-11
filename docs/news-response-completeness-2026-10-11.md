# Replacement-news response completeness, October 11, 2026

Read-only production audit take-response-audit-1791752372.json shows39 of40 recent Walnut Take requests consumed exactly their output-token limit. Corresponding scheduled logs repeatedly report unparseable/missing items despite HTTP200. Eight-article batches have720output tokens, but output IDs previously repeated provider URLs up to500characters each. The single-article256-token request used141tokens. This is observed truncation evidence; request success alone is not semantic success.

## Change

Use short request-local string IDs in the model input/output, then map validated results back to unchanged durable article/cache IDs. Article URLs, provider summaries, thumbnails, source timestamps, output order, cache reuse and the public payload contract stay intact. Reordered batches are accepted; unknown, duplicate or missing IDs, empty takes/invalid biases and explicit incomplete Responses statuses fail closed to the existing clearly labelled fallback. No retry loop, extra model, larger token budget or new schedule is introduced. Successful complete batches log item/token counts without content or credentials.

Usage accounting now records zero web-search calls because this request supplies no search tool. This corrects internal accounting; it is not a claim about a provider invoice. Existing generated-cache prompt versions remain reusable because the editorial task has not changed.

[Official OpenAI documentation](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses) documents incomplete max-output-token responses and checking completion before consuming JSON. Used the OpenAI Docs skill; no new API-key acquisition tool is available or needed for mocked tests, and live credentials remain server-side.

## Validation

Python3.14.2:34 selected tests pass across the new response-ID checks, insights snapshots, free research consumers and Finnhub news rollout. Tests cover long source URLs, reversed eleven-article/two-batch mapping, exact provenance-field retention, cache reuse without a new request, and malformed/incomplete/duplicate/missing-ID rejection. Eight-article budget remains720tokens and the three-article remainder270. Representative mocked output JSON is less than one-third its former size, not a measured live token-saving claim. Initial test invocation used the repository root instead of backend and collected no tests; corrected invocation passes. git diff --check passes.

Release and actual model/consumer verification are pending. Senate scheduled recovery/access, institutional growth/capacity, core/transcript coverage, all alerts/rankings and final Massive/globalFMP/billing remain open. No frontend or landing-page change.


## Deployed and live verified, October 11, 21:15 UTC

PR582 merged as0ca2260997854e0a7183eac8e2ebcef918098807. Backend deployment38175162701 succeeded. Secret scan and Vercel preview pass; existing unchanged dependency audits fail, and automated code-review quota is unavailable. The app attachment limit prevented attaching this PR; its ordinary GitHub URL remains available. No frontend change.

Receipt two-service-verify-1791753171.json verifies24 exact runtime hashes, release image, unchanged shared2CPU/2GB API and performance1CPU/2GB database, empty queues and institutional publication false. News returns50 items in0.638s, feeds0.380/0.558s, NVDA profile0.594s and fresh AAPL/NVDA contexts1.281/1.143s. These short samples do not prove sustained capacity.

One manual invocation of the existing insights refresh was enqueued through the serialized data lane, key66e87f5dda39a079c4756ff3, target run35. The receipt confirms run35 finished with exit0 and no pending/running instance. No new recurring schedule was introduced. Actual source observation stays21:04:03.603040UTC. Before:50articles,2generated takes/48fallback. After:50generated/zero fallback. All13 compared identity/provenance/content fields, original article order and50image URLs match exactly;50identity tuples are unique. Image presence is not a claim that every URL is an article-specific thumbnail.

Six real eight-article model batches complete at21:13:13-23UTC using251/264/243/254/256/259output tokens, each below the unchanged720limit. All48 previously failing articles succeed; two cached takes are reused. The mixed last40 audit still contains33 historical capped requests, not33 failures after deployment. Repeated public read returns in0.390s with cache_hit=true, stale=false and identical50articles/takes.

Artifacts: news-refresh-request-oct11.json; news-before-short-ids-oct11.json; news-after-short-ids-1791753219.json; news-short-ids-parity-oct11.json; news-short-ids-repeat-oct11.json; take-response-audit-1791753245.json. No new subscription/model/token allowance, provider selection, customer test email or FMP shutdown.

Separate reliability observation: before deployment, concurrent public member profile requests took73-76seconds with many slow database checkouts. A contemporaneous metric sample has zero five-minute CPU throttling and about76CPU-seconds reserve, so that burst is not proven to be CPU quota exhaustion. One separate request identified an offline-mirror user agent; this does not establish the origin of all profile traffic. Code inspection finds db_pool_checkout_slow measures elapsed time since request start, not acquisition wait; its count is not proof of pool contention. Member recent-trade enrichment still permits live user quote fetching, which is a candidate requiring measurement. Profile query/provider work needs focused diagnosis before claiming sustained fast service. No hardware change is justified by this observation alone.
