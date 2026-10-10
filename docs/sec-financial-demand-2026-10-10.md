# SEC financial-panel demand, October 10

PR546/a4989c00 deployed successfully with six exact source/proof hashes. The public XOM panel correctly queued a cache miss, but one explicit invocation of the existing general enrichment worker completed120 jobs (118successful,2failed) without reaching it. XOM remained queued with0attempts. The dedicated SEC warmer previously considered only watched, recently viewed and default symbols, so a new requested stock could wait behind unrelated general enrichment.

The warmer now reads up to1000 due queued ticker_financials symbols, bulk-loads their source caches and applies the same freshness/version rules as the public reader. Up to ten of the unchanged20batch slots prioritize cache misses from those requests. Watched/default symbols keep rotation slots, and unused slots can serve further demand. Fresh cached requests, future retries and unrelated job types do not consume demand priority. Failed source scopes rotate by attempt time; shared cooldown and collector exclusion remain. Existing queue identities/status/attempts are unchanged; a later general job simply reads the prepared cache.

The public endpoint still performs no SEC source requests. Existing five-minute schedule and single data lane are unchanged; no paid-provider dependency or new recurring schedule is added. Receipts expose requested scope/missing-cache counts.

## Connection and repeat validation

An isolated PostgreSQL16.15 one-slot test initially caught the shared collector retaining an idle acquisition transaction across source reads. Its session advisory lock now commits the acquisition transaction before yielding; the session lock survives, and explicit unlock/connection close remains. The legacy orphan-recovery criteria are unchanged. A second connection still cannot acquire the active collector lock.

The final actual-source replay prepares XOM from a queued request with no watched/default stock, restores6annual/12quarterly rows, then repeats without source reads. Across the statement and job paths, nine overlapping statement refreshes are refused. No pooled slots or idle transactions remain during source I/O; failed source requests preserve cache and release locks. Queue identity is unchanged. Disposable schema removed and local server stopped; no production writes, external requests or emails in the replay.

Initial34financial/demand checks pass on Python3.14.2; final expanded financial, metadata, earnings, institutional-prior/reference and collector-schedule group passes106checks (overlapping the initial group). No full-suite claim. Local evidence: financial-demand-postgres-result.json and original XOM manifests.

Release and ordinary scheduled demand/public-reader verification remain pending. CoreFMP, institutional/transcript gates, remaining source coverage, consumer windows and finalMassive remain unchanged.
