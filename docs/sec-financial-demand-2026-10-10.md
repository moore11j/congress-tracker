# SEC financial-panel demand, October 10

PR546/a4989c00 deployed successfully with six exact source/proof hashes. The public XOM panel correctly queued a cache miss, but one explicit invocation of the existing general enrichment worker completed120 jobs (118successful,2failed) without reaching it. XOM remained queued with0attempts. The dedicated SEC warmer previously considered only watched, recently viewed and default symbols, so a new requested stock could wait behind unrelated general enrichment.

The warmer now reads up to1000 due queued ticker_financials symbols, bulk-loads their source caches and applies the same freshness/version rules as the public reader. Up to ten of the unchanged20batch slots prioritize cache misses from those requests. Watched/default symbols keep rotation slots, and unused slots can serve further demand. Fresh cached requests, future retries and unrelated job types do not consume demand priority. Failed source scopes rotate by attempt time; shared cooldown and collector exclusion remain. Existing queue identities/status/attempts are unchanged; a later general job simply reads the prepared cache.

The public endpoint still performs no SEC source requests. Existing five-minute schedule and single data lane are unchanged; no paid-provider dependency or new recurring schedule is added. Receipts expose requested scope/missing-cache counts.

## Connection and repeat validation

An isolated PostgreSQL16.15 one-slot test initially caught the shared collector retaining an idle acquisition transaction across source reads. Its session advisory lock now commits the acquisition transaction before yielding; the session lock survives, and explicit unlock/connection close remains. The legacy orphan-recovery criteria are unchanged. A second connection still cannot acquire the active collector lock.

The final actual-source replay prepares XOM from a queued request with no watched/default stock, restores6annual/12quarterly rows, then repeats without source reads. Across the statement and job paths, nine overlapping statement refreshes are refused. No pooled slots or idle transactions remain during source I/O; failed source requests preserve cache and release locks. Queue identity is unchanged. Disposable schema removed and local server stopped; no production writes, external requests or emails in the replay.

Initial34financial/demand checks pass on Python3.14.2; final expanded financial, metadata, earnings, institutional-prior/reference and collector-schedule group passes106checks (overlapping the initial group). No full-suite claim. Local evidence: financial-demand-postgres-result.json and original XOM manifests.

Release and ordinary scheduled demand/public-reader verification remain pending. CoreFMP, institutional/transcript gates, remaining source coverage, consumer windows and finalMassive remain unchanged.


## Approved release

PR547 merged as a365d3c666fd44fe9a67327d194da05ac9af7e54; workflow38092703846 dispatched. Secret scan passes; unchanged dependency audits still fail in Next.js-related packages and Pillow10.4.0. No admin bypass or full-CI-pass claim. Runtime and ordinary scheduled demand verification remain pending until the workflow completes.


## Final deployed and scheduled verification,22:49 UTC

PR546/a4989c00 and follow-up PR547/a365d3c6 are deployed; workflows38091993340/38092703846 succeed. Eight exact source/job/proof hashes match. The ordinary22:49 SEC run now picks up XOM's queued demand, prepares6annual/12quarterly rows and30.553bn trailing free cash flow, with zero canonical writes/emails in the receipt. The original queue identity remains queued for its later general-worker cache hit, without duplicate source publication.

Backend and app.walnutmarkets.com each return the same complete retained response subset twice: source sec_edgar,6annual/12quarterly, expected cash flow, fingerprint0949ce79b222afee724f4135be5f0ac4d25d1ecbc4a9770fc1ff10a1563ad055. Four200responses take0.456–0.540seconds. Unsupported forecast, market-ratio and other financial fields remain explicit; partial status does not claim universal metric coverage. Receipts continuity-runtime-1791672563.json and continuity-public-final-1791672583.json preserve evidence.

Final two-service receipt1791672533 verifies imagea365d3c6/seven operational hashes,1556MB available and empty queues before the due run. Feed0.55–0.79s, profile0.48s and new AAPL/NVDA contexts1.62/1.02s, all200. No further capacity or provider selection change. XOM's separate current-ratio preparation cache has not yet rotated; core remains defaultFMP. NEE direct-filing fallback, other source/consumer coverage and finalMassive remain open. No new subscriptions, customer test email or FMP cancellation.
