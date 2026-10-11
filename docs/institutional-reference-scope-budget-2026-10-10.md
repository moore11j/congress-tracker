# Balance the institutional reference planning window

October 10 local / October 11 UTC, 2026. Institutional publication remains disabled.

The existing reference collector interleaves current and prior quarter requests, but its planning window was filled before that interleaving. One large, recently staged prior-quarter filing could consume almost the entire 5,000-scope window. The read-only production receipt at 00:58:46 UTC contains 4,966 June scopes and only 34 September scopes. Both latest batches requested only June evidence. This limits progress without being a provider quota or server capacity problem.

The repair bounds discovery independently at 5,000 scopes per quarter, counts repeated holdings within each admitted quarter window, then interleaves ranked candidates into the same shared 5,000-scope plan. Spare capacity goes to the other quarter when one is smaller. Intermediate counters are bounded at 10,000 keys; the source scan remains capped at 500 documents and keeps its existing 15-minute cache. Scope iteration is deterministic. Versioned plans invalidate the old unfair cached selection on the next ordinary run.

Daily attempt timestamps now survive temporary exclusion from a changed bounded window and expire after a day. This prevents a scope returning to the window from bypassing its retry limit. Actual requests remain capped at two per run under the existing schedule, collector lock, background pressure guard and serialized data lane. No new service, subscription, schedule, price request, publication or email behavior is introduced. Fair selection does not prove complete coverage or eliminate truncation.

## Evidence and checks

- Three regression assertions fail on the original implementation: starvation with either quarter visited first and loss of an excluded scope's daily attempt timestamp.
- Python 3.14.2: 57 focused reference, warming, publication-batch and schedule checks pass after the repair. Two additional cached-plan/spare-capacity checks pass in the final 13-test warming run, for 59 distinct passing checks. No full-suite pass is claimed.
- Source-only export at 00:57:39 UTC captures 107 already-staged needed references, with zero source requests or production writes. Merging with previous captured evidence produces 109 distinct query scopes. The diagnostic reduces unresolved CUSIPs from 188 to 185 and finds five comparisons with no remaining mapping failures, versus one earlier. This alone does not authorize publication; four newly cleared comparisons are undergoing the existing isolated publisher and consumer replay.
- Earlier Dimensional action documents still do not establish executed share-conversion ratios. Those security transitions and the MBGL distribution treatment remain held.

Deployment, exact runtime hash, ordinary selected-quarter receipts and the four-comparison replay remain pending at preparation time. Public institutions remain FMP. The two-service hosting configuration and final Massive subscription sequence are unchanged.
