# Free institutional identifier preparation

October 9 Pacific / October 10 UTC, 2026. This package prepares dated CUSIP-to-ticker evidence through the existing free Massive reference endpoint. It does not select Massive prices, purchase a subscription, enable institutional publication or claim that a reference retrieved today was observed at the historical filing date.

## Evidence and scope

The official [All Tickers documentation](https://massive.com/docs/rest/stocks/tickers/all-tickers) supports a CUSIP filter and historical date on Stocks Basic. Actual bounded calls at 05:33 UTC resolve 30609A109 to OKE, 84615Q103 to SPCX and 60744M106 to MBGL at September 30, 2026. Each returns one active US/USD common-share identity with a share-class FIGI. These three successes do not establish complete identifier coverage.

The adapter binds the requested CUSIP and period, safe source URL, canonical parsed response hash and actual retrieval timestamp. It never stores credentials or pagination URLs, never backdates availability, rejects ambiguous/unsupported identities and refuses to override an exclusive conflicting candidate. Stored evidence has immutable revisions; unchanged facts retain their first observation. The representation is canonical parsed JSON, not original response bytes.

An opt-in job stages at most two references per run, with a persistent 60-second minimum interval and five-minute cooldown after provider refusal. It shares the existing collector lock and background-pressure guard. It releases database transactions before HTTP, caches its work plan for fifteen minutes, defers unsuccessful identities for a day and retains explicit bounded coverage. Only the two latest completed quarters, at most 500 documents and 5,000 scopes are considered. It is disabled by default and requires shadow mode. It does not update canonical holdings, events, scores or email deliveries.

A read-only production benchmark initially hit the twenty-second timeout when aggregating all historical mappings. Restricting that lookup to the two relevant reporting periods completed the actual plan in 11.53 seconds at 05:48 UTC. It hit the 5,000-scope cap, so coverage is explicitly truncated. The highest priorities include CUSIP 30233Q108 in 110 filings and ONEOK's new CUSIP in 28. The benchmark used an enforced read-only transaction and made zero provider requests.

The institutional projector also holds a same-symbol CUSIP transition instead of treating it as a sale and new purchase. ONEOK's actual SEC reorganization evidence demonstrates why this is necessary. A verified ticker alone does not establish corporate-action continuity. Integration into publication, source-bound transitions and broader replay remain separate work.

## Validation and rollout

Sixty-six focused adapter, staging, warming, publication and worker checks pass on Python 3.14.2. Five warming checks pass again after bounding the historical mapping query. Coverage includes checksum/query mismatch, observation timing, ambiguous references, unchanged-revision repeat, changed-fact revisions, two-request quota bounds, persistent refusal recovery, source-status/date filters, plan refresh and zero public records/emails. Existing institutional publication remains disabled. No full-suite claim.

This report initially describes local preparation. Deployment, runtime verification, actual durable staging and scheduled observation must be recorded below before claiming them complete. Institutional publication and global FMP shutdown remain gated; Massive paid prices remain the final subscription step.
