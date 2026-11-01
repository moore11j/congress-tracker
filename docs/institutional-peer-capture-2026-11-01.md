# Institutional canonical peer validation package

Prepared November 1, 2026. Status: local implementation and validation; no additional production capture, transfer or publication is authorized or executed.

## Problem and scope

The accepted source snapshot `d3fbcad8a206404bbf261f81044276c2` preserves complete originals and canonical rows for source holders. Its earliest CUSIP mapping samples do not include all other holders needed to validate global institutional summaries and downstream alerts. The original snapshot and live isolated replay remain useful and unchanged; neither proves complete global peer coverage.

The proposed supplement reuses all 6,472 verified parsed originals. Its canonical scope contains 39 source report periods and 54,327 CUSIP/period pairs, bound to the accepted baseline SHA256 `e9488c89f9438f904a1cb76a3652990e4fe3c0f57da3cfba9e0bc8015f9a74ca`. Exact scope file: 1,816,732 bytes, SHA256 `94eb0e5f13edb84db40b36fbb6e313202d04aefc20da844e3488413e0420ed62`. This scope is proposed, not approved.

For each source CUSIP, capture includes every existing symbol mapping, including ambiguous and historical candidates. It includes every holder's complete position/change/activity/summary rows for those symbols in the report and following quarters, complete parent filing/public holder tables, and every institutional canonical event including orphaned identities. This is conservative validation scope, not authority to resolve a ticker or publish a filing. No customer account, watchlist, delivery-state or credential tables are exported.

## Implementation and bounds

`institutional_peer_capture.py` validates the exact scope and current source inventory, exports complete rows, and receives into a spool with schema, source binding, identity, scope, size, checksum and terminal checks. It does not apply records. Changed/missing source identities, cap overflows, truncation and duplicate rows fail acceptance.

`capture_institutional_peers.py` adds a manual-only data-lane command, protected against deployment overlap. It requires a separate, short-lived owner approval request bound to the installed package and exact scope. No request or recurring trigger is installed by the code. Existing imports serialize with it; the delivery lane remains independent.

The command uses PostgreSQL repeatable-read/read-only, three-second statement and 500ms lock limits, a 60-second idle transaction timeout, 480-second wall/85-second CPU budget, 1GB process address-space limit, 750MiB available-memory guard, 600MB disk headroom and 250MB compressed-file cap. Record caps are 5,000 filings, 5,000 holders, one million positions, one million changes, 250,000 activities, 150,000 summaries and 100,000 events, with two billion uncompressed bytes maximum. Exceeding a bound leaves an incomplete file, never an accepted truncated capture. No retries silently overwrite a prior cycle.

The terminal receipt is written only after the read-only transaction closes. File acceptance still requires the independent peer receiver; the shared file manifest's historical original-scope label is transport-only, not peer or cutover acceptance.

## Existing evidence and limits

The 21:36 UTC read-only production aggregate probe counted 1,686 filings, 1,839,902 total positions, 1,076,747 total changes, 128,127 summaries and 868 holders. These are entire-table counts, not the proposed selected scope. The activity count hit its three-second timeout; the institutional event figure of 44,070 is a planner estimate, not a count. No row payload left production in those probes. Complete selected-scope sizing and production capacity remain unverified.

An offline rehearsal on a separate copy of the immutable approved baseline validates the entire proposed scope and every selected retained row: 436 parent filings, 70 holders, 32,568 positions, 21,443 changes, 2,448 activities, 35,338 summaries and four events. Independent row comparisons and a second receiver pass are identical. The original baseline is unchanged. Output is 169,222,040 uncompressed bytes and 28,194,154 compressed bytes. Combined local export, independent comparisons and two receiver passes used 108.42 CPU seconds/113.72 wall seconds; that is not a production export timing or capacity claim.

Receipts live in `artifacts/institutional-current-nov1`: `peer-capture-local-proof.json`, `peer-capture-proposed-scope.json`, `global-peer-capacity-1793569019.json`, plus the immutable original capture and `repair-baseline-receipt.json`. No missing peers are invented locally. Original capture permission is consumed and is not reused for this supplement.

Initial local suites: 101 transport/file checks passed; 75 peer/job/existing job checks passed with eight Linux-only skips. The final local suite passed 382 checks with 13 Linux-only skips in 92.95 seconds. Linux process checks remain pending.

## Remaining acceptance

Finish exact-release review and Linux checks, verify selected-scope capacity, then obtain explicit additional snapshot/OneDrive-transfer approval. A later supplement has its own capture time: compare overlapping records and retain conflicts, never silently merge it into the original snapshot as though captured atomically. Full chronological replay/repeat, complete peer summaries, canonical-event preservation, no-false-alert/rebaseline checks and all retained consumers remain gates before publication. Core fundamentals, transcripts and prices remain FMP; institutional publication remains disabled. Pending Fly app-wide secret-version synchronization is a separate unexecuted approval request.
