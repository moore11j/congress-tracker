# Automatic institutional prior collection

October 8, 2026 Pacific. Continues the authorized FMP replacement after [the deployed evidence package](direct-13f-worker-2026-10-08.md#approved-release-and-live-staging).

## Source-bound unattended work

The existing bounded collector now supports automatic discovery from a filing-date boundary. It selects at most twenty parsed originals from a rolling maximum 93-day window, with a 20,000-candidate safety bound. Processing receipts are stored under `_prior_collection` within staging reconciliation metadata, preserving the existing canonical comparison. Each receipt binds current source/metadata hashes, prior source/metadata hashes and the actual attempt time. It never advances the original source's checked time or changes original-byte revisions.

New/changed sources precede retries. Successful source/prior pairs cool down for seven days while the linked prior remains parsed and unchanged; held attempts retry after 24 hours. Interrupted or unprocessed sources remain eligible. Invalidated current/prior evidence is reconsidered immediately. Prior states are fetched in one bounded query rather than a query per filing. HTTP denial/cooldown stops a batch; the shared collector lock and existing database-pressure guard remain in force.

The proposed cron entry is every five minutes at minutes 3 through 58, up to twenty filings each time, from October 7. This is a staging schedule only. The global shadow-mode switch still controls collection; institutional publication and source ownership remain separate and off/default FMP. `--preview` cannot request transport or write a receipt. No new table or dependency is required.

A live PostgreSQL **read-only** preview using the new selection query finds **160** eligible sources and returns the first twenty. No code was installed by that preview and it made no database writes. Local combined validation: **93 passing tests**, including the prior, identifier, immutable evidence, publisher, worker, batch and existing schedule suites, Python 3.14.2. The exact release checkout also passes all **93** checks. Report links and diff checks pass.

## Earlier mapping baseline repaired locally

A database-enforced read-only capture retrieves the actual REAX mapping dated August 14, 2023 and its real parent filing; no mapping date is invented or backdated. The 1,512-byte supplement binds to the original baseline hash and has SHA-256 `8ea84c961029d368704686cf4063410adf63f5c264e5e17bb0f32b1dd7670121`. The rehearsal loader rejects conflicting existing rows/parents and accepts only source-bound read-only supplements.

The 22-source pair replay now completes seven current comparisons, with three mapping waits and two Steginsky value holds. It still inserts 20 filings/937 positions, now derives 138 changes/summaries and ten activity records. There are still **zero qualifying public feed events**, so real institutional no-send alert coverage remains open. All state is identical on repeat: `4a4359a5c076da239e373b0a105ab6b035c255633a48161b8e93a401dc157e3f`; first/repeat took 52.61 seconds locally. No production canonical correction/publication or customer email.

Next: review/release the bounded schedule, observe its actual execution and source-repeat behavior, resolve remaining date/class mapping holds and obtain broader qualifying-event coverage before institutional source activation. Prices/fundamentals/content and whole-product alerts remain separate FMP shutdown gates.
