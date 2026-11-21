# Bounded copies of the complete institutional source archive

November 21, 2026. Prepared on application baseline `46f86b547f7eee4559199863700edf2f94ff725f`. Not deployed or activated by this document.

The current SEC13F archive contains 17,452 records and 3,586,013,717 original bytes. The deployed whole-export tool allows 8,500 records and one billion original bytes. Preparation is complete for all 16,675 parsed originals, but a larger source copy is needed for current full consumer validation. The other records are 775 quarantines and two failed downloads; both missing originals belong to failed records.

This change adds an explicitly approved, source-only piece to the existing manually queued capture worker. It leaves legacy whole exports and their limits intact. It adds no recurring schedule, public reader, canonical write or provider switch.

## Request and execution contract

- The existing capture job, exclusive data lane, designated-machine check, background/collector guards, CPU85/90-second limit, 480-second wall limit, one-GB address-space limit and 750-MiB available-memory stop remain in force.
- A version2 request names `institutional_originals_segment`, the existing owner-local OneDrive destination, the exact installed package hash, a unique request ID, a one-hour approval window, and one piece of a frozen inventory. Legacy requests remain version1 and keep their old4-KB request limit. Version2 requests have a64-KB limit.
- An inventory may contain at most20,000records and4,000,000,000original bytes. Each of at most128pieces contains at most250records and64MB of complete uncompressed transport, including its terminal receipt. Compressed files also have a64-MB bound. Plans reserve space for escaped metadata and scope receipts; a larger final encoding fails closed.
- Before reading any body, every piece recomputes the whole inventory fingerprint in its read-only repeatable-read transaction. Document/revision row versions, content hashes, identities, statuses and byte lengths must match the frozen plan. An unrelated newly added or changed record invalidates the plan too. Inventory age may not exceed one hour at admission. Source bytes are then fetched one record at a time and independently hashed.
- Failed or interrupted pieces remain incomplete and cannot be accepted or automatically overwritten. No retries, deadline renewals or unbounded capture loops are installed by this release.

The current sizing-only estimate is79pieces for raw source plus metadata. It is not the final capture plan: framing, escaped metadata and scope receipt reserves can increase the count. A fresh complete manifest determines the actual count within the128-piece bound. A changed inventory requires reconciliation rather than silently combining different snapshots.

## Local acceptance

`backend/scripts/receive_institutional_originals.py` accepts a complete plan, one unique transferred cycle per piece, and a new local destination. It forces a local SQLite configuration and disables network connections. It verifies each file checksum, stream receipt, planned identity and original hash; independently reparses all successful originals; and checks complete inventory closure before publishing the final source receipt. Missing, duplicated, reordered, changed or truncated pieces are rejected. Partial output remains visibly incomplete.

The receipt and filenames explicitly mean **originals only**. The ordinary canonical-baseline receiver rejects this new payload. The new receiver rejects canonical records inside a source piece. It derives the complete manager/CUSIP/period scope from the originals for a separately bounded canonical/peer reference capture. No source-only receipt can complete consumer or publication acceptance.

## Validation and release requirements

The local regression suite covers bounds beyond the old document/byte cap, missing failed originals, source and inventory drift, versioned approvals, incomplete/tampered transfers, full-inventory assembly and legacy capture compatibility. Actual PostgreSQL contracts exercise row-version binding, changes outside the selected piece, a10001-record inventory, a saved-hash/source mismatch, and the complete retained19-MB JPMorgan original through export and independent receiver parsing. Linux CI also exercises both request versions in the real serialized child process and rejects lost ownership.

`backend/scripts/verify_original_segments_fixture.py` provides the installed-code proof with the same complete original, a disabled database/network environment and bounded Linux CPU/memory/wall limits. Exact installed-file and package hashes must pass before any capture request. Installing this support does not authorize exporting data.

The proposed destination is a new owned cycle under `C:/Users/Owner/OneDrive/Documents/congress-tracker/artifacts/institutional-current-nov21`. Any actual copy needs explicit owner approval of the public source/provenance payload, destination, source/record/piece bounds and time window. Account, watchlist, billing, credential and delivery records are excluded. Canonical/peer rows, publication rehearsal, ongoing preparation, SEC ranking effects and source activation retain their separate gates.
