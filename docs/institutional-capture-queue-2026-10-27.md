# Queued institutional validation capture, October 27, 2026

## Purpose and scope

The current institutional validation cohort exceeds the old capture's document, raw-byte and compressed-file limits. Its one-time idle preflight also leaves a race with the next scheduled import. This package prepares a fixed manual capture command in the existing data queue, protected against deployment interruption. It does not publish institutions, select a replacement provider, schedule recurring work or transfer a production payload.

The earlier automatic approval review requires explicit owner approval of the provider-derived production snapshot and its local OneDrive destination. That approval remains pending. Installing the command alone cannot execute a capture: it additionally requires a short-lived approval file bound to the exact installed package. No such approval file has been created.

## Execution controls

- Fixed manual key: `maintenance-institutional-capture-v2`; no calendar trigger. Data imports serialize with it, while the existing delivery lane remains available.
- The child verifies the designated host, job key, queue path, token, cycle and process group against the actual sole live data-lane claim. It checks ownership during export, rather than relying on environment variables or one idle preflight.
- The deployment reservation and capture claim share the existing SQLite transaction. Exactly one can win. Active capture blocks deployment; an existing deployment reservation defers capture without deleting it.
- The read-only PostgreSQL transaction uses a repeatable snapshot, three-second statement timeout and sixty-second idle-in-transaction timeout. A terminal transport receipt is produced only after that transaction closes.
- The job yields CPU at nice19 and is bounded by 85 CPU seconds, a 90-second hard CPU limit, 480 wall seconds, one billion bytes of virtual address space and at least 750 MiB available system memory. These limits fail the diagnostic rather than increase resources or return an incomplete baseline.
- Limits remain 6,000 documents, 750 MB original bytes, 2 GB record payloads, 250 MB compressed output and 600 MB initial free disk. Individual records and canonical table counts are bounded. The complete original-source receiver is required after transfer; a transport checksum alone is insufficient.
- Files use exclusive request IDs. A failed or completed request cannot silently overwrite or restart the same capture. A completion manifest appears only after payload closure and flush; interrupted files remain incomplete. Transfer verification checks size and SHA256 before original-source verification.

The local PostgreSQL comparison retains the original CIK normalization semantics, complete source rows, canonical positions, activity, events and original-period summary evidence. It is detailed in [the capacity report](institutional-capture-capacity-2026-10-27.md). An older synthetic 6,000-source benchmark excludes current production database query cost; it does not prove the full current capture fits its CPU budget.

## Approval and operating procedure

After review, deployment, installed-package verification and explicit owner approval, an operator may create `/data/institutional-capture-request.json` with version1, a unique 32-character lowercase hexadecimal request ID, the installed `package_hash()`, numeric approval/expiry timestamps no more than one hour apart, payload `institutional_originals_and_canonical_reference_rows` and destination `owner_local_onedrive_migration_validation`. These identifiers bind the approved scope; they are not credentials. The exact request must be reviewed before creation. No automatic enablement or recurring schedule is part of this release.

Enqueue only that fixed manual key. Observe the same queue cycle, live process identity and eventual exit code; an observation timeout is not permission to restart it. Require a terminal successful job and matching completed manifest before transferring the exact request's file. The intended local destination is the owner's existing `C:/Users/Owner/OneDrive/Documents/congress-tracker/artifacts/` migration evidence directory, with a unique capture subdirectory. A new snapshot remains unaccepted until its bounded receiver reparses every original, verifies complete manager/CUSIP/period query scope and preserves canonical rows for the full cutover/consumer replay.

Cleanup must identify only this request's files, occur after its process is terminal, and preserve the accepted local evidence. Never remove another capture or restart an ambiguous live job. No current production capture, transfer, cleanup or approval-file creation has occurred.

## Validation state

Local implementation is on `codex/institutional-capture-queue`, based on deployed2db40965. Initial Windows checks pass101 with seven platform skips. Initial Linux queue/release/capture regression passes114 with one unavailable-Supercronic skip. New coverage exercises a real child process under the actual queue claim, refusal after ownership loss, missing-approval rejection before database imports, simultaneous release/claim races, complete-file receipt and transfer corruption.

Review and the first full application/PostgreSQL pipeline exposed a timestamp serialization defect in the new file manifest. The failed run removed its owned schema and stopped its local server; it produced no accepted capture. The correction uses the existing canonical JSON encoding and returns the persisted representation. Final focused Linux and full PostgreSQL/application replay results are pending at this checkpoint. No review, deployment or production acceptance is claimed yet.

CoreFMP remains selected and institutional publication remains disabled. This package supplies validation evidence; all publication, consumer, source-coverage, sustained-capacity, finalMassive and globalFMP/billing gates remain separate.

## Final local verification, October27 07:29UTC

Final Linux3.12.3 regression passes117checks with one unavailable-Supercronic skip in87.70seconds. This includes two actual child processes, approval rejection before ORM imports, loss of the real SQLite claim during export, five simultaneous capture/release races, unchanged scheduled-job behavior and the actual guarded-release shell. The real-timestamp regression now passes. No full-suite pass is claimed.

The complete application pipeline on local PostgreSQL16.15 passes. It uses the actual `export_snapshot`, bounded gzip writer, completed manifest, transfer verification and full original-scope receiver. All769documents,718parsed originals and complete canonical row hashes match the old exporter and its repeat, including two matching synthetic events and the legacy extra-zero CIK. The payload contains84,279,374original bytes and30,053,915compressed bytes; producer CPU13.828125seconds. The reference/candidate/repeat query comparison uses138queries each and22.375/12.0/12.96875CPU seconds. This retained fixture does not establish current production cohort capacity. Receipt: `artifacts/capture-oct27/application-postgres-capture-oct27.json` in the primary checkout.

The local temporary files and owned schema were removed and PostgreSQL stopped. A separate liveness check reached the local port only after that normal cleanup and received connection refused; the original process handle then returned terminalsuccess. No restart occurred. No production reads/writes, external provider requests or emails were used by this replay. Application/source hashes remained fixed during the corrected final runs. Review and installed verification remain next; no approval file, enqueue, production capture or transfer has occurred.
