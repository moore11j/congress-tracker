# Consistent institutional source copies across queued pieces

November 22, 2026 UTC, November 21 Mountain time. Production remains `7ec3491c`; this repair is not deployed or activated.

The approved single same-image restart completed successfully, preserving the image/configuration fingerprint, thirteen installed hashes, source settings and readiness. Its release reservation was released. All eleven subsequent readiness observations passed over five minutes, with a minimum1,355.11MiB available against the unchanged1,100MiB admission threshold. This is temporary recovery, not a permanent memory-retention fix.

The approved copy window began at04:05:22UTC with a fixed05:05:22UTC deadline, at most128requests,20,000records and4GBoriginals. A missing temporary directory after restart stopped inventory setup before any output/request. Read-only reconciliation proved that no plan or worker attempt existed. The operator repaired directory creation and retained the original deadline; no worker was retried.

The fresh plan contained17,452records/3,586,013,717original bytes in91pieces. Three queued captures succeeded and transferred. Capture run8, the fourth request, stopped with a ValueError before exporting a document. Only a20-byte incomplete gzip header exists for that request; it was never transferred or accepted. No retry, replacement inventory, deadline renewal or source activation occurred.

## Exact consistency failure

A whole-inventory comparison found one changed document row version, ID22109, accession0001697375-26-000009. Count, content hash, parsed status, revision ID/version and measured byte lengths were unchanged. The normal prior-filing collector updated `_prior_collection.checked_at` at04:08:03.993557UTC. This processing receipt is part of the export payload, so ignoring it would weaken snapshot consistency. The strict inventory rejection was correct.

The existing collector lock protected each piece but was released between pieces. A normal collector could therefore update the archive between two queued copies. The fix retains all source, row-version, hash, CPU, memory, file-size, expiry and complete-inventory checks.

## Prepared repair

An explicit operator command can create a package-bound source-copy window lasting at most one hour. Creation and removal acquire the existing PostgreSQL collector advisory lock, so a hold cannot begin while a source writer is active. The file is created exclusively and is never renewed or overwritten; closing preserves an owned approval receipt. Missing or expired windows leave normal behavior unchanged. Invalid/future/mismatched active approvals fail closed. Installing this code creates no window or recurring schedule.

Both normal SEC13F collection and prior-filing processing consult the window inside the same collector lock. SEC13F-only collection returns an explicit hold before source/schema work. Mixed collection removes only SEC13F and continues its other sources. Quotes, alert delivery, other source collection and the data scheduler are not paused by this feature. Normal collection becomes eligible again at the absolute expiry or explicit owned close; no automatic replacement copy or extra collection request is scheduled.

New approved piece requests can name the exact source-window ID and must match its package and absolute approval times. Existing legacy requests remain supported, with their existing strict drift checks. The capture package now binds the window module, operator and two collector entry points. Publication remains disabled and there are no canonical database writes.

## Evidence and next gate

Local checks pass74capture/window tests with8Linux-only skips,108collector/window tests and9actual PostgreSQL contracts; suites overlap and these counts are not distinct. The PostgreSQL tests demonstrate active-collector exclusion, no lease renewal, unchanged inventory across held prior-processing attempts, successful source export, then restored prior processing after close. The original timestamp-change rejection remains enforced. The same suite retains the complete19.48MB source fixture and10,001-record inventory proof. The owned PostgreSQL instance stopped.

The three transferred pieces independently verify750records/699parsed originals and86,063,914source bytes. This is partial evidence only: no complete-inventory receipt or canonical/consumer acceptance exists. Local receipts are `original-copy-restart-execution-nov21.json`, `original-copy-postrestart-health-nov21.json`, `original-copy-approved-execution-nov21.json`, `original-copy-inventory-drift-nov21.json`, `original-copy-changed-document-nov21.json` and `original-copy-partial-offline-nov21.json` under `artifacts/`.

Next: exact-head CI/release review, installed-file and lifecycle checks, then a separately approved fresh copy window that includes the bounded SEC13F collection hold. It must use a new inventory and destination, archive the exact failed request as failed, keep the old incomplete files separate, and stop/release the hold on any failure or expiry. The remaining canonical/reference capture, complete current consumer/publication acceptance, ongoing preparation, FIPequity and Walnut2.0 product gates remain open.
