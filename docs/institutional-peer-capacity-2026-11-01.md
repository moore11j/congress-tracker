# Measured canonical peer capture capacity

November 1, 2026. PR761 is deployed as `9aafde080c88bc0290660c71dc900ee976499f95` through guarded workflow41354191439. Nine installed hashes, absent approval request, absent recurring trigger and unchanged source flags are verified. No peer capture or transfer has run.

A single read-only aggregate probe evaluated the exact proposed scope from all6,472 retained originals:40 current/following periods and54,918 mapped symbol-period pairs. It returned counts only; no production record payload, canonical write, provider request or email. Scope identifiers/hash were uploaded to a temporary capacity-check file, separate from the command's approval and scope paths.

| Table | Complete selected rows | Existing cap | Result |
|---|---:|---:|---|
| Positions | 865,427 | 1,000,000 | Fits |
| Changes | 498,634 | 1,000,000 | Fits |
| Activity | 268,491 | 250,000 | Does not fit |
| Summaries | 35,342 | 150,000 | Fits |

The probe completed in23.121wall/3.146processCPU seconds, with each SQL statement capped at3seconds and locks at500ms. All40 periods were counted for each table. Receipt `artifacts/institutional-current-nov1/full-peer-scope-capacity-1793570110.json`. These counts are current sizing evidence, not proof of full export speed, byte size or downstream correctness.

The follow-up changes only the activity row cap to300,000 and tests the exact boundary plus rejection of the first excess row. It preserves one million positions/changes, all other table limits, two billion uncompressed bytes,250MBcompressed,85CPU/480wall seconds,1GBprocess address space,750MiBavailable-memory and600MBdisk guards. It does not truncate, filter away rows, increase host resources or authorize collection. If any remaining bound is exceeded, capture remains incomplete and cannot pass the receiver.

After the correction's checks/deployment/installed verification, separate explicit approval is still needed for one bounded canonical-peer capture and transfer to the existing OneDrive evidence folder. No additional source-original collection is required. Snapshot-time overlap conflicts, full peer recomputation and no-false-alert consumer checks remain separate gates. Original live isolated replay continues untouched; institutions publicationfalse and core/transcripts/pricesFMP remain.

Public HTML samples after PR761 returnedHTTP200 without the previously reported server-error marker: Feed0.726s, Insights0.449s, AAPL0.705s and NVDA0.532s. These are bounded public responses, not authenticated consumer or sustained-capacity proof. Receipt `peer-public-pages-1793570127.json`.
