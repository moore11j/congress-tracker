# Historical institutional activity rebaseline, November 6, 2026

Status: tested release candidate. No production correction or publication activation.

A historical amount correction can cross a feed threshold without changing an activity's direction or score. A generic refresh can also overwrite an existing event's date with the latest summary date, while alert SQL and Python readers use payload filing dates ahead of event timestamps. These paths must not present historical repairs as new disclosures.

The new explicit historical rebaseline refreshes complete existing summaries and current activity, updates only already existing legacy feed events, preserves event IDs, timestamps, payload date presence/values, source URLs and source-availability evidence, and never inserts a feed event or sends an alert. Current activity dates that differ remain separately labeled. Published SEC evidence stays immutable, including when the derived current activity is withdrawn. Normal publication remains unchanged. Unchanged summary repeats preserve their generation timestamps. The helper has no schedule, endpoint, source switch or production apply entry point.

Validation:

- 185 local checks passed across historical repair, direct publication, empty filing recovery, worker, reference mapping, institutional activity and monitoring. One pre-existing institutional-profile holder-name expectation was excluded after its failure was reproduced using unchanged HEAD code. The 16 final focused checks pass after the generation-time repair. These groups overlap; they are not additive.
- The first full-data pass exposed ten legacy payload-date changes. Both Python and SQL date regressions were added. A later repeat exposed summary generation-time churn; unchanged historical summary generation times now remain stable.
- The completed disposable SQLite rehearsal starts from the immutable complete canonical peer union, loads 1,686 filings, 75,827 scoped positions, 21,904 changes, 822 summaries, 9,096 activities and 1,428 feed events. All 36 complete source originals are reparsed and checked against the source-bound proposal before 1,196 position corrections and 921 derived changes are applied locally.
- Actual summary and activity code reprocesses all 822 scopes. All 36 filings reconcile completely afterward. The 7,770 activity candidates create zero new activity IDs in this captured cohort, including the 40 selection additions identified in the prior pure projection, which already have canonical activity records.
- 727 existing feed payloads update; all 1,428 event IDs, dates, source identities/URLs and effective alert dates remain identical. All 1,428 synthetic baseline alert copies and delivery/read state remain unchanged, and rerunning alert creation adds zero. No customer account or email was used.
- Repeating the whole summary/activity refresh yields equal fingerprints for filings, positions, changes, summaries, activities and feed events. Snapshot fingerprints exclude managed created_at/updated_at audit columns; generated_at and event date fields are included. No stored fixture or production data is mutated.

Source baseline SHA256: `2a367b72b5513db5350489d61c43993e1c9c8541fe3db1625d74ccec09438efb`.
Source proposal SHA256: `ad8583fd003793c5aeb0ad9bda3cdde491b4b36d387c3286123492afbd872a73`.
Local receipt: `artifacts/institutional-current-nov1/full-historical-rebaseline-nov6.json`.

The prior payload-only diagnostic first counted timestamp string differences as changes. Its v2 separates 574 timezone-serialization differences from eight actual legacy timestamp differences. The complete rebaseline additionally caught ten payload effective-date differences; the passing final run preserves both stored and effective dates. No earlier failed rehearsal is accepted.

Remaining: production-bound whole-source correction planning, locking, receipt/rollback and exact apply/repeat checks; PostgreSQL validation; institutional activation and scheduled coverage; financial/content/capacity/all-consumer checks; Massive last, zero-FMP and billing. FMP remains selected for unaccepted capabilities.
