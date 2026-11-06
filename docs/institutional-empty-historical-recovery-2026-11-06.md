# Empty historical institutional filing recovery, November 6, 2026

The completed6,472-source replay found39 existing-filing source conflicts. Three are untouched historical filing shells: canonical IDs1018,1021 and1094 have complete SEC originals but no stored positions, position changes or activity for their holder/quarter. They contain45,846 and199 holdings respectively. The prior15-filing partial review did not include these cases.

The candidate shared publication fix fills only an exact historical13F-HR shell with matching CIK, accession, filing date, quarter and period end. It requires no processed timestamp, amendment, supersession, competing holder/quarter parent, Walnut ownership/repair metadata, positions, changes, activity or matching holder-quarter feed events. Partial filings and current-boundary filings remain held. Whole-source hash, cover totals, value consistency, security class and semantic guards still run first.

Recovery retains the existing filing ID, original metadata, filing date and creation timestamp, records complete source provenance, and follows the historical_no_alerts path. A later current quarter must pass the ordinary verified-prior rules. Repeating recovery inserts nothing and leaves the full relevant canonical state identical. This does not implement broad existing-position correction or rebaseline cached classifications.

Validation:89 focused checks passed on local Python3.14 across publication, worker, priors, reference mapping and the new recovery tests. One initial positive-event fixture used an increase below feed eligibility; it was corrected to exercise qualifying new and exited positions without changing product rules. Exact three-original rehearsal used the immutable complete canonical peer fixture,926 point-in-time mapping witnesses and the same hash-verified complete SEC comparison cohort as the full replay. All1,090 holdings recovered, zero new filing IDs/changes/activity/feed events, and exact full-state repeats passed. No source was recollected and no production data changed. The original full replay and1,970-event consumer checks predate this candidate and are not represented as validation of an already deployed release.

Evidence: `artifacts/institutional-current-nov1/empty-historical-originals-nov6.json`, `existing-conflicts-inspection-nov6.json`, and `existing-conflict-inputs-nov6.jsonl.gz`. The three-original receipt follows.

```json
{
  "status": "three_complete_originals_recovered_in_memory",
  "source_rows": 1090,
  "filings": 3,
  "canonical_filing_ids_preserved": true,
  "complete_peer_evidence_sha256": "56bb3409dea884d19ec2fb3f8cf37dd0abcca0f46075d91ee8164ba1a50fa0bc",
  "mapping_rows": 926,
  "results": [
    {
      "filing_id": 1018,
      "accession": "0001172661-26-003258",
      "source_sha256": "e0e17da67bf29d7cfe4f19be7ed9746e2626f2311e671e3e5c806e0c17dbf0a6",
      "first": {
        "status": "projected",
        "inserted_filings": 0,
        "inserted_positions": 45,
        "enriched_positions": 0,
        "derived_state": "historical_no_alerts",
        "changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "production_writes": 0
      },
      "repeat": {
        "status": "existing",
        "inserted_filings": 0,
        "inserted_positions": 0,
        "enriched_positions": 0,
        "derived_state": "historical_no_alerts",
        "changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "production_writes": 0
      },
      "full_state_repeat_equal": true
    },
    {
      "filing_id": 1021,
      "accession": "0001921196-26-000004",
      "source_sha256": "b84f174741902af8c08549521b654a486a9fc52c513dea9079e0f7bcb5ef2c18",
      "first": {
        "status": "projected",
        "inserted_filings": 0,
        "inserted_positions": 846,
        "enriched_positions": 0,
        "derived_state": "historical_no_alerts",
        "changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "production_writes": 0
      },
      "repeat": {
        "status": "existing",
        "inserted_filings": 0,
        "inserted_positions": 0,
        "enriched_positions": 0,
        "derived_state": "historical_no_alerts",
        "changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "production_writes": 0
      },
      "full_state_repeat_equal": true
    },
    {
      "filing_id": 1094,
      "accession": "0001567890-26-000003",
      "source_sha256": "e86fe51a3681858dba05380ab19312b7d21943187edfcdfad028fb4e0ad18455",
      "first": {
        "status": "projected",
        "inserted_filings": 0,
        "inserted_positions": 199,
        "enriched_positions": 0,
        "derived_state": "historical_no_alerts",
        "changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "production_writes": 0
      },
      "repeat": {
        "status": "existing",
        "inserted_filings": 0,
        "inserted_positions": 0,
        "enriched_positions": 0,
        "derived_state": "historical_no_alerts",
        "changes": 0,
        "summaries": 0,
        "activity_events": 0,
        "feed_events": 0,
        "production_writes": 0
      },
      "full_state_repeat_equal": true
    }
  ],
  "elapsed_seconds": 30.73950230004266,
  "production_reads": 0,
  "production_writes": 0,
  "emails": 0,
  "full_cutover_ready": false
}
```

Release status: local candidate; Linux3.12 CI and review pending. No production activation, deployment, subscription, email or FMP switch. Full institutional reconciliation, peer-aware summary/alert rebaseline, broader financial/capacity/Massive/zero-FMP/billing gates remain.
