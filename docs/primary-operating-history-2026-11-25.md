# Source-qualified operating history

November 25, 2026. Isolated candidate based on deployed851ba270. Not deployed; public core fundamentals remain FMP.

DMLP and FIP publish complete reconciled operating-cost statements, but their standard company facts omit OperatingIncomeLoss. The existing operating-history reader requires gross profit and operating expenses, so it cannot use these statements. The primary-subtotal qualifier was included in PR838/46f86b54 as offline code but had no reader integration; the still-open PR835 is not evidence that this module is absent from current main.

This candidate connects that complete-original qualifier to the existing bounded acquisition and financial reader. It applies only when gross profit is absent and no reported operating income or conflict overlaps the current/prior comparison window. Older reported operating history is preserved. Current and prior trailing revenue inputs must align exactly with reported CostsAndExpenses, including currency, filing, accession and period. Every selected original must then independently reconcile revenue, its exhaustive operating-cost inventory, and its income or pretax/nonoperating bridge. The entire series is held on missing or conflicting source evidence. No reported operating series or conflict is overwritten, and no ticker whitelist or vendor amount is used.

The planner selects at most three originals within the existing six-original acquisition budget. It ignores irrelevant filings older than four years while retaining strict identity checks for every in-scope filing. This avoids DMLP's unrelated2002-2007text filings blocking current recovery. Current unsupported filings, foreign reporting changes, stale periods and incomplete comparisons still hold.

Derived records retain source URL, checksum, filing date, accession, unit, period and complete reconciliation. The public metric source label identifies the calculated subtotal. Cache readers validate the stored proof, and alert identity includes the new measurement basis so source recovery or rollback does not create a company-change alert. Financial-sector scoring methodology, provider selection and recorded history remain unchanged.

## Complete-source checks

Six existing complete SEC filing fixtures and two hash-bound public company directories cover both issuers. Their source acquisition uses three unique originals per issuer. Current/prior operating TTM values are DMLP103,781,000/55,403,000USD and FIP-22,871,000/-71,147,000USD. All are derived from the reconciled originals; they are not directly reported annual figures.

The initial18source/guard checks pass. The complete23-test suite passes on Windows in434.49seconds, including ordinary acquisition, full projection and repeat, all previously available calculation values, unchanged ROE/leverage, source labels, malformed cached proof, score consumer, alert recovery/repeat/crossing/rollback, and zero provider calls/customer deliveries/events. Two operating-margin fields recover for each issuer in these retained-source scenarios. FIP's separate temporary-equity provider discrepancy remains held.

Existing-source regression checks and exact Linux validation remain pending. Local runtime is not a production-capacity qualification. Current production acquisition, scheduled cache rotation and broad universe/consumer acceptance must be verified before counting these as live coverage. This is not full financial coverage or completion of the FMP migration.

Evidence: artifacts/primary-operating-ttm-feasibility-nov25.json, primary-history-consumers-nov25.xml; complete fixture sources are listed in backend/tests/fixtures/sec_primary_operating_subtotal/sources.json.


## Full upstream payload qualification, November 25

Four fresh public SEC captures at17:30UTC provide complete company directories and companyfacts for DMLP and FIP. This exposed an omission in the earlier test inputs assembled from six complete filings: DMLP retains37reported operating observations ending in2016. The earlier all-history guard prevented its current fallback, so the earlier251/2055checks did not prove current DMLP acquisition. Those results remain evidence for the previous head only.

The revised planner rejects any reported observation or conflict that overlaps the entire current/prior comparison window, including a quarter that is not an exact trailing-input period. It preserves every older reported point and derives only the wholly missing comparison series. Existing projected comparison observations also prevent application. Full SEC payloads replace reconstructed facts in the candidate's acquisition, projection, cache, source-label, score and alert tests. No production behavior has changed; revised exact-head validation is pending.

The revised27-case suite passes locally in391.92seconds with complete upstream payloads, six complete originals, all37old DMLP operating points preserved, overlap/conflict rejection and point-in-time acquisition. Cache/source/score/alert recovery/repeat/crossing/rollback checks pass. The earlier future-date test was corrected because the full payload legitimately supplies older eligible filings; a separate point-in-time test proves no future filing enters the plan. Revised Linux CI remains pending.
