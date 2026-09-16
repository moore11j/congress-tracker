# Approved bullish forward study and email follow-up

User approved September 15, 2026: carry out the proposed internal forward comparison and requested an email result. The connected Gmail profile is moore11j@gmail.com, but automatic approval review rejected the email-enabled schedule because the user has not explicitly confirmed this destination and the internal-research payload. **Email sending is disabled pending that confirmation.** No email has been sent at setup time. This schedule may perform the local research and produce a local report only.

## Current state and scope

Active thread heartbeat: `walnut-bullish-forward-test`, daily at 4:30 PM in the user's local time zone (America/Los_Angeles). Created September 15, 2026 with email explicitly disabled. Its scope includes completing setup and verifying the first real capture; scheduling alone is not evidence that predictions have already been collected.

The expanded options backtest is complete and negative. It is not the forward test. At scheduling time there is no verified running prospective collector or frozen prospective prediction cohort. The scheduled task must establish and verify collection; it must not merely wait for a report that nobody is producing.

### Verified implementation — September 15 first scheduled run

The collector and evaluator now exist. Read `docs/confirmation-forward-study-protocol-2026-09-15.md` and run the command below; do not rebuild or retune the frozen model. Nine focused tests passed. The first successful read-only capture inventoried 4,299 existing confirmation anchors from 33,749 source snapshots, 61,030 insider display events, 60,655 normalized transactions, and 95,083 cached price rows. These existing anchors establish the baseline and enroll **zero** prospective decisions. First captured inputs are in `raw/20260915T234145Z.json`, with receipt `captures/2026-09-15.json`. The latest captured confirmation was calculated September 15 at 19:58 UTC. A failed initial date-parameter export is retained separately; active code/protocol checksums are in `frozen-plan-v2.json`.

For each daily run, execute from the repository root:

```powershell
$env:PYTHONTZPATH="$PWD/backend/.venv/Lib/site-packages/tzdata/zoneinfo"
C:/Python314/python.exe backend/scripts/research/forward_bullish_study.py --capture --evaluate
```

The transport is a reviewed read-only Fly database export. Verify the new capture receipt and evaluation, including `missing_capture_days`, `latest_confirmation_at`, and any pending price coverage. Repeated same-day capture is intentionally a no-op. Keep the frozen plan, first baseline, every capture, predictions and measured research labels intact. Investigate operational failures without loosening the hypothesis, enrolling old events, or modifying earlier predictions. If upstream confirmations become stale for multiple trading sessions, notify the user rather than silently claiming useful new predictions are arriving.

The first capture was also checked as a price-only coverage supplement for the frozen options predictions. It repaired zero missing measurements; the original model, results and predictions were preserved. The separate artifact is `supplements/options-price-coverage-015c328e7ac3.json`. Do not repeat historical model searches on routine runs. Future coverage supplements must use the same frozen predictions and preserve previously measured results.

Work in `C:/Users/Owner/OneDrive/Documents/congress-tracker`. Keep all work in local research scripts and artifacts. Production access is read-only. Do not change public confirmation scoring, bearish scoring, historical entries, measured outcomes, public event continuity, or application caches. Do not deploy, purchase data, upgrade subscriptions, or activate a public options-flow feature.

## Fixed initial study window

Begin setup and collection on September 15, 2026. Enroll prospective decisions actually recorded through October 15, 2026, using real capture times. Never backdate missed captures or reconstruct a supposedly prospective prediction from later data. If setup is delayed, disclose the shorter enrollment period rather than inventing prior predictions.

Use 30D as the primary horizon and 7D as secondary. Make the first complete assessment on November 17, 2026, after the enrollment window's next-session entries should have reached 30 calendar days and a trading-session close. If required prices or predictions are missing, report the missing coverage and an inconclusive result on that date; do not wait indefinitely or extend the study until a favorable percentage appears. If verified scheduling/market dates require a later maturity, state the exact remaining measurements and complete once those are due.

## Work to perform on scheduled runs

1. Inspect current application snapshot storage and existing research scripts. Verify what is already preserved for each original confirmation. Use reviewed read-only exports and existing/free data access. Complete the local prospective collector and evaluator, with focused tests, and freeze the protocol and code before recording experimental predictions.
2. Use one internal bullish challenger based on the already selected unusual-insider-purchase hypothesis: at least twice the same buyer's median purchase size, at least three prior known-dollar purchase filing groups in 1,095 days, and positive fully known 90-day net insider buying. Reuse the existing duplicate, security-type, and suspect-value rules in the September 14 disclosure experiment. Do not sweep thresholds or fit to the new forward outcomes. Missing conviction inputs mean unknown, with original-behavior fallback; report their coverage separately. Keep bearish behavior unchanged.
3. Capture original confirmation inputs and the experimental decision together before a common future research entry. Include scores, direction, source contributions, disclosure provenance, available market/sector inputs, capture timestamps, and code/data hashes. Use only data actually available at capture time. Record a separate next-session research entry for both alternatives where the public event's original entry has already passed; never substitute that entry into the public ledger. Freeze one primary research decision per new confirmation event and report ticker/date concentration and a first-per-ticker sensitivity. Do not retrospectively enroll existing calls as if predicted earlier.
4. Store daily snapshots and predictions append-only under `frontend/test-results/confirmation-research/forward-bullish-2026-09-15/`. A repeat run must reuse completed captures rather than duplicate or overwrite them. Record missed capture dates and collection failures explicitly. On days without a new trading decision, maintain status without manufacturing observations. Verify the first successful capture instead of assuming collection is active.
5. Repair research price coverage where possible using existing cached or free attributable data, preserving the earlier frozen options study and public ledger. Any supplemented historical evaluation must retain original predictions and rules and be clearly labeled as a coverage supplement. Keep it separate from prospective results.
6. Measure both alternatives using the same entry, horizon, price-basis rules, and existing direction/excess grading. Freeze each measured research result with its source evidence. Report accuracy, counts, retained coverage, raw and SPY-relative returns, missing/immature results, date and ticker concentration, and uncertainty. Do not label 75% validated based on a narrow or small selected subset. Compare against the unchanged baseline and disclose if the sample is inadequate.

## Email and stopping rule

After the final assessment is written, prepare the result in a local report and notify the user in this task. Do not send email, create a remote email draft, or otherwise transmit the report while destination/payload approval is pending. Intended destination, subject, and content for the pending user confirmation are: moore11j@gmail.com; `Walnut: 30D bullish forward-test results`; study dates, baseline and challenger 30D/7D accuracy and sample counts, retained coverage, raw and SPY-relative returns, missing-data limits, and the recommendation. The old options report is not the forward-test result.

Only after the user explicitly confirms the destination and payload may this plan and the automation be updated to permit delivery. At present, completion means saving the local assessment, reporting completion in this task, and pausing the heartbeat. Never claim an email was scheduled for delivery or sent while it remains disabled. Surface a collection block or outstanding delivery approval in this task.

Stay quiet during routine unchanged runs. Notify in this task only for a meaningful setup failure, missed-collection problem requiring attention, completion, or another required user action. The user requested a result email, not daily email updates. Local scheduled work requires the computer to be on, the desktop app running, and this workspace available.
