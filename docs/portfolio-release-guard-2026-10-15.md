# Portfolio refresh and backend release coordination

## Confirmed interruption

On October15, Scheduled Ingest38685216877/job116258395722 started portfolio-simulation-refresh at05:09:05UTC during backend deployment38685205187 (05:08:49–05:10:03). The scheduler recorded exit-15 at05:09:42. The CI diagnostic tool confirms that the transport retry retained the same request identity and reported failure correctly.

Minimized read-only production receipt artifacts/portfolio-failure-category-1792046017.json records one terminated request, zero application/deadline/yield failures,38 attempted of1135 planned scopes and zero recorded failed scopes. No active or continuing request remained. These are counts/booleans only; no checkpoint payloads or member/provider records were exported. The separate previously rejected payload exports remain unrun.

One fresh normal workflow38693528033 was dispatched at06:37:33UTC through the existing serialized queue. No failed receipt was altered or uncertain checkpoint resumed. Backend deployments are held while this cycle remains live. At06:43UTC it has341 attempted scopes and zero scope failures; this is progress, not completion. Public Feed samples0.162/0.185seconds and Insights0.343/1.963seconds return200 without server errors. These samples do not establish sustained capacity. The normal price-repair path can still use FMP.

## Local reservation primitives

The scheduler adds one durable release_hold row and two owner-checked CLI commands, reserve-release and release-reservation. A reservation is refused if a portfolio cycle is active or waiting to continue, or if the legacy all job is active. A recognized portfolio checkpoint also blocks release when its command is no longer in the manifest. Claiming a job and reserving a release use the same SQLite BEGIN IMMEDIATE serialization, closing the check-then-start race.

During a reservation, dedicated portfolio and legacy all jobs retain their pending timestamps but cannot start. Other data collection and delivery jobs remain eligible. An owner cannot remove another release's reservation. Same-owner transport retries are idempotent and do not extend its expiry. Reservations are bounded60–900seconds, default600; abandoned holds expire without clearing or falsifying a job receipt. The CLI requires the active combined scheduler machine and an existing queue. No network/provider calls or product-data writes occur in these primitives.

Initial Linux Python3.12.3 validation:54 tests pass, one Supercronic-dependent check skipped. Tests cover real SQLite races, active and continuing portfolios, legacy all, unknown portfolio checkpoint, restart persistence, expiry, rejected owner/duration, unchanged pending times and unaffected news/delivery claims, plus existing scheduler/continuation/forked-worker checks. Final12 release-guard tests pass, including the added subprocess CLI ownership/refusal/release check (55 distinct passing checks across these runs). No full-suite claim.

## Staged release requirement

This first package installs the scheduler primitives only. It does not yet change the deployment workflow and is not an automatic production safeguard until that workflow is wired and verified. Bootstrap it only after the current portfolio run is authoritatively terminal and the queue recheck is safe. Do not deploy into the running validation.

The follow-up deployment workflow should build before reserving, reserve against the installed scheduler, refuse deployment on a busy cycle or failed reservation, bound the deployment operation below the reservation duration and release in an always-run cleanup step. A failed or timed-out GitHub caller is not proof that a durable portfolio cycle has stopped. Verify the actual queue before any deployment. Do not mark an unfinished request successful or delete its checkpoint.

FMP remains enabled. Full financial/source coverage, institutional activation, transcript/analyst/valuation gaps, all scheduled consumers, sustained performance, final Massive validation and zero-FMP/billing verification remain gates. No schedule, source selector, hardware, subscription, customer email, video or landing-page change.

## Full portfolio cycle and scheduler release verified,14:32UTC

Fresh aggregate receipt artifacts/portfolio-failure-category-1792074563.json confirms request38693528033-1 succeeded:1135/1135 attempted, zero failures,227 completed for each365/30/90/180/1095-day window, no active/continuing state. The GitHub caller ended at07:07:53 after its30-minute observation limit; the durable cycle subsequently completed without being restarted. This is full-plan execution evidence using the existing source inputs, not proof of zero-FMP traffic or complete price coverage. The initial interrupted receipt remains failed and separate. A delayed local observation timed out; it was not treated as terminal evidence or used to restart the job.

Fresh queue receipt1792074599 has zero pending, active or continuing jobs. Feed0.425s/Insights0.516s are200/no exception before deployment. [PR669](https://github.com/moore11j/congress-tracker/pull/669) merged885e896fde206280e6cbcb774aab17359ec2374a, deployed through38752047285/job116480669124 successfully. Runtime Python3.12.15 verifies four exact hashes and isolated real queue reserve/refusal/news/delivery/restart/expiry/release behavior with network blocked. Production schema is present with zero active holds; no production queue mutation or payload export in verification. Receipt: artifacts/portfolio-release-guard-runtime-1792074762.json.

PR669 secret scan and preview pass; unchanged Pillow advisories and frontend five vulnerability groups (four high/one critical) remain. No dependency files changed. The attachment tool reached its100-item limit; the PR link remains usable.

## Workflow wiring prepared after installed-runtime verification

The follow-up shell builds/pushes a unique image before acquiring a900-second scheduler reservation. GitHub run/attempt/SHA identities are validated. A missing/failed/uncertain reservation prevents deployment. Actual deployment uses the built image and is bounded to330seconds plus15seconds termination grace, with180-second machine and120-second release-command waits. Successful rollout releases only its own reservation. Failed/uncertain rollout leaves the bounded hold to expire, because stopping the local client may not stop an in-flight Fly action. Other data and delivery commands remain eligible throughout.

The operational workflow now calls this shell locally; release is pending. Nineteen integrated Python3.12.3 checks pass, including seven actual Bash executions against a fake Fly client for success, build/reservation/deployment/status/cleanup failures and invalid identity. Bash syntax passes. Tests verify command order, exact image/owner reuse, no deployment after reserve failure, no early cleanup after a failed rollout, and unrelated queue behavior. This is not yet a live end-to-end guarded deployment receipt.

The initial bootstrap prerequisite is now met. Retain scheduler reservation support for rollback, and use the guarded workflow for subsequent releases. Direct manual Fly deployment bypasses the workflow and requires equivalent coordination. Full FMP replacement remains incomplete; no provider selectors or subscription state changed.

## Guarded workflow deployed and verified,14:37UTC

[PR670](https://github.com/moore11j/congress-tracker/pull/670), source70bd70013b65a3c8b815956ad275971afb4f343a, merged/deployed2543d4105483c9af6f01fb61446c4d358b58c292. Workflow38752813421/job116483332793 succeeds. Required CI-tool logs verify image build completed14:36:11, reservation ready/existingfalse14:36:16, rollout of the exact SHA/run-labelled image, owner cleanup released14:36:58, and readiness/Premium privacy checks passed14:36:59. The new guarded workflow performed this release.

Deployed Python3.12.15 verifies five exact source hashes, isolated queue refusal/restart/expiry/news/delivery behavior and zero residual production release holds. Receipt artifacts/portfolio-release-guard-runtime-1792075072.json. The fresh portfolio receipt still reports succeeded1135/1135,227 in each of five lookbacks, zero failed scopes and no active/continuing state after deployment (portfolio-failure-category-1792075076.json). The original failed receipt remains unchanged.

Public readiness200/databaseokay, Feed0.418s and Insights0.321s/no exception are immediate spot checks (fusb-public-1792075077.json), not sustained-capacity proof. PR670 secret scan passes; exact CI logs retain unchanged Pillow advisories and frontend five vulnerability groups (four high/one critical), no dependency-manifest edits. Both PR attachments hit the100-item limit; ordinary links remain available.

All newly started local test/watch/verifier handles are terminal. No source selector, calendar, hardware, subscription, customer email, video or landing-page change. This closes the diagnosed deployment-overlap repair and demonstrates one full portfolio refresh on existing inputs. Repeated ordinary operation, full financial/price/source quality, remaining consumers and every wider FMP retirement gate remain open. CAT PR656 remains unmerged/unreleased at a31dbb1d with275 affected checks and complete current-policy original replay. Original CAT/institutional payload-export approvals remain pending and those commands were not retried.
