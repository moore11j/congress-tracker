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
