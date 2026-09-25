# Product and growth release — September 25, 2026

The owner approved committing and deploying the remaining signup, reporting, Premium access, upgrade presentation, and video-worker changes described in the September 20–21 implementation reports.

## Release contents

- Email/password signup without mandatory names or billing addresses; existing password, verification, checkout and rate-limit protections remain. Generic registrations open a noindex welcome page, while explicit return destinations are preserved.
- Growth reporting separates page views, product actions, known accounts, browser sessions, and verified live-payment evidence. Internal/test exclusions are enabled by default.
- Research Memory and prepared Company Developments require Premium or higher, enforced on the server and reflected in frontend access states. Public research briefs remain public.
- Premium presentation gates for outcome charts and consistent ticker upgrade prompts.
- An isolated continuous video worker, bounded safe-stage recovery, and more reliable browser capture targets.
- Historical growth/SEO notes and clearly labeled static ticker design concepts. The concept images are reference artifacts, not new application screens.

## Validation

- Backend focused suites: **108 passed** across signup/reporting, Premium access, Research Memory, evidence matching/locks, video automation and scheduler configuration.
- Frontend focused suites: **47 passed** across signup interactions, upgrade prompts, outcome charts, ticker layouts and research access.
- Full frontend production build: **passed**, including lint/type checks. The temporary build-directory entry added by Next.js was removed from tsconfig afterward.
- Backend tests used an in-memory SQLite database and the existing Python 3.14 test environment. Production uses the pinned Python 3.12 Docker image. Historical reports retain the previously documented unrelated full-suite failures; this release does not claim the entire test suite is green.

## Rollout

Backend commit: `dc9c8f74`. [Backend deployment workflow](https://github.com/moore11j/congress-tracker/actions/runs/36184539098).

Backend deployment succeeded. Production returned healthy database readiness, HTTP 401 for anonymous Research Memory, Company Developments and admin reporting requests, and an OpenAPI registration schema requiring only email and password. Frontend release follows this verified backend deployment.

Deploy and verify the backend before pushing the frontend, because the previous registration API required the fields removed from the new form. Check production database readiness, anonymous denial of private research/admin routes, and the registration OpenAPI schema before proceeding. Verify frontend deployment and the live signup/welcome pages afterward.

No real account or payment is created by these release checks. Authenticated account reporting, paid-user journeys, actual email delivery, and video-provider output are not established by anonymous smoke checks.

Local build/test logs and production checks are under `artifacts/release-sept25-*` and are intentionally untracked.
