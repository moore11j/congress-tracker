# Runtime dependency security refresh, October 15, 2026

Status: deployed and runtime verified through PR671/bbbc72c3; see the release receipt below. Preparation evidence follows. Based on backend main 2543d410. This removes recurring dependency audit failures from the migration release path; it does not activate a provider or complete FMP retirement.

## Change

Pillow 10.4.0 becomes 12.3.0. Next.js remains on the 15.x line, from 15.5.18 to 15.5.28. The lockfile updates Sharp to 0.35.5, Nano ID to 3.3.20, source-map-js to 1.2.2 and their required runtime packages. A scoped `next.postcss` override selects 8.5.30 because Next 15.5.28 still pins 8.4.31; the root PostCSS also resolves 8.5.30. React and product application code are unchanged. A targeted Git attribute preserves LF for the existing guarded backend deployment shell.

Official sources: [Pillow 12.3 security fixes](https://pillow.readthedocs.io/en/stable/releasenotes/12.3.0.html), [Pillow 12 compatibility changes](https://pillow.readthedocs.io/en/stable/releasenotes/12.0.0.html), [Next.js 15.5.28 security release](https://github.com/vercel/next.js/releases/tag/v15.5.28). Existing uses do not call the removed Pillow helper APIs inspected in this upgrade.

## Local verification

The targeted LF checkout change also passes all19 guarded deployment and portfolio reservation checks.

- Linux Python 3.12.3: 209 existing disclosure, quarantine, duplicate, preparation, image/layout and marketing checks pass, one pre-existing Starlette test-client deprecation warning. Video generation/publishing remains disabled; tests are isolated.
- Seven retained public House originals yield identical extraction method, page count, text SHA256 and error state with Pillow 10.4.0 and 12.3.0. Five use pypdf; two invoke real Poppler 24.02 and Tesseract 5.3.4 OCR across five pages. No new source requests or production writes. Fixture originals retain their hashes.
- Initial uncontrolled OCR comparison timed out on 9116361 under 12.3.0 at the existing 20-second page timeout. The completed comparison uses one OCR thread for both versions and the unchanged production timeout. This proves controlled compatibility, not production OCR capacity; no timeout or worker configuration changed.
- Completed isolated production requirements install: all 43 resolved packages audited with zero known vulnerabilities. The first audit overlapped installation and saw only ten packages; it is superseded by the completed audit. Local `pip-audit -r` could not bootstrap a temporary venv because local ensurepip is absent; the full installed-path audit is local evidence and exact CI requirements audit remains required.
- Frontend production audit: zero known vulnerabilities. Development-only audit findings remain outside this production dependency repair.
- Node 24.12.0: production build compiles, type-checks and finishes page generation successfully. Backend was deliberately unavailable at loopback; no production data was fetched for the build. CI uses Node 22 and remains separate evidence.
- Frontend suite: 726 pass / 62 fail. An archived clean main source tree under the same installed runtime reproduces exactly the same 62 failing cases, with zero new or resolved failures. This comparison identifies existing source assertions; it is not a clean full-suite pass or an old-versus-new dependency runtime comparison.
- Local built Insights, ticker and pricing routes return 200, CSP and loadable compiled CSS; actual image optimization returns a 64-pixel WebP through Sharp 0.35.5. These checks are narrower than full page success. Ticker final headers are private/no-store and do not include Cookie in Vary; public cache partitioning is not claimed.

## Reliability finding to repair separately

Fault injection with the backend unavailable reveals ticker React Server Component errors despite HTTP 200. `frontend/app/ticker/[symbol]/page.tsx` has an existing `loadPublicTickerSnapshot` catch that rethrows non-404 failures, then awaits it in metadata. Logs include error digests 1548337582 and 746358424. A literal HTML application-error check initially missed the streamed failure; the saved receipt is corrected and must not be described as full ticker success. Repair and regression-test this path while preserving truthful metadata and indexing policy. This is a verified code/fault-injection finding, not proof that it caused the earlier production Insights incident.

## Evidence and remaining work

Local artifacts: pillow123-regression-oct15.log, house-pillow-parity-oct15.json, backend-security-audit-oct15.json, frontend-security-final-audit-oct15.json, frontend-security-tests-oct15.log, frontend-security-baseline-tests-oct15.log, frontend-security-baseline-comparison-oct15.json, frontend-security-build-oct15.log and next-security-runtime-oct15.json. Artifacts may be local-only; this report preserves consequential results.

The preparation-time release gates below are superseded by the deployed receipt. Broader migration gates remain. Core financial coverage, institutions, transcripts, all scheduled alerts/rankings/consumers, sustained performance, final Massive activation, zero-FMP traffic and billing remain open. CAT PR656 is unmerged/unreleased; rejected production-export checks remain unrun and pending explicit approval. No secrets, account records or production snapshots were read for this work.


## Deployed release and runtime verification, October15 15:49UTC

[PR671](https://github.com/moore11j/congress-tracker/pull/671) source6c549122 merged bbbc72c3c19c7a9e846ff5548238c87634f654bc. Exact-head security workflow38762960441 passed both audits and secret scan; required CI-tool logs verify `pip_audit -r backend/requirements.txt` reports no known vulnerabilities and production npm audit reports zero. Preview succeeds. Main Vercel deployment HJQeUABSVm2tBp9SwvV35TEK5R5E also reports success for this exact merge.

Guarded backend workflow38763224424/job116519661522 succeeds: exact image built15:47:04, reservation ready15:47:09, owner release15:47:52 and readiness/Premium privacy checks pass. Runtime Python3.12.15 verifies Pillow12.3.0 and unchanged pdf2image1.17.0/pytesseract0.3.13/pypdf6.19.0, exact requirements hash c85d144222e61cec2600797f983bf82334620fb077539a6cf123321dd6f6fc04, and synthetic PNG/JPEG/WebP encode/decode. No production database reads, writes or network calls inside the verifier. Receipt runtime-security-1792079361.json.

Public readiness200/databaseokay, Feed0.409s and Insights0.290s/no literal exception page (fusb-public-1792079363.json). These are immediate HTTP spot checks, not signed-in/streamed rendering or sustained capacity proof. New local ticker snapshot reliability repair is tracked separately. CAT/institutional export restrictions remain unchanged. Attachment attempt failed at the100-item chat limit; the PR link remains usable.
