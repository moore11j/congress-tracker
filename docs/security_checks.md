# Security Checks

These checks are CI and developer guardrails only. They must not change application runtime behavior, deployment secrets, database URLs, provider keys, Stripe configuration, ingest behavior, or rollback artifacts.

## Frontend Dependency Audit

Run the production dependency audit from the frontend workspace:

```powershell
cd frontend
npm audit --omit=dev
```

CI runs the same audit with a high/critical failure threshold:

```powershell
cd frontend
npm audit --omit=dev --audit-level=high
```

The high/critical threshold keeps CI repeatable if a moderate advisory remains in a framework-bundled transitive dependency and cannot be corrected without an unsafe downgrade. Do not run `npm audit fix --force` in CI.

## Backend Dependency Audit

Use `pip-audit` as a CI/developer tool, not as a production runtime dependency.

```powershell
python -m pip install pip-audit
python -m pip_audit -r backend/requirements.txt
```

Known false positives should be documented with the advisory ID, affected package, reason, and review date. Avoid broad ignores.

## Secret Scanning

Install `gitleaks` locally with your preferred package manager, then scan with redaction enabled:

```powershell
gitleaks detect --source . --redact --no-banner --exit-code 1
```

For a staged-change check before committing:

```powershell
gitleaks protect --staged --redact --no-banner
```

CI runs gitleaks on pull requests, manual dispatches, and the weekly schedule. The workflow uses the `pull_request` event, does not reference repository secrets, disables scanner comments/artifact uploads, and requests redacted findings.

The repository includes a narrow `.gitleaksignore` for historical `generic-api-key` false positives in generated dependency/build artifacts that are no longer tracked: `backend/.venv312/...sqlalchemy...` and `frontend/.next/...`. Do not add broad path or rule ignores; add a fingerprint only after confirming the value is not a secret.

## If A Secret Is Found

Stop work on the branch until the finding is understood. Do not paste or print the secret in an issue, PR, chat, or terminal transcript.

Remove the secret from the working tree. If it was committed, remove it from git history before merging. Rotate the affected secret in its provider or secret store, then verify deployments, CI logs, local logs, screenshots, and shared archives did not expose it.

## Local Artifact Hygiene

Local `.env`, database, SQLite journal/WAL/SHM, log, screenshot, generated audit, and backend artifact paths are ignored by `.gitignore`. Keep intentional product assets in tracked asset directories such as `frontend/public`; keep local debugging screenshots and generated reports out of commits.

Before sharing a branch, check tracked sensitive artifact patterns without printing file contents:

```powershell
git status --short
git ls-files | findstr /i "\.env \.db \.sqlite \.sqlite3 \.log"
```

On macOS/Linux:

```sh
git status --short
git ls-files | grep -Ei '(\.env|\.db|\.sqlite|\.sqlite3|\.log)'
```
