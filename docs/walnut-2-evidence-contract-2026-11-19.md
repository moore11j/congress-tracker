# Walnut 2.0 evidence contract foundation

The owner approved the full Walnut 2.0 sequence November 15 and reaffirmed continued implementation November 19. This isolated foundation work overlaps with migration preparation. It does not add routes, change navigation, connect model calls, activate a provider or start delivery.

The repository already has `app/schemas.py`. Creating the proposal's illustrative `app/schemas/intelligence.py` package would conflict with existing imports, so the standalone contract lives in `app/intelligence_contract.py`.

The contract represents retained sources and revisions, security identities, locators, publication/event/first-available times, reported observations, facts, interpretations, risks, watch-next items, coverage and methodology. All collections and text are bounded. Unknown fields and operations are rejected. Timestamps must carry timezones, and evidence cannot become available or be published after the result's data cutoff. Scheduled future event times remain separate from publication/availability.

Reported numeric facts must match a retained observation's exact decimal value, unit and currency. Citations must resolve within the bundle and security scope. Missing evidence has explicit partial, stale, unavailable or quiet states; empty evidence cannot claim complete coverage. A source correction changes its revision and therefore its content fingerprint.

Personal monitoring/thesis operations require an owner. A consumption check compares the server-resolved account and access projection again, including after cache retrieval; a plan change requires a fresh projection. The fingerprint includes account, projection, source revisions and content. This is a defense against mixing cached results, not authentication or permission to access a source. Existing server authentication, entitlement checks and record ownership must run before assembly. Nothing is connected to a cache or endpoint yet.

The contract validates structural grounding, not the semantic truth of prose, correct provider data, source freshness policy or completeness of a research task. Labels and interpretations still require grounded generation and separate claim validation. V1 accepts reported facts only; calculations must get a versioned deterministic contract before derived results are supported. No model/prompt metadata is claimed because this foundation makes no model calls.

Acceptance tests cover source/date/identity injection, altered numbers and currency/units, nonfinite values, duplicate IDs, missing/duplicate citations, explicit empty states, owner and plan isolation, source-correction fingerprints, bounded queries and revalidation of Pydantic copies that bypass construction checks. Network access is forbidden in the roundtrip check. These tests are not the later 100-case Ask evaluation.

Next: implement cache-only authorized readers against the existing evidence services and source selectors, then connect a bounded facade and the new interface. Preserve the existing 84-route compatibility baseline and advanced tools. No public Ask or full monitoring completion is claimed.

Local validation at 2026-11-19 21:49 MST: 48 tests pass on the configured Python 3.14 environment in 1.19 seconds. Dedicated Python 3.12 Linux CI is prepared; no CI result yet.

2026-11-19 22:20 MST: Reconciled with deployed main9253e258; no application conflict. Prior code at ebe8a208 passed48Linux tests and all6checks. The approved proposal and historical audit sources are retained in this branch; no new product or source activation occurs.
