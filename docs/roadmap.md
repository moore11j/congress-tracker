# Congress Tracker Roadmap (Free-first)

## 1) Objective and constraints

Congress Tracker is being built as an investor-grade market intelligence platform that can meet or exceed QuiverQuant/CapitolTrades baseline value in the **free tier first**, then unlock monetization through Premium/Advanced/Enterprise later.

### Hard constraints
- Planning-only artifact; no backend code changes in this task.
- Preserve canonical events table and existing backend timestamp correctness.
- Assume congressional reporting lag is unavoidable; mitigate via product design, labeling, attribution models, and cross-tape confirmation.
- Every roadmap item below has a measurable definition of done (DoD).

---

## 2) Competitive feature map (Quiver vs Congress Tracker)

| Feature | Quiver has | We have now | Gap | Priority | Tier (free/premium/advanced) | Notes |
|---|---|---|---|---|---|---|
| Congressional trades feed | Yes | Yes (House + Senate, filters, pagination, whale filters) | Add disclosure-lag-aware labeling and defaults | P0 | Free | Keep ordering correct to canonical event timestamps |
| Member pages + trade history | Yes | Yes (member profile endpoints exist) | Add performance/"alpha" views vs SPY, role/committee UX slots | P0 | Free | Committee views become active when data available |
| Ticker pages for political flow | Partial | Yes (ticker profiles exist) | Add flow timeline + explainers + confirmation panel placeholders | P1 | Free now; advanced depth later | Visual-first over tables |
| Insider trades feed | Yes | In progress / partial | Complete actionable UX: buy/sell mode, min value, role/title, security class, value math | P0 | Free | Required for baseline parity |
| Institutional/"whale" style signals | Yes | Partial (whale-style filters in feed) | Define multi-source interface now; ingest later | P1 | Free interface now; Premium data velocity later | Keep UI contract stable before data expansion |
| Government contracts data | Yes | Not yet | Add dataset slot + schema contract + UI placeholder | P2 | Free baseline cards later | Start with summary widgets before deep drill-down |
| Lobbying data | Yes | Not yet | Add roadmap slot and event model extension design | P2 | Free baseline cards later | Tie to member/ticker context |
| Unified searchable events tape | Partial | Partially unified (trades + watchlist feeds) | Build unified events tape UX across congress + insider (then options/dark pool) | P0 | Free | Primary user entry point |
| Alerts | Yes | Watchlist base exists | Add rule-based alerts and explainers; avoid noisy defaults | P1 | Free basic now; Premium limits later | Entitlements scaffold now, no gating yet |
| Backtesting | Yes | Not yet | Build lag-aware, slippage-aware framework (bands, not single line) | P1 | Advanced later | Start with spec + attribution model in free docs/UX |
| Mobile parity | Mixed | Partial | Full parity checklist for feed/member/ticker/watchlists | P0 | Free | Ship parity scorecard per release |
| Visualization clarity | Mixed feedback | Partial | Flow timelines, role/security badges, readable charts, progressive disclosure | P0 | Free | Reduce filter overload and cognitive load |
| API / webhooks | Yes (some plans) | Not yet | Plan contracts + entitlement hooks | P2 | Premium/Enterprise later | No paywall now; implementation readiness only |

---

## 3) Tier scope (design now, gate later)

> Principle: **Free-first, no premature paywalls.** Build entitlement-ready architecture now, but keep core value open until free baseline dominance is reached.

### Free (current focus)
- Congress trades feed + member pages.
- Insider trades feed.
- Unified events tape.
- Basic filters/sort/search with sane defaults.
- Disclosure-lag-aware UI (trade date vs disclosed date clarity).
- Basic watchlists and event explainers.

**DoD for free baseline:**
- New user reaches first meaningful view in **<60 seconds** (measured in product analytics).
- Feed supports congress + insider events in one tape with mode-aware filters.
- All congress event cards show both trade date and disclosure date where available.

### Premium (later unlock)
- Unusual signals engine access control.
- Real-time net options flow overlays.
- Higher alert limits and richer signal explainers.
- API/webhooks.

**DoD for premium readiness (not gated yet):**
- Every premium candidate capability is wrapped by entitlement checks configurable per tier.
- Feature flags can toggle premium features without schema changes.

### Advanced (later unlock)
- Multi-sort and advanced filter builder, saved views.
- Cross-tape correlations + strategy builder.
- Backtesting with slippage/lag/impact modeling.
- Exports.

**DoD for advanced readiness:**
- Strategy/backtest jobs have auditable assumptions and versioned parameters.
- Saved views serializable/shareable via stable query schema.

### Enterprise (later)
- Dedicated support and higher API limits.
- Organization-level controls and SLAs.

**DoD for enterprise readiness:**
- API rate-limit policy supports per-org overrides.
- Support observability dashboard exists for enterprise tenants.

---

## 4) User pain points and product responses

### Pain point 1: “Data lag makes congress trades non-actionable.”
**Response**
- Explicit “Disclosure Lag” labeling at card and detail level.
- Dual-date rendering (trade date and report/disclosure date).
- Backtesting attribution model based on disclosure-date availability.
- Later: add predictive proxies (options flow, dark pool blocks, insider buys) + confirmation score.

**DoD**
- 100% of congress cards render lag metadata where source data includes it.
- Backtest spec documents execution assumptions using disclosure date.
- UX usability test: ≥80% of users correctly interpret lag labels in first session.

### Pain point 2: “Alerts are only good when paired with more data.”
**Response**
- Cross-tape signal panel per event (confirmers/contradictors).
- Mode-aware defaults to reduce noisy alerts.
- Event explainers with transparent heuristic logic.

**DoD**
- Signal panel schema supports at least 3 confirmer types (insider, options, dark pool placeholders acceptable initially).
- Alert rule creation defaults to low-noise preset.
- Explainers visible on all alert-triggered events.

### Pain point 3: “Backtesting is too theoretical.”
**Response**
- Backtesting framework includes disclosure lag, slippage, and market impact approximation.
- Output shows realistic performance bands (base/best/worst), not single equity curve.

**DoD**
- Backtest UI spec requires lag + slippage inputs.
- Report output includes scenario bands and assumption table.
- Backtest run metadata persisted with assumption version.

### Pain point 4: “Mobile parity is lacking.”
**Response**
- Responsive parity checklist for feed/member/ticker/watchlists.
- Touch-first interactions for filters and watchlist actions.

**DoD**
- Four critical views pass parity checklist on mobile breakpoints.
- No horizontal scrolling on core cards at supported breakpoints.

### Pain point 5: “Visualization clarity can improve.”
**Response**
- Standardized visual modules: flows timeline, role/ownership badges, security class chips.
- Later modules for contracts/lobby spend charts.
- Progressive disclosure to prevent filter overload.

**DoD**
- Every event card uses standardized badge taxonomy.
- Timeline module available on ticker and member pages.
- Chart legends and units shown by default on all visual modules.

---

## 5) Milestones with measurable definitions of done

## NOW (Free baseline dominance)

### 5.1 Insider trade UX polish (actionable free experience)
**Scope**
- Purchases/sales-only mode.
- Minimum value threshold.
- Role/title and security class labels.
- Total value (`shares * price`) and `shares @ price` display.

**DoD**
- Filters for side + min value available in one-click controls.
- Insider cards show role/title and security class in compact layout.
- Value math displayed consistently across feed + detail views.

### 5.2 Disclosure-lag-aware congress UI
**Scope**
- Clearly display report/disclosure date.
- Preserve canonical ordering semantics.
- Publish “as-filed” backtesting foundation specification.

**DoD**
- Congress cards display dual dates when available.
- Ordering behavior documented and matches canonical events table semantics.
- Backtesting foundation spec approved by product + engineering.

### 5.3 Noise reduction defaults
**Scope**
- Mode-aware filtering with sane defaults.
- Reduced first-load clutter.

**DoD**
- Default filter state yields materially fewer low-signal events.
- Time-to-first-meaningful-signal median <60s for new users.

## NEXT (Free moat)

### 5.4 Member alpha/performance pages (lag-aware attribution)
**Scope**
- Member performance vs SPY and attribution notes tied to disclosure constraints.

**DoD**
- Member page includes benchmark comparison + attribution disclaimer.
- Attribution windows use disclosure-date model by default.

### 5.5 Watchlists v2
**Scope**
- Per-watchlist feed.
- Alert rules and templates (still free at this stage).
- Entitlement-ready limits (disabled gating).

**DoD**
- Each watchlist has dedicated event stream and filter memory.
- Users can create rule-based alerts from feed/member/ticker contexts.
- Limits configurable by tier flag without DB migration.

### 5.6 Free signal explainers
**Scope**
- “Why this matters” heuristics for key events.

**DoD**
- Explainers displayed on at least congress + insider events.
- CTR on explainer interaction tracked in analytics.

## LATER (Premium/Advanced unlock path)

### 5.7 Options flow ingestion + UI
**DoD**
- Net options flow metrics available in unified tape schema.
- Ticker page renders options flow panel with latency indicators.

### 5.8 Dark pool/institutional flow ingestion + UI
**DoD**
- Dark pool events mapped into canonical event model.
- Confirmation panel can reference dark pool signals.

### 5.9 Cross-tape confirmation panels
**DoD**
- Each major event type can display confirmer/contradictor evidence.
- Confirmation score computation is documented and versioned.

### 5.10 Realistic backtesting (slippage/lag/impact)
**DoD**
- Strategy runs expose lag/slippage/impact parameters.
- Results shown as performance bands with confidence/assumption notes.

### 5.11 API/webhooks + copy-trading integration plan
**DoD**
- API endpoints defined for core event/ticker/member/watchlist entities.
- Webhook delivery contract and retry semantics documented.
- External brokerage integration risks and boundaries documented.

---

## 6) Engineering roadmap (epics → milestones → dependencies)

## Epic A — Data model and event unification (Backend/Data)
**Milestones**
- A1: Unified event schema contract for congress + insider (NOW).
- A2: Source adapters for options/dark pool placeholders (NEXT/LATER).
- A3: Contracts/lobbying schema slots (LATER).

**Dependencies**
- Depends on canonical events table invariants.
- Unblocks Epics B, C, and D.

**DoD**
- Schema contract documented with versioning strategy.
- New source adapters can be added without breaking existing API consumers.

## Epic B — Free-first product UX (Frontend/Product)
**Milestones**
- B1: Insider actionable cards + filters (NOW).
- B2: Disclosure-lag-aware congress cards and dual-date views (NOW).
- B3: Unified events tape and visual modules (NEXT).
- B4: Mobile parity checklist completion (NOW/NEXT).

**Dependencies**
- Depends on Epic A schema consistency.
- Unblocks Epic D (signal explainers) and key activation metrics.

**DoD**
- Core views (feed/member/ticker/watchlist) meet parity and clarity criteria.
- Design system includes standardized role/security/flow badges.

## Epic C — Intelligence and attribution layer (Backend/Analytics)
**Milestones**
- C1: Lag-aware attribution specification (NOW).
- C2: Member performance baseline vs SPY (NEXT).
- C3: Backtesting scenario engine with assumption bands (LATER).

**Dependencies**
- Depends on A (event integrity) and market price data integration.
- Unblocks Premium/Advanced strategy experiences.

**DoD**
- Attribution methodology is reproducible and versioned.
- Backtest outputs include assumption metadata and scenario ranges.

## Epic D — Signals, alerts, and explainability (Backend/Frontend)
**Milestones**
- D1: Free explainers and low-noise defaults (NEXT).
- D2: Rule-based watchlist alerts v2 (NEXT).
- D3: Cross-tape confirmation scoring (LATER).

**Dependencies**
- Depends on A (multi-source events) and B (UI modules).
- Unblocks retention and alert usefulness metrics.

**DoD**
- Every alert includes at least one human-readable rationale.
- Alert usefulness telemetry (open/act/save) is tracked end-to-end.

## Epic E — Platform readiness for entitlements and scale (Infra/Backend)
**Milestones**
- E1: Tier entitlement service interface (NEXT, dark launch).
- E2: Feature-flag control plane for tiered rollout (NEXT).
- E3: API/webhook reliability and enterprise controls (LATER).

**Dependencies**
- Depends on stable feature boundaries from B/C/D.
- Unblocks monetization without product rewrites.

**DoD**
- Entitlement checks can be applied without endpoint redesign.
- Feature rollout can target cohorts/tier plans safely.

---

## 7) KPI framework and targets

## Activation
- **Metric:** time to first meaningful view.
- **Target:** median <60s for new users.
- **Instrumentation DoD:** event `first_meaningful_view` logged with source context.

## Retention
- **Metric:** D1 and D7 returning user rates.
- **Target:** sustained week-over-week improvement after NOW milestone completion.
- **Instrumentation DoD:** cohort dashboard segmented by acquisition source and first feature touched.

## Feed freshness
- **Metric:** `max(events.ts)` advances daily; source-level lag tracking.
- **Target:** no silent staleness windows during market days.
- **Instrumentation DoD:** freshness monitor and stale-feed alerts in ops dashboard.

## Alert usefulness
- **Metric:** explainer CTR, watchlist add rate after alert view, alert open-to-action rate.
- **Target:** upward trend after explainers + low-noise defaults release.
- **Instrumentation DoD:** linked funnel from alert delivery to action.

## Time-to-signal
- **Metric:** elapsed time from landing to first saved watchlist/signal interaction.
- **Target:** reduce progressively each release.
- **Instrumentation DoD:** path analysis for feed → explainer → watchlist actions.

---

## 8) Release governance checklist

Before moving milestones from planned to shipped:
- DoD criteria met and documented.
- Telemetry added for corresponding KPI.
- Mobile parity checklist passed for touched surfaces.
- Disclosure-lag semantics preserved for congress data views.
- Entitlement-ready boundaries maintained (even if not gated).

This roadmap is intentionally free-first while engineering the architecture for later Premium/Advanced monetization without rework.
