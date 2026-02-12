# Congress & Smart Money Flow Intelligence Platform

## Product Vision
Build a premium investor intelligence platform that tracks:

- U.S. Congress stock trades
- Institutional money flow (13F filings, hedge funds, insiders)
- Smart-money signals & unusual activity

Goal: become the go-to dashboard for political + institutional capital movement, combining CapitolTrades + WhaleWisdom + insider tracking with modern fintech UX.

## Core Value Proposition
Investors want actionable intelligence—not raw data.

- Who’s buying what
- Who consistently beats the market
- Smart-money trend confirmation
- Early signal detection
- Clean, fast, addictive UX

## Current State (Already Built)
- Backend: FastAPI + SQLite (Fly volume at `/data/app.db`), Uvicorn on `0.0.0.0:8080`
- Startup autoheal ingests data if DB empty
- Live data: Congress trades (House + Senate), enriched member metadata, securities table
- Watchlists implemented
- Feed supports whale trades, recency + amount filters, pagination

### Existing API Domains
- Trades: global feed, member profiles, ticker profiles
- Watchlists: CRUD + watchlist feed

## Target UX (Better Than CapitolTrades)
### Core principles
- Sub-second interactions
- Visual over tabular where possible
- Insights surfaced automatically
- Minimal clicks to alpha

### Must-have UX patterns
- Feed: infinite scroll, quick filters, saved views (premium), whale highlights, trend badges
- Ticker pages: smart money score, political sentiment score, accumulation/distribution charts, top buyers over time
- Member/Fund pages: performance vs S&P 500, win rate, favorite sectors, largest convictions
- Watchlists: signal alerts, smart clustering (e.g., “3 funds + 2 senators accumulating this week”)

## Monetization (Baked In)
### Free tier
- Basic Congress feed
- Limited filters
- Delayed data
- Limited watchlists

### Pro tier ($20–50/month)
- Real-time updates
- Advanced filters
- Smart money overlay
- Performance analytics
- Alerts
- Historical backtesting

### Institutional tier (later)
- API access
- Bulk exports
- Advanced signal models
- Custom dashboards

## Advanced Features (Differentiators)
### A) Smart Money Overlay
Merge Congress trades, 13F filings, insider buying, whale options flow (later) and show political + institutional confluence signals.

Example: “UNH bought by 4 senators + accumulated by 7 hedge funds this quarter.”

### B) Performance Intelligence
- ROI on disclosed trades
- Sharpe-like signal score
- Sector hit rate
- Average holding duration
- Rankings (best performing politicians, best smart money followers, best predictive tickers)

### C) Signal Engine (Premium)
- Accumulation alerts
- Unusual activity alerts
- Cross-flow confirmation
- Trend reversals

Example: “Whale accumulation detected in NVDA across Congress + funds in last 10 days.”

### D) Advanced Watchlists
Track themes (AI, Defense, Energy), members, funds, and flow signals.

### E) Backtesting
Answer: “If I followed congressional whale buys over the last 2 years, what return?”

## Technical Direction
- Keep FastAPI backend
- Gradually move to Postgres as scale matters
- Build analytics layer separately from ingestion
- Frontend in Next.js with modern fintech UI patterns

## Roadmap
### Phase 1 — UX MVP
Build modern frontend: feed, ticker pages, member pages, watchlists. Goal: feel better than CapitolTrades immediately.

### Phase 2 — Intelligence Layer
Smart money overlay, performance metrics, signal scoring, better enrichment.

### Phase 3 — Premium Features
Alerts, saved filters, backtesting, historical analytics.

### Phase 4 — Institutional Expansion
API, advanced datasets, custom dashboards.

## Non-Goals (For Now)
- No social features
- No trading execution
- No complex ML until signal logic is validated

## Guiding Principle
“Does this help users make better investing decisions faster?” If not — cut it.
