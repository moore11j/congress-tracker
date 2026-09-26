# Top Stock Ideas and discovery access — September 26, 2026

Implementation validated locally. The owner approved committing and deploying on September 26 after reviewing two explicitly requested sample emails. The rank/ticker/company line is bold in both email variants.

## Search-facing copy decision

Inspected the [production homepage](https://walnutmarkets.com/) on September 26. Its H1 is **Follow the Insiders. Know More Before You Buy.**, with the eyebrow **Insider Trading Tracker & Stock Research**. Both remain unchanged, along with the search title, description, canonical and structured-data identities.

Reviewed the latest saved Keyword Planner evidence in [keyword-indexing-priorities-2026-09-21.md](keyword-indexing-priorities-2026-09-21.md) and [homepage-keyword-direction-2026-09-21.md](homepage-keyword-direction-2026-09-21.md). US/Google/last-12-month estimates: “insider trading tracker,” “congress stock trades,” “stock research” and “stock research platform” each 1,000–10,000 monthly searches; “insider buying stocks” 100–1,000. These are ranges, not traffic forecasts or conversion evidence. Also reviewed [seo_keyword_language.md](seo_keyword_language.md).

The latest saved Search Console review, [September 25](seo-focused-improvements-2026-09-25.md), reports 3 homepage clicks, 37 impressions and average position 15.9 over its 28-day window. This small sample does not establish a better headline. No new live Keyword Planner or GSC export was obtained in this implementation.

The supporting hero message now says Walnut filters reported insider buying/selling, Congress trades, institutional holdings, financials and technicals to surface the strongest ideas. Those research terms remain covered. The primary CTA promises immediate access to #1/#2. The workflow underneath is ranked ideas → optional supporting research → monitoring.

## Access contract

| Viewer | Stock ideas | Evidence | Participant leaderboards |
|---|---|---|---|
| Guest | True ranks #3–#5; #1/#2 are identity-free locked cards | Short reason and source labels | True #3–#5 identities; #1/#2 locked |
| Free account | #1–#5 | Ticker, company, rank, short reason, basic labels | #1–#5 identities; paid metrics withheld |
| Premium | Up to 10, bounded by existing screener result entitlement | Confirmation Score and detailed source summaries; Pro sources remain protected | Full stored Congress/insider rankings; institution identities only |
| Pro/admin | Up to 25, bounded by existing screener result entitlement | Entitled deeper source summaries | Full stored rankings including institutions |

The shared `ranking_access.py` projection runs before serialization on `/api/top-stocks`, `/api/leaderboards/preview`, `/api/leaderboards/dashboard`, and the section routes. Guest/Free rows use explicit field allowlists. Candidate arrays, filter variants, unknown metadata, score bands/directions and full explanations are absent from these responses. Guests cannot obtain #1/#2 identities by requesting the authenticated route without a valid session. Invalid/expired sessions receive the guest projection. Authenticated responses are private/no-store; preview responses are no-store. Public UI reads use a versioned preview URL.

The marketing page reads the authenticated dashboard only with a session; otherwise it requests the guest preview. Its own projection also strips scores and rejects guest ranks outside #3–#5. No client-side blur or CSS hiding implements access control.

## Ranking

Existing canonical ticker Confirmation Score and bullish/strong qualification remain primary. No public Idea Score was added. Equal scores are ordered by observed score change, bullish source alignment, Congress-confirmed insider clusters, insider cluster breadth, published strategy entries, then the existing market-cap/symbol tie-breaks.

The scheduled refresh reads normalized insider purchases using the profile cluster rules and a bounded date window, plus recent `trade_added` events for published strategies. Acceleration uses a previous canonical snapshot with the same context version, between six hours and seven days old. Missing or incompatible history contributes no acceleration tie-break. Existing source inputs cover Congress/insider buying, signals, institutional accumulation, government contracts, analyst changes, fundamentals and price/volume where available.

Canonical positions are computed before tier evidence redaction, so upgrading does not relabel a stock's rank. Public reasons are short descriptions, such as “Strong multi-source confirmation” or “Insider cluster with Congress confirmation.” Detailed source summaries reuse existing tier redaction. The existing Premium ticker research supplies Cross-Source Divergence and Similar Historical Setups; strategy following keeps its existing entitlement.

## Email and signup

`top_stock_ideas_frequency` defaults to `off` for existing and new accounts. Account Settings offers Off, Weekly and Daily; Daily is disabled for Free and rejected server-side. Password and Google signup routes show the optional weekly prompt on the welcome page and preserve an explicit research destination as a continuation link. Pressing the subscription button is the opt-in; registration alone does not subscribe.

Free receives at most five ideas weekly. Premium/Pro can choose weekday daily or weekly delivery with their idea limits and entitled summaries. A retained Daily preference after a downgrade is delivered weekly at Free limits. Verification, suspension/deletion and existing master delivery opt-outs are respected. The standalone opt-in endpoint does not re-enable master switches or change other subscriptions.

The existing digest CLI, templates, delivery logging, provider handling and idempotency are reused. New kind: `top_ideas`; template: `alerts.top_stock_ideas`. Scheduled runs are 13:10 Pacific on weekdays with retries at :25/:40/:55; weekly delivery runs on Friday. Identity is per user/local date or week. Retries seek beyond recipients already delivered so the per-run limit does not repeatedly select the first recipients. Force never bypasses Top Ideas consent or duplicate protection. No qualifying ideas produces no email. Every message includes the snapshot timestamp and a manage/turn-off link.

Dry-run command:

```sh
python -m app.jobs.send_email_digests --kind top_ideas --limit 100 --dry-run
```

## Release requirements

Deploy the backend schema/access changes before the frontend. The existing SQLite and PostgreSQL account-schema setup adds the preference with default Off. Refresh the leaderboard snapshot to populate optional cluster/strategy/acceleration context. Seed the new email template through the existing template mechanism. Clear any previously cached public ranking responses/pages that contained the old #1/#2 teaser; changing future response cache headers cannot retract older public responses. Preserve the existing `EMAIL_DIGEST_SCHEDULE_ENABLED` and dry-run controls and inspect a Top Ideas dry-run before activating real delivery for opted-in users.

## Verification

- 107 existing/extended backend ranking, leaderboard, digest and Fly schedule checks passed.
- 32 new HTTP access, real-session, preference, email-tier, deduplication and template-escaping checks passed.
- 49 focused frontend homepage/SEO/leaderboard/signup/session checks passed.
- TypeScript and the final production build passed.
- Browser inspected the homepage at 1280px and 390px, with one H1 and no horizontal overflow. Local fixture cards showed locked #1/#2 followed by real #3/#4/#5 positions. Inspected the weekly opt-in prompt and preserved continuation link. Fixtures are clearly named Example/DEMO and were never inserted into product data.
- Browser outage behavior was also observed: unavailable rankings render an honest unavailable message without fabricated rows.

Two explicitly requested, one-time samples were sent to the owner's specified address through Postmark (delivery records 837 and 838). Subscription settings were not changed. No production signup or billing action was executed. Existing unrelated work and files were retained.
