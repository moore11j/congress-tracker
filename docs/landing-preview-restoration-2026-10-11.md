# Landing preview and pricing restoration, October 11, 2026

Owner explicitly requests restoring public ranks 3-5 in Top-Ranked Stocks and Congress/insider/institutional previews, plus visible Premium/Pro prices. Scope is these loading failures only; no redesign, copy changes, new access rules or SEO work.

## Reproduction and cause

Live homepage returned HTTP200 in4.87seconds with zero ranked stock cards, all three performer previews unavailable and See pricing page fallbacks. Direct public plan config succeeded in19.14seconds and leaderboard preview in32.89seconds. Each of all four preview datasets contains exactly ranks3/4/5. Prices are PremiumUSD24.95/month or249.50/year and ProUSD39.95/month or399.95/year, matching existing frontend defaultPlanConfig.

Both landing calls had a2.5second no-store cutoff. Git blame traces that behavior to July pricing/September preview code, not a new intentional removal. Existing production shared-CPU saturation makes those fallbacks visible. Browser independently reproduces absent ranks; prices may reappear on an individual faster request.

## Scoped repair

Public preview only uses Next persistent cache, five-minute stale-while-revalidate. Refresh allows45seconds for the currently slow upstream and rejects missing ranks before cache publication. Exceptions remain inside the cache callback so a failed refresh retains its previous success. Authenticated dashboards stay outside that cache and can fall back only to the public preview on failure. First cold-cache load still depends on upstream; warm and verify at release. Source timestamps remain unchanged. This does not establish sustained backend performance recovery.

Pricing uses existing verified defaults if live plan config or a tier price is unavailable; successful live prices remain authoritative. Hero/layout/content and access rules unchanged.

## Checks and release

Eight focused rendering/projection checks pass, covering all four3-5 previews, outage prices, anonymous privacy and signed-in behavior. TypeScript passes. Broader31 checks yield26passes/five failures, all five reproduced against unchanged main; baseline also has one additional source-format assertion failure. Initial baseline helper used a Windows path instead of file URL and failed before testing; corrected comparison is saved locally. Production build and deployment verification pending. No backend deployment, provider switch, purchase, video or social publishing.
