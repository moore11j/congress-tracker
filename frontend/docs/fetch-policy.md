# Congress Tracker Fetch Discipline Policy

## Route-load policy

1. Initial route load may request only data required by currently rendered content on that route.
2. Hidden UI (tabs, drawers, accordions, popovers, non-active panels) must not trigger network requests.
3. Detail fetch chains must be user-interaction gated (click/tap), not default-selected on mount.
4. Heavy dynamic routes (`/member/[slug]`, `/insider/[slug]`, `/ticker/[symbol]`, `/watchlists/[id]`) must not be auto-prefetched from list pages.
5. Duplicate same-resource requests within a route lifecycle are disallowed when avoidable.

## Operational guardrails

- Use `prefetch={false}` on links to heavy detail routes.
- Avoid metadata-time background fetches for detail records.
- Keep server fetches in the route subtree that directly renders the visible section.
- For temporary diagnostics, enable `CT_DEBUG_FETCH=1` (or `NEXT_PUBLIC_CT_DEBUG_FETCH=1`) to print `[ct-fetch]` traces with callsite context.
