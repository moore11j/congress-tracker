# Walnut chat workspace

November 24, 2026. Owner changes the preferred product direction to a chat-first main application, with the ticker research page inside the conversation, contextual follow-up questions and a left sidebar for Leaderboards, Outcomes and tools. Premium and Pro gates must remain. This supersedes the earlier Discover-dashboard visual direction; it is not approval to replace the production homepage or activate model spending.

## Reviewable implementation

The additive `/ask` preview has a full-height conversation layout, desktop sidebar, native mobile menu and persistent question composer. Company lookup uses the existing same-origin search. An exact match opens research; ambiguous results require a selection. The selected ticker is in the URL, so reloads preserve the company. Conversation text and recent companies are held only in component memory, not persisted to an account, local storage or a model service.

The server page renders the **unchanged existing ticker page component** inside the conversation. It receives normal request cookies/headers through the same existing server readers, with no query-supplied tier or auth override. There is no iframe, copied ticker backend, token bridge, modified CSP or relaxed access check. The outer route is dynamic and noindex. Existing charts, financials, news, source dates, coverage messages, Premium/Pro gates and explicit follow actions remain those of the ticker page.

Supported follow-ups navigate the active company's existing chart, financials, news, filings, ownership, research or overview/catalyst/risk panels. This is a bounded command interpreter, not an AI explanation. Unsupported questions are not answered from model memory. The preview explicitly says that open-ended AI answers are still to come. No OpenAI/provider credential work, model call, new spending or answer-generation backend is included.

Ranked stocks can open within the conversation using the existing permission-projected cache component. Selecting a ranked company keeps its research in chat; modified clicks retain the normal ticker link. Other tools remain direct sidebar links to their existing gated routes. Their feature flags are retained. Existing main navigation, Feed and marketing pages remain on their current routes; only `/ask` uses the new shell.

Browser verification found that the existing upgrade-return parameter allowlist omitted `symbol`, losing the selected chat company on a pricing round trip. That research parameter is now retained while auth/campaign fields remain stripped. Existing pricing tiers and checkout behavior are unchanged.

## Remaining product work

1. Owner review of this concrete interaction and visual direction.
2. A bounded Ask backend that receives the selected company and permitted evidence, answers follow-ups with citations/dates, validates claims/numbers and rechecks current access before every answer. Paid data must never enter an unauthorized answer or shared cache. Existing AI integration and explicit spend/credential gates apply before model execution.
3. Conversation persistence with account ownership, deletion/retention and plan-change reauthorization. The preview does not promise saved conversations.
4. More tools as typed interactive conversation results, reusing their existing calculations and access controls. Outcomes must retain its ten-row Free preview and Premium navigation/sort/size rules; Feed exports/sorting remain Pro.
5. Authenticated Free/Premium/Pro/admin browser acceptance, production login-return compatibility, passive network/load checks and migration release sequencing before making this the main app entry.

## Checks

Nine chat cases cover bounded context navigation, unsafe symbols/URLs, preservation of legacy chrome, use of the original server ticker renderer, ignored query entitlement hints, same-origin lookup, ambiguity, failure, cancellation and upgrade return state. A ranking-link case checks inline versus modified clicks. A middleware case verifies noindex and marketing-to-app routing. Existing workspace/auth/lifecycle/upgrade checks also run:44 focused frontend cases pass. Exact TypeScript, preview CI and desktop/mobile browser results are recorded separately when complete.

The first exact preview `1e94c7e3` rendered the native NBIS ticker inside chat and switched to Financials after a typed follow-up, preserving the company URL. Guest Premium/Pro gate callouts remained visible. The financials panel reported unavailable and the quote displayed a delayed-refresh warning; these are outstanding source/data conditions, not successful financial coverage. Mobile390 had no document overflow and its composer stayed within the viewport. The native sidebar opened and selected rankings appeared inline with guest ranks3–5. No console errors were observed. These guest checks do not establish full authenticated paid-tier parity, passive network acceptance or production readiness.

The institutional replay continues independently without restart or changes to its bound source files. No production release, source activation, pricing change or customer send is part of this preview.
