# Meta and Alphabet official transcript preparation, October 10

Adds two reviewed company periods to the existing bounded issuer collector: Meta Q2 2026 (July29) issuer PDF with an exact official-host CDN redirect, and Alphabet Q2 2026 (July22) official event HTML. Existing Microsoft and NVIDIA sources remain unchanged. Collection remains at most two reviewed pages per hourly run with a daily per-page recheck. Public transcript selection remains FMP and canonical publication remains separately gated.

## Original-source evidence

Meta original: https://investor.atmeta.com/files/doc_financials/2026/q2/META-Q2-2026-Earnings-Call-Transcript.pdf
Exact redirect: https://s21.q4cdn.com/399680738/files/doc_financials/2026/q2/META-Q2-2026-Earnings-Call-Transcript.pdf
Source SHA256 e2afcb9b452656bea2706e1dedc190d31b3aafe5528af7f3c19de6ab347a34ec; extracted60,793 characters across21pages; text SHA25649f0d6f28908c85333d431c9acc87fc27467b663f9d76c541b99ce6e68b38fee. Company, fiscal quarter, publication date, issuer attribution and Q&A verified.

Alphabet original: https://abc.xyz/investor/events/event-details/2026/2026-Q2-Earnings-Call-2026-GgTAq7Is0z/default.aspx
Source SHA25600df600d5f03db207a1ced5bb4b02fc090af3a13e8a3bf8a91175084f81af974; extracted55,407 characters; text SHA2566ae54d608efa57165694346203a958b588d287b0fb007ba3f29190e72b4b261f. Official page contains full transcript, date, company and Q&A. Earlier guessed short URL returned404 and is not registered.

## Validation and limits

76 focused issuer/PDF/direct-feed/schedule checks pass on Python3.14.2. New checks reject wrong company, date and period and enforce unique issuer-period registry identities. Captured original replay in isolated SQLite blocks HTTP/model/email transport, prepares one research document per source, repeats with identical whole-database fingerprints, then adds a transport-only revision and proves no duplicate document or receipt rewrite. Zero evidence events, email rows or production writes. Actual originals and receipts are retained locally under artifacts/direct-feeds/free-replacements-2026-10-09; hashes and conclusions above are durable.

Alphabet is collected once under GOOGL. GOOG remains explicitly unconfigured: canonical research documents currently carry one security association, and an implicit alias could return a document bound to the wrong share class. That association requires a separate consumer/schema decision; no duplicate source is registered to hide this gap. These four reviewed companies do not constitute broad transcript coverage or permission to disable FMP.

Deployment and actual scheduled collection still require verification. No subscriptions or public provider switches in this package.
