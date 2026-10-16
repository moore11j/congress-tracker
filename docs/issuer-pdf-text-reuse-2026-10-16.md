# Bounded issuer PDF text reuse, October 16, 2026

## Evidence and change

The previous read-only ten-issuer production verification timed out without a completed receipt. Exact process scans followed the samePID2396, then confirmed it absent at00:59UTC (issuer-reader-liveness-1792112345.json). No duplicate was started, and its missing receipt is not acceptance or proof of a particular failure cause.

Local profiling of complete saved public originals identifies substantial repeated pypdf extraction cost. An initial profile omitted the registry symbol and failed only after extracting text; that run is not successful source validation. Corrected Linux3.12.3 profiling parses all eight available static originals successfully. The older MSFT FY2026Q2 original is missing locally and is explicitly excluded; no availability claim for it.

Move the existing full PDF extraction procedure unchanged into app/clients/issuer_pdf_text.py. A per-process exact-byte/page-policy cache retains at most8MiB of byte/string payloads and16entries; bounded mapping overhead is additional. It caches only immutable extracted text after complete permission/page/decoded/extracted-size checks. Large uncached results remain complete. Exceptions are not cached. Independent documents parse outside the lock; concurrent identical misses may parse twice, but only one result contributes to retained accounting.

Every read still validates current issuer, publication date, publisher, fiscal period and Q&A metadata. Prepared readers still verify raw source checksums, exact stored text/parsed metadata and source-chain identity. Different raw bytes or the40/60page policy cannot reuse another extraction proof. No persistent cache, source flags, freshness policies, schedule, request budgets, public data or host changes.

## Validation

476affected tests pass on LinuxPython3.12.3, including PDF permissions/limits, cache revision and metadata integrity, bounded eviction, concurrent readers, transcript canonical repeat and all eight discovery policies/collector scheduling. Initial run474passed/two Microsoft tests failed; both failures reproduce with the exact unchanged55f32028 parser. Their immutable-revision ORM default used the actual clock despite the fixture's simulated observation date. A scoped before-insert fixture listener now uses that simulated clock and is removed on teardown. Production availability checks and test assertions remain intact. No full-suite claim.

Nine complete public originals, seven PDFs and two HTML pages, match frozen55f32028 text and parsed metadata exactly on cold/repeated reads. They cover latest reviewed MSFT,NVDA,META,GOOGL,IBM,SOFI,MRNA,RDDT andUL. The comparison uses the existing complete files and original hashes, not excerpts or truncated inputs. Baseline total parser CPU11.991202seconds; first new parse11.328041seconds; full repeated cohort0.0171765seconds. Retained payload2,901,195bytes under8,388,608budget. This is a local repeated-parser saving, not cold startup, scheduled workload, live API latency, new transcript coverage or production capacity proof. Receipt: artifacts/issuer-text-reuse-linux-oct16.json. Cross-platform baseline loading initially failed because Linux Git cannot interpret the Windows worktree pointer; exact host-Git source was saved before the successful replay.

A prepared installed-code verifier passes local Python3.14.2 rehearsal: both file hashes, exact text repeat without re-extraction, changed-issuer rejection, fresh mutable parsed output and retained payload bound; zero network/production records. Receipt issuer-text-runtime-1792112877.json. Installed verification awaits deployment.

## Release and remaining work

Local implementation is pending reviewed release. PR694-698 remain merged but undeployed. At01:08UTC the protectedportfolio cycle is553/1135with zero failed scopes; an actual ordinary leaderboard refresh is live. No release retry/bypass/checkpoint clearing. Receipts release-blocker-1792112898.json and active-work-1792112902.json. Source coverage and full prepared-reader acceptance remain distinct from parsing efficiency.

Previous goal turn made concrete progress through Shell merge and ordinary issuer collection acceptance. This turn adds measured parser repair and complete-source equality. Goal remains active; core financials/transcripts remainFMP and institutional publication remainsoff. Broad financial/institutional/transcript coverage, all alert/digest/ranking consumers, sustained capacity, final paid-provider/Massive validation, zero-FMP traffic and billing retirement remain incomplete. Pending private exports untouched; no purchase/email/video/landing change.


## Reviewed merge and configured-reader acceptance, October16

[PR699](https://github.com/moore11j/congress-tracker/pull/699), tested4127d3a6629def70ce2756c6ebc06b45343fe2e9, mergedf81769811a98a9acf4806116c32b39fc41663076 at01:11:45UTC. All exact-head dependency audits, secret scan and Vercel preview succeed. Fetched merged-source diff is empty for both application files and both tests. PR attachment reached the existing100-item chat limit; the ordinary URL remains available. Unrelated frontend build metadata retains its originalA185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA hash.

Completed read-only production verification of all10configured readers: 9prepared (MGA,MSFT,NVDA,META,GOOGL,IBM,SOFI,MRNA,RDDT), holds=[{"symbol": "UL", "reason": "latest_issuer_index_not_verified"}]. Every prepared result has exact reader-validated source/text hashes, document/revision provenance and Q&A; no text/customer payload exported, zero writes. This is current selected reader behavior on the existing deployed image, not the new cache runtime or broad all-ticker transcript coverage. Receipt artifacts/issuer-reader-stream-1792113034.json.

The prior timed-out process was confirmed absent before this replacement probe. The new bounded20/25CPU-second probe streams each completed public-status result to a local receipt, preserving partial evidence if observation fails. It does not restart the prior process or change production collectors. Prepared transcript data remains separately gated; TRANSCRIPT_PROVIDER remainsFMP. Institutional/financial broad coverage, all source/alert/ranking consumers, sustained capacity and final providers/zero-FMP/billing gates remain. PR694-699 are merged, not deployed; no protected portfolio interruption or deployment retry.
