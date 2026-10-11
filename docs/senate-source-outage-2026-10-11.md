# Senate source reliability audit, October 11, 2026

The active FMP replacement goal requires revalidating already selected feeds as well as unfinished adapters. Current evidence contradicts a blanket claim that official Senate collection is reliable.

## Production evidence

Read-only production receipts at 20:18 and 20:21 UTC cover 60 Congress collection runs. Thirty-four contain complete Senate searches. The latest completed successful discovery is run85, October10 at16:22:21UTC, covering October3-10 with one filing. Twenty-five subsequent scheduled attempts return HTTP403 from the official notice page, starting October10 at20:17 and continuing through October11 at20:17 (run176). A separate historical running receipt is not counted as success. House discovery in run176 finds five reports; Senate discovery is null, rather than falsely empty.

The source control still selects official_senate, generation2, from October1. Two saved Senate originals remain parsed: Whitehouse October1 and Fetterman October7. No production source, data, schedule, resource or ownership change was made. The onset follows the combined-host handoff, but these receipts do not prove why the external site denies requests. No proxy rotation, challenge bypass or silent vendor fallback was attempted. Restoring reliable permitted unattended access or validating an independent backup remains necessary.

## Independent free snapshot comparison

Downloaded the public [community snapshot](https://huggingface.co/datasets/austin-starks/congressional-stock-trades/resolve/main/snapshot.json), generated October11 at11:32:37.357UTC. Manifest SHA256 is5769a981fd6eb48cea17133e3554644357a438e9530d3511dae8306f63a56f9a. Verified the exact manifest size and SHA256 of its2026 filings partition (74,222bytes,5ed7cb5418449ddf7c669aa63ba5373f4feb147191d735cc634fc4f86eac06d7) and economic-events partition (216,318bytes,da517deb1afade1f8fbe1094dfa4079429ed2e676bb4be439c95e424476674be).

The actual partition contains the same two October Senate filings. Both raw source hashes equal our independently captured official HTML and current production content hashes. Reparsed those originals with the current deployed parser, then compared complete economic row multisets: five Whitehouse stock partial sales and two Fetterman dependent-child corporate-bond purchases match on ticker, owner, action, partial-sale flag, transaction date, amount bounds, official URL, asset type and description. Multiplicity is preserved. The bonds remain non-equity, without fabricated tickers.

This establishes no additional October filing in that snapshot, not complete official coverage or reliable future delivery. Snapshot refresh time is distinct from source filing time. Its end-of-day availability convention and zero-based display row indices cannot replace official filing clocks or printed row identities. No community records were imported or published; no duplicate alerts or customer emails were created. Broader original/amended/scanned coverage and ongoing freshness remain unverified.

## Checks and operational implications

Current deployed source passes54 isolated Senate/direct-feed tests on Python3.14.2, including denial handling, complete-search receipts, pagination/identity rejection and staging repeat. A first local comparison helper incorrectly unpacked the two-value low-level parser; using the existing three-value parse_document interface corrected it before successful comparison. No application change or new deployment is included.

CPU observation also shows the latest startup repair is insufficient for sustained operation. At20:17 API burst reserve is about56.86CPU seconds and five-minute throttling zero. At20:22 reserve is approximately0.02CPU seconds and throttling0.6515CPU seconds/second. Diagnostic work is included; no attribution to ordinary traffic alone. This supersedes the optimistic post-restart samples as capacity evidence. Further load diagnosis remains necessary before institutional activation.

Evidence in the local direct-feeds folder: senate-runtime-1791749931.json, senate-runtime-1791750101.json, senate-crosscheck-snapshot-oct11.json, both checksum-verified Parquet partitions, senate-crosscheck-original-parses-oct11.json, senate-crosscheck-result-oct11.json and cpu-quota-1791750152.json. The durable findings are recorded here; local artifacts alone are not repository-wide availability.

Next: repair sustained capacity and Senate access, retaining complete-window recovery and provenance; finish institutional growth/capacity, core financial/transcript coverage and all scheduled consumers. Massive personal Stocks remains last. FMP stays active. Neither old successful Senate runs nor matching community rows close the current source-reliability gate.
