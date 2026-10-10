# Institutional ticker history reconciliation

The completed twelve-source diagnostic leaves 85 current comparisons waiting for identifiers. Many conflicts are the same common-stock CUSIP carrying historical and current ticker symbols. Eleven additional rules resolve only exact known CUSIPs, only after the relevant quarter-end, and only when independent evidence already supplies the current candidate. Unknown candidates, older reporting periods and unrelated CUSIPs remain held. No symbol is inferred from a similar issuer name.

| CUSIP | Change | Effective date and issuer evidence |
|---|---|---|
| 165167735 | CHK → EXE | [October 2, 2024, annual report](https://www.expandenergy.com/download/2024-annual-report/?wpdmdl=16645) |
| 668771108 | NLOK → GEN | [November 8, 2022](https://investor.gendigital.com/news/news-details/2022/Introducing-Gen-The-Company-to-Power-Digital-Freedom/default.aspx/1000/) |
| 88023U101 | TPX → SGI | [February 18, 2025, 8-K](https://www.sec.gov/Archives/edgar/data/1206264/000120626425000037/tpx-20250218.htm) |
| 69121K104 | ORCC → OBDC | [July 6, 2023; unchanged CUSIPs](https://www.blueowlcapitalcorporation.com/investors/news-events/press-releases/detail/58/owl-rock-capital-corporation-announces-upcoming-name-and) |
| 852234103 | SQ → XYZ | [January 21, 2025](https://investors.block.xyz/investor-news/news-details/2025/Block-Announces-Ticker-Symbol-Change-to-XYZ-To-Report-Fourth-Quarter-Results/default.aspx) |
| 02156V109 | ALCC → OKLO | [May 10, 2024](https://oklo.com/newsroom/oklo-inc-begins-trading-on-the-new-york-stock-exchange) |
| 34964C106 | FBHS → FBIN | [December 15, 2022](https://ir.fbin.com/investor-faqs) |
| G3223R108 | RE → EG | [July 10, 2023](https://investors.everestglobal.com/news/news-details/2023/Everest-to-Rebrand-Company-Name-and-NYSE-Ticker-to-Reflect-its-Evolution-Global-Growth-and-Diversification-Strategy/default.aspx) |
| 714046109 | PKI → RVTY | [May 16, 2023](https://ir.revvity.com/news/investor-news/news-details/2023/Launching-Revvity-A-Scientific-Solutions-Company-Powering-Innovation-from-Discovery-to-Cure/default.aspx) |
| 75524B104 | ROLL → RBC | [September 26, 2022](https://investor.rbcbearings.com/node/14696) |
| 90984P303 | UCBI → UCB | [August 6, 2024; ticker and CUSIP](https://ir.ucbi.com/resources/investor-faqs) |

Sources checked October10UTC. Each row's CUSIP candidates are also present in the captured institutional evidence; the issuer announcement establishes the dated ticker transition. Foreign dual-listing conflicts, missing candidates and evidence that arrived after a target filing remain unresolved. Sixty-two focused checks pass. The full 286-document, 143-pair replay completes in 1,110.13 seconds: 44 current comparisons pass versus 41 previously, while 82 still wait for identifiers. Twenty-six holds, two missing prior quarters and three unverified prior quarters remain. It prepares 259 filings, 72,561 positions, 3,181 changes, 3,165 summaries and 248 activity records. There are 14 final feed events; 39 is the incremental materialization counter, not the unique event count. Real monitoring, daily and watchlist builders yield 12, 8 and 14 items, respectively. Complete state and all three consumer contexts are identical on repeat. Zero production writes or email deliveries. State SHA-256: `ebdf20197a54103cd4be05203614d32bdb6c435cd0af34a18c26790954e42e86`. No institutional activation; remaining mapping, prior/value holds and live publication gates stay open.
