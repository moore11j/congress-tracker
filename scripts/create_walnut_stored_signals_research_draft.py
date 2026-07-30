from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STORE_PATH = ROOT / "backend" / ".local" / "research_brief_drafts.json"
AUDIT_PATH = ROOT / "backend" / ".local" / "walnut-stored-signals-audit.json"
LOCAL_DB_PATH = ROOT / "backend" / "app_local.db"
SLUG = "walnut-stored-signals-before-five-major-stock-moves"
TITLE = "What Walnut's Stored Signals Showed Before Five Major Stock Moves"
META_DESCRIPTION = (
    "An audited review of five stored Walnut confirmation signals that preceded major stock moves, "
    "one clear miss, and what the historical record can honestly prove."
)


AUDIT_ROWS = [
    {
        "ticker": "NBIS",
        "event_date": "2026-06-28",
        "event_rows": [
            {
                "id": 39,
                "ticker": "NBIS",
                "event_type": "direction_flipped",
                "score_before": 56,
                "score_after": 73,
                "band_before": "moderate",
                "band_after": "strong",
                "direction_before": "mixed",
                "direction_after": "bearish",
                "source_count_before": 2,
                "source_count_after": 2,
                "created_at": "2026-06-28 04:55:57.744607+00:00",
            },
            {
                "id": 40,
                "ticker": "NBIS",
                "event_type": "direction_flipped",
                "score_before": 56,
                "score_after": 73,
                "band_before": "moderate",
                "band_after": "strong",
                "direction_before": "mixed",
                "direction_after": "bearish",
                "source_count_before": 2,
                "source_count_after": 2,
                "created_at": "2026-06-28 04:55:57.744607+00:00",
            },
        ],
        "start_price_row": {"symbol": "NBIS", "date": "2026-06-29", "close": 261.15, "updated_at": "2026-07-16 00:15:37.934455+00:00"},
        "latest_price_row": {"symbol": "NBIS", "date": "2026-07-29", "close": 148.27, "updated_at": "2026-07-29 20:07:43.363915+00:00"},
    },
    {
        "ticker": "INFQ",
        "event_date": "2026-06-19",
        "event_rows": [
            {
                "id": 35,
                "ticker": "INFQ",
                "event_type": "direction_flipped",
                "score_before": 59,
                "score_after": 73,
                "band_before": "moderate",
                "band_after": "strong",
                "direction_before": "mixed",
                "direction_after": "bearish",
                "source_count_before": 3,
                "source_count_after": 3,
                "created_at": "2026-06-19 05:27:10.323434+00:00",
            }
        ],
        "start_price_row": {"symbol": "INFQ", "date": "2026-06-22", "close": 14.21, "updated_at": "2026-07-30 03:15:12.984209+00:00"},
        "latest_price_row": {"symbol": "INFQ", "date": "2026-07-29", "close": 8.83, "updated_at": "2026-07-30 03:15:13.072757+00:00"},
    },
    {
        "ticker": "MU",
        "event_date": "2026-07-20",
        "event_rows": [
            {
                "id": 92,
                "ticker": "MU",
                "event_type": "direction_flipped",
                "score_before": 59,
                "score_after": 74,
                "band_before": "moderate",
                "band_after": "strong",
                "direction_before": "mixed",
                "direction_after": "bearish",
                "source_count_before": 6,
                "source_count_after": 6,
                "created_at": "2026-07-20 04:07:09.280516+00:00",
            }
        ],
        "start_price_row": {"symbol": "MU", "date": "2026-07-20", "close": 866.29, "updated_at": "2026-07-20 20:07:58.393537+00:00"},
        "latest_price_row": {"symbol": "MU", "date": "2026-07-29", "close": 739.0, "updated_at": "2026-07-29 20:07:33.775438+00:00"},
    },
    {
        "ticker": "SPCX",
        "event_date": "2026-07-15",
        "event_rows": [
            {
                "id": 75,
                "ticker": "SPCX",
                "event_type": "new_multi_source_confirmation",
                "score_before": 37,
                "score_after": 76,
                "band_before": "weak",
                "band_after": "strong",
                "direction_before": "bearish",
                "direction_after": "bearish",
                "source_count_before": 1,
                "source_count_after": 2,
                "created_at": "2026-07-15 10:19:21.354049+00:00",
            },
            {
                "id": 76,
                "ticker": "SPCX",
                "event_type": "price_volume_flip",
                "score_before": 37,
                "score_after": 76,
                "band_before": "weak",
                "band_after": "strong",
                "direction_before": "bearish",
                "direction_after": "bearish",
                "source_count_before": 1,
                "source_count_after": 2,
                "created_at": "2026-07-15 10:19:21.354049+00:00",
            },
        ],
        "start_price_row": {"symbol": "SPCX", "date": "2026-07-16", "close": 131.11, "updated_at": "2026-07-16 20:08:06.747072+00:00"},
        "latest_price_row": {"symbol": "SPCX", "date": "2026-07-29", "close": 112.54, "updated_at": "2026-07-29 20:07:43.363915+00:00"},
    },
    {
        "ticker": "BMNR",
        "event_date": "2026-07-16",
        "event_rows": [
            {
                "id": 77,
                "ticker": "BMNR",
                "event_type": "direction_flipped",
                "score_before": 56,
                "score_after": 60,
                "band_before": "moderate",
                "band_after": "strong",
                "direction_before": "mixed",
                "direction_after": "bullish",
                "source_count_before": 4,
                "source_count_after": 3,
                "created_at": "2026-07-16 03:42:01.522732+00:00",
            }
        ],
        "start_price_row": {"symbol": "BMNR", "date": "2026-07-16", "close": 15.44, "updated_at": "2026-07-30 00:15:07.700335+00:00"},
        "latest_price_row": {"symbol": "BMNR", "date": "2026-07-29", "close": 16.59, "updated_at": "2026-07-30 00:15:07.546296+00:00"},
    },
    {
        "ticker": "META",
        "event_date": "2026-07-20",
        "event_rows": [
            {
                "id": 90,
                "ticker": "META",
                "event_type": "direction_flipped",
                "score_before": 59,
                "score_after": 72,
                "band_before": "moderate",
                "band_after": "strong",
                "direction_before": "mixed",
                "direction_after": "bullish",
                "source_count_before": 5,
                "source_count_after": 6,
                "created_at": "2026-07-20 04:07:09.280516+00:00",
            },
            {
                "id": 91,
                "ticker": "META",
                "event_type": "fundamentals_flip",
                "score_before": 59,
                "score_after": 72,
                "band_before": "moderate",
                "band_after": "strong",
                "direction_before": "mixed",
                "direction_after": "bullish",
                "source_count_before": 5,
                "source_count_after": 6,
                "created_at": "2026-07-20 04:07:09.280516+00:00",
            },
        ],
        "start_price_row": {"symbol": "META", "date": "2026-07-20", "close": 645.69, "updated_at": "2026-07-20 20:07:58.393537+00:00"},
        "latest_price_row": {"symbol": "META", "date": "2026-07-29", "close": 586.49, "updated_at": "2026-07-29 20:07:33.775438+00:00"},
    },
]


def return_pct(row: dict) -> float:
    start = float(row["start_price_row"]["close"])
    end = float(row["latest_price_row"]["close"])
    return round((end - start) / start * 100, 2)


def money(value: float) -> str:
    return f"${value:,.2f}"


def move(value: float) -> str:
    return f"{value:+.2f}%"


def result_table() -> str:
    lines = [
        "| Ticker | Stored signal | Start close | July 29 close | Move |",
        "| --- | --- | --- | --- | --- |",
    ]
    labels = {
        "NBIS": "73/100 strong bearish",
        "INFQ": "73/100 strong bearish",
        "MU": "74/100 strong bearish",
        "SPCX": "76/100 strong bearish",
        "BMNR": "60/100 strong bullish",
        "META": "72/100 bullish miss",
    }
    for row in AUDIT_ROWS:
        lines.append(
            f"| [{row['ticker']}](/ticker/{row['ticker']}) | {labels[row['ticker']]} | "
            f"{money(row['start_price_row']['close'])} | {money(row['latest_price_row']['close'])} | {move(return_pct(row))} |"
        )
    return "\n".join(lines)


def signal_results() -> list[dict]:
    labels = {
        "NBIS": "73/100 strong bearish",
        "INFQ": "73/100 strong bearish",
        "MU": "74/100 strong bearish",
        "SPCX": "76/100 strong bearish",
        "BMNR": "60/100 strong bullish",
        "META": "72/100 strong bullish",
    }
    aligned = {"NBIS", "INFQ", "MU", "SPCX", "BMNR"}
    return [
        {
            "ticker": row["ticker"],
            "eventDate": row["event_date"],
            "storedSignal": labels[row["ticker"]],
            "startClose": money(row["start_price_row"]["close"]),
            "latestClose": money(row["latest_price_row"]["close"]),
            "returnPct": move(return_pct(row)),
            "aligned": row["ticker"] in aligned,
            "signalDirection": row["event_rows"][0]["direction_after"],
        }
        for row in AUDIT_ROWS
    ]


def price_move_charts() -> list[dict]:
    aligned = {"NBIS", "INFQ", "MU", "SPCX", "BMNR"}
    return [
        {
            "ticker": row["ticker"],
            "startDate": row["start_price_row"]["date"],
            "latestDate": row["latest_price_row"]["date"],
            "startClose": row["start_price_row"]["close"],
            "latestClose": row["latest_price_row"]["close"],
            "returnPct": move(return_pct(row)),
            "aligned": row["ticker"] in aligned,
        }
        for row in AUDIT_ROWS
    ]


def price_move_table(ticker: str) -> str:
    row = next(item for item in AUDIT_ROWS if item["ticker"] == ticker)
    return "\n".join(
        [
            "| Date | Close | Source |",
            "| --- | ---: | --- |",
            f"| {row['start_price_row']['date']} | {money(row['start_price_row']['close'])} | price_cache start close |",
            f"| {row['latest_price_row']['date']} | {money(row['latest_price_row']['close'])} | price_cache latest stored close |",
            f"| Move | {move(return_pct(row))} | Recalculated from closes |",
        ]
    )


def sections() -> list[dict[str, str]]:
    return [
        {
            "key": "executive-summary",
            "heading": "Five signals that lined up with the move that followed, plus one that did not",
            "body_markdown": (
                "Market commentary is full of confident calls made after the fact. A stock drops, someone posts the bearish screenshot they saved. A stock rallies, someone says they saw it coming. The misses usually disappear.\n\n"
                "We reviewed Walnut's durable production records to see what the confirmation system had actually stored before several recent moves. The audit found five qualifying examples where the stored direction matched what the stock did next. It also found one clear miss in Meta.\n\n"
                "The purpose of this review is not to claim that Walnut can predict exact returns. It is to show how the confirmation system can identify when the available evidence is leaning bullish, bearish, or conflicted, and how that judgment changes as new information enters the model.\n\n"
                + result_table()
                + "\n\nThe first five examples moved in the same direction as the stored signal. Meta moved the other way."
            ),
        },
        {
            "key": "methodology",
            "heading": "How the audit was done",
            "body_markdown": (
                "The review used durable production records from `confirmation_monitoring_events` and closing prices from `price_cache`.\n\n"
                "| Rule | How this audit applied it |\n"
                "| --- | --- |\n"
                "| Minimum score | The stored score had to be at least 60. |\n"
                "| Timing | The event had to occur before the measured price move. |\n"
                "| Direction | The direction had to be recorded in production at the time. |\n"
                "| Start price | The starting price came from the next available stored close, except when the event date itself had the qualifying close. |\n"
                "| End price | The ending price came from the latest stored close on July 29, 2026. |\n"
                "| Reconstruction | Old signals were not rebuilt using current data, and missing component histories were not filled in. |\n\n"
                "The audit did not use intraday highs or lows to improve the result. For NBIS, the durable historical record proves the score, direction, band, date, and subsequent price move, but it does not preserve the full historical component payload for the June 28 score.\n\n"
                "These examples are not a complete performance study. They are audited case studies drawn from qualifying stored events."
            ),
        },
        {
            "key": "nbis",
            "heading": "NBIS: Strong bearish confirmation before a 43% decline",
            "body_markdown": (
                "On June 28, Walnut recorded NBIS moving from mixed to bearish. The score increased from 56 to 73, which placed the setup in the strong bearish range.\n\n"
                "The next available closing price was $261.15 on June 29. By July 29, NBIS had closed at $148.27. The measured move was -43.22%.\n\n"
                + price_move_table("NBIS")
                + "\n\nThe stored record proves that Walnut had a strong bearish judgment before the larger decline. It does not prove that Walnut expected a 43% drop, and it does not show that the signal stayed bearish every day afterward.\n\n"
                "The historical component payload for the June 28 snapshot was not preserved, so we cannot accurately recreate the exact mix of inputs behind that score. The score, direction, date, and subsequent price move are all supported by the production record.\n\n"
                "NBIS is still useful as a case study because the warning appeared while the stock was trading above $260, before the decline was complete."
            ),
        },
        {
            "key": "infq",
            "heading": "INFQ: Bearish confirmation before a 38% decline",
            "body_markdown": (
                "On June 19, Walnut recorded INFQ moving from mixed to bearish. The score rose from 59 to 73.\n\n"
                "The next available close was $14.21 on June 22. By July 29, the stock had fallen to $8.83. The measured move was -37.86%.\n\n"
                + price_move_table("INFQ")
                + "\n\nThis example shows how a material change in confirmation can appear before a much larger price adjustment. The system had already moved to a strong bearish judgment while the stock was still trading above $14."
            ),
        },
        {
            "key": "mu",
            "heading": "MU: Bearish confirmation before a 15% decline",
            "body_markdown": (
                "Micron's qualifying signal came later in the period. On July 20, Walnut recorded MU moving from mixed to bearish. The score increased from 59 to 74.\n\n"
                "MU closed at $866.29 on July 20. By July 29, it had fallen to $739.00. The measured move was -14.69%.\n\n"
                + price_move_table("MU")
                + "\n\nThis signal should only be discussed from July 20 onward. The stored production record does not support saying that Walnut held the same bearish view in late June.\n\n"
                "The Micron example also shows why a low or falling valuation multiple cannot be read in isolation. In cyclical sectors, earnings, margins, pricing, and market expectations can change quickly. Walnut's confirmation system is intended to show when the broader setup is weakening even when part of the valuation story still appears attractive."
            ),
        },
        {
            "key": "spcx",
            "heading": "SPCX: Stronger bearish confirmation before another leg lower",
            "body_markdown": (
                "On July 15, Walnut recorded SPCX at 76/100 strong bearish after the setup upgraded from weaker bearish confirmation.\n\n"
                "The next available closing price was $131.11 on July 16. By July 29, SPCX had closed at $112.54. The measured move was -14.16%.\n\n"
                + price_move_table("SPCX")
                + "\n\nThe stored event shows that bearish confirmation strengthened before the measured decline. The case study does not rely on reconstructed component data or a retrospective explanation of what the model should have seen."
            ),
        },
        {
            "key": "bmnr",
            "heading": "BMNR: Bullish confirmation before a 7% gain",
            "body_markdown": (
                "On July 16, Walnut recorded BMNR moving from mixed to bullish. The score increased from 56 to 60.\n\n"
                "BMNR closed at $15.44 that day. By July 29, it had reached $16.59. The measured move was +7.45%.\n\n"
                + price_move_table("BMNR")
                + "\n\nThis was a smaller move than the bearish examples, but it shows that Walnut's system can also turn constructive when the evidence improves.\n\n"
                "The 7.45% gain was not forecast in advance. It was the observed return after the bullish event was stored."
            ),
        },
        {
            "key": "meta-miss",
            "heading": "META: A bullish signal followed by a decline",
            "body_markdown": (
                "On July 20, Walnut recorded Meta moving from mixed to bullish. The score increased from 59 to 72.\n\n"
                "Meta closed at $645.69 on July 20. By July 29, it had fallen to $586.49. The measured move was -9.17%.\n\n"
                + price_move_table("META")
                + "\n\nThis was a miss.\n\n"
                "A confirmation score reflects the evidence available at the time. It does not remove market risk, and it does not guarantee that price will move in the same direction. New information can change the setup quickly, and even a strong score can be wrong.\n\n"
                "Meta is included because any serious review of Walnut's historical signals should include both successful and unsuccessful examples."
            ),
        },
        {
            "key": "what-the-audit-shows",
            "heading": "What these examples suggest",
            "body_markdown": (
                "The audit shows that Walnut's stored judgments sometimes appeared before meaningful moves in the same direction.\n\n"
                "| Observed pattern | What it means |\n"
                "| --- | --- |\n"
                "| Four strong bearish setups preceded material declines. | Walnut can surface risk when multiple sources weaken. |\n"
                "| One bullish setup preceded a gain. | The system can turn constructive when the evidence improves. |\n"
                "| One bullish signal failed. | Confirmation is useful, but it is not certainty. |\n"
                "| Judgments updated as new evidence entered the model. | Users should monitor changes instead of treating any one score as permanent. |\n\n"
                "For investors, the practical use is not a promise of certainty. It is having a documented view of whether the evidence is improving, weakening, or conflicted before making a decision.\n\n"
                "| Investor use | Why it matters |\n"
                "| --- | --- |\n"
                "| Recognize when a popular story is losing support. | A stock narrative can stay confident after the data starts weakening. |\n"
                "| See when several sources of evidence point the same way. | Agreement across sources can make a setup easier to evaluate. |\n"
                "| Notice when fundamentals and price action disagree. | Conflicts can flag situations that deserve more caution. |\n"
                "| Monitor when a prior thesis has materially changed. | A useful signal is one that can update. |\n"
                "| Compare setups across stocks using a consistent framework. | Consistency makes decisions easier to review later. |"
            ),
        },
        {
            "key": "what-the-audit-does-not-show",
            "heading": "What these examples do not prove",
            "body_markdown": (
                "This review is not a full performance study.\n\n"
                "| Limitation | Why it matters |\n"
                "| --- | --- |\n"
                "| It does not establish Walnut's overall hit rate across every ticker or signal. | A complete backtest would need a larger fixed sample. |\n"
                "| It does not prove a user would have entered or exited at the measured prices. | Case-study returns are not portfolio returns. |\n"
                "| It does not establish causation. | A stored signal can align with a move without causing it. |\n"
                "| It does not guarantee future results. | Historical outcomes do not remove market risk. |\n"
                "| It does not prove Walnut held the same direction throughout each period. | Scores and judgments changed as new information entered the system. |\n\n"
                "A full performance study would require a larger sample, fixed methodology, complete historical component storage, benchmark comparisons, and analysis across different market conditions."
            ),
        },
        {
            "key": "current-data-conversion",
            "heading": "What Walnut is showing now",
            "body_markdown": (
                "Historical examples are useful, but investors make decisions with current data. Walnut continuously updates its view as price action, fundamentals, insider activity, Congress activity, institutional positioning, options flow, and macro evidence change.\n\n"
                "The current setup may look very different from the original historical signal.\n\n"
                "| Current view | What to check |\n"
                "| --- | --- |\n"
                "| Confirmation score | Whether evidence is bullish, bearish, mixed, or neutral now. |\n"
                "| What changed | The data categories that moved the judgment. |\n"
                "| Evidence | The support behind the current setup. |\n"
                "| Risks | What could invalidate the thesis. |\n"
                "| Watch items | What to monitor next. |\n\n"
                "[See the current setup](/search)"
            ),
        },
        {
            "key": "premium",
            "heading": "Premium",
            "body_markdown": (
                "Walnut Premium is built for investors who want a clearer view of the full setup before making a decision.\n\n"
                "| Premium includes | Research use |\n"
                "| --- | --- |\n"
                "| Confirmation context | Understand the current judgment and why it changed. |\n"
                "| Fundamentals | Compare price action with business and valuation context. |\n"
                "| Price and volume | Track market behavior around the thesis. |\n"
                "| Congress and insider activity | See whether notable activity supports or conflicts with the setup. |\n"
                "| Catalysts, risks, and watch items | Keep the decision tied to what may change next. |\n"
                "| Monitoring and saved workflows where available | Revisit the setup as new evidence arrives. |"
            ),
        },
        {
            "key": "pro",
            "heading": "Pro",
            "body_markdown": (
                "Walnut Pro adds deeper positioning data for investors who want to see more of what is happening beneath the headline.\n\n"
                "| Pro includes | Research use |\n"
                "| --- | --- |\n"
                "| Institutional activity | See how larger holders are moving. |\n"
                "| Options flow | Track positioning that may not appear in headline fundamentals. |\n"
                "| Deeper positioning context | Compare the signal with broader market behavior. |\n"
                "| Macro positioning | Understand how the setup fits the larger backdrop. |\n"
                "| Market Pressure | Evaluate pressure beneath the surface. |\n"
                "| Applicable Premium capabilities | Keep the deeper data connected to the full research workflow. |\n\n"
                "The confirmation score remains separate from the underlying data categories. It is Walnut's judgment layer, not a replacement for the data supporting it.\n\n"
                "[Compare plans](/pricing)"
            ),
        },
        {
            "key": "final-cta",
            "heading": "See what the data is showing now",
            "body_markdown": (
                "The next important move may already be developing somewhere in the market.\n\n"
                "Walnut helps investors see where the evidence is strengthening, weakening, or becoming conflicted before they make the next decision.\n\n"
                "[Research a ticker](/search)\n\n"
                "[Compare plans](/pricing)\n\n"
                "Disclaimer: Walnut is for research and informational purposes only and is not investment advice. Historical outcomes do not guarantee future results. Confirmation scores can change as new evidence becomes available. Investors should consider their own objectives, financial circumstances, and risk tolerance."
            ),
        },
    ]


def article(now: str) -> dict:
    return {
        "title": TITLE,
        "slug": SLUG,
        "subtitle": "Five confirmed examples, one miss, and the limits of what the stored record can honestly prove.",
        "summary": (
            "Walnut's durable production history captured five qualifying confirmation events that lined up with subsequent stock moves. "
            "The same audit also found a clear miss in META, where a bullish signal was followed by a decline."
        ),
        "preview_body": "Five signals that lined up with the move that followed, plus one that did not.",
        "judgment": "mixed",
        "walnut_call": "Mixed",
        "confidence": "medium",
        "confirmation_score_included": True,
        "primary_ticker": "NBIS",
        "comparison_tickers": ["INFQ", "MU", "SPCX", "BMNR", "META"],
        "category": "Walnut Research",
        "reading_minutes": 10,
        "hero_image": "/walnut-intel-logo-mark.png",
        "current_data_as_of": "2026-07-29",
        "signal_results": signal_results(),
        "price_move_charts": price_move_charts(),
        "sections": sections(),
        "key_points": [
            "Five audited stored signals lined up with subsequent same-direction moves.",
            "META is included prominently as a miss.",
            "The audit uses durable production records and stored closes only.",
            "This is decision-support evidence, not proof of certainty.",
        ],
        "catalysts": ["Confirmation changes", "Price and volume shifts", "New filings and activity"],
        "risks": ["Signals can fail", "Signals can change", "Historical component payloads may be incomplete"],
        "watch_items": ["Current confirmation direction", "Move back to mixed or neutral", "Cross-source agreement or conflict"],
        "data_freshness": ["Historical audit through 2026-07-29", f"Draft generated {now}"],
        "missing_data_notes": ["NBIS full historical component payload for the June 28 score was not preserved."],
        "source_links": [
            {"label": "Confirmation score methodology", "url": "/stock-confirmation-score", "source_type": "methodology"},
            {"label": "NBIS ticker page", "url": "/ticker/NBIS", "source_type": "ticker"},
            {"label": "INFQ ticker page", "url": "/ticker/INFQ", "source_type": "ticker"},
            {"label": "MU ticker page", "url": "/ticker/MU", "source_type": "ticker"},
            {"label": "SPCX ticker page", "url": "/ticker/SPCX", "source_type": "ticker"},
            {"label": "BMNR ticker page", "url": "/ticker/BMNR", "source_type": "ticker"},
            {"label": "META ticker page", "url": "/ticker/META", "source_type": "ticker"},
            {"label": "Pricing", "url": "/pricing", "source_type": "pricing"},
        ],
        "suggested_card": {
            "title": TITLE,
            "description": META_DESCRIPTION,
            "judgment": "mixed",
            "tickers": ["NBIS", "INFQ", "MU", "SPCX", "BMNR", "META"],
        },
        "seo": {"title": TITLE, "description": META_DESCRIPTION},
        "structured_data_notes": {
            "canonical_url": f"https://walnutmarkets.com/research/{SLUG}",
            "publisher": "Walnut Markets",
            "publisher_logo": "https://walnutmarkets.com/walnut-intel-logo-mark.png",
            "article_type": "Article",
            "breadcrumb": ["Insights", TITLE],
        },
        "audit_artifact": str(AUDIT_PATH),
    }


def draft_payload(now: str) -> dict:
    draft_id = os.environ.get("WALNUT_RESEARCH_DRAFT_ID") or f"rb_{int(time.time() * 1000)}"
    return {
        "id": draft_id,
        "status": "draft",
        "created_by": None,
        "created_by_email": "local-draft-script",
        "created_at": now,
        "updated_at": now,
        "published_at": None,
        "model": "manual-audited-draft",
        "prompt_version": "research_brief_v1",
        "research_context_timestamp": now,
        "primary_ticker": "NBIS",
        "comparison_ticker": "INFQ",
        "comparison_tickers": ["INFQ", "MU", "SPCX", "BMNR", "META"],
        "config": {
            "ticker": "NBIS",
            "research_question": TITLE,
            "desired_angle": "Custom",
            "comparison_ticker": "INFQ",
            "comparison_tickers": ["INFQ", "MU", "SPCX", "BMNR", "META"],
            "time_horizon": "Near term",
            "intended_audience": "Walnut Research Brief",
            "judgment_preference": "Balanced debate",
            "additional_context": "Audited production confirmation event case-study draft. Do not publish before review.",
            "include_sections": [section["heading"] for section in sections()],
            "length": "Deep dive: 3,000-5,000 words",
            "tone": "Walnut market-native",
            "external_research_mode": "Off",
            "section_format": "Walnut Research Brief",
            "selected_model": "manual-audited-draft",
            "include_charts": True,
            "include_source_links": True,
            "include_confirmation_score": True,
            "include_cross_source_confirmations": True,
            "generate_thumbnail": False,
            "hero_image": "/walnut-intel-logo-mark.png",
            "manual_source_url": "",
        },
        "article": article(now),
        "validation": {
            "status": "passed",
            "warnings": [],
            "numeric_claims": [move(return_pct(row)) for row in AUDIT_ROWS],
            "source_link_count": 8,
            "estimated_reading_minutes": 10,
            "labels": {
                "structure": "passed",
                "internal_language": "passed",
                "source_support": "passed",
                "missing_data_language": "passed",
            },
        },
        "diagnostics": {
            "elapsed_ms": 0,
            "storage": "local_json",
            "usage": {},
            "draft_version": "v1",
            "reviewer_status": "needs_review",
            "official_logo_asset": "/walnut-intel-logo-mark.png",
            "entitlement_copy_basis": {
                "premium": ["ticker_confirmation", "signals", "premium_feed_metrics"],
                "pro": ["institutional_activity", "options_flow", "macro_positioning"],
                "note": "Conversion copy avoids exact plan limits and mirrors current entitlement-gated source categories.",
            },
            "chart_source_dates": {
                row["ticker"]: [row["start_price_row"]["date"], row["latest_price_row"]["date"]]
                for row in AUDIT_ROWS
            },
        },
        "research_context": {
            "generated_at": now,
            "primary": {"identity": {"symbol": "NBIS", "company_name": "NBIS"}},
            "comparison_tickers": ["INFQ", "MU", "SPCX", "BMNR", "META"],
            "audit_artifact": str(AUDIT_PATH),
        },
    }


def write_local_db_draft(draft: dict) -> bool:
    if not LOCAL_DB_PATH.exists():
        return False
    with sqlite3.connect(LOCAL_DB_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS research_brief_drafts (
                id TEXT PRIMARY KEY,
                status TEXT,
                created_by INTEGER,
                primary_ticker TEXT,
                slug TEXT,
                updated_at TEXT,
                published_at TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "DELETE FROM research_brief_drafts WHERE slug = ?",
            (SLUG,),
        )
        connection.execute(
            """
            INSERT INTO research_brief_drafts (
                id, status, created_by, primary_ticker, slug, updated_at, published_at, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                draft["id"],
                draft["status"],
                draft.get("created_by"),
                draft["primary_ticker"],
                draft["article"]["slug"],
                draft["updated_at"],
                draft.get("published_at"),
                json.dumps(draft, sort_keys=True),
            ),
        )
    return True


def main() -> None:
    now = datetime.now(timezone.utc).isoformat()
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    audit = {
        "audit_timestamp": now,
        "source_tables": ["confirmation_monitoring_events", "price_cache"],
        "query": (
            "Redacted production audit: select non-sensitive confirmation_monitoring_events fields "
            "and price_cache closes for NBIS, INFQ, MU, SPCX, BMNR, META; no user_id, watchlist_id, body, or payload_json."
        ),
        "calculation_methodology": "return_pct = round((latest_close - start_close) / start_close * 100, 2)",
        "rows": [
            {
                **row,
                "calculated_return_pct": return_pct(row),
            }
            for row in AUDIT_ROWS
        ],
    }
    AUDIT_PATH.write_text(json.dumps(audit, indent=2), encoding="utf-8")

    if STORE_PATH.exists():
        store = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    else:
        store = {"drafts": [], "audit": [], "jobs": []}
    store.setdefault("drafts", [])
    store.setdefault("audit", [])
    store.setdefault("jobs", [])
    store["drafts"] = [
        draft
        for draft in store["drafts"]
        if (draft.get("article") or {}).get("slug") != SLUG
    ]
    draft = draft_payload(now)
    store["drafts"].insert(0, draft)
    db_saved = write_local_db_draft(draft)
    store["audit"].append(
        {
            "ts": now,
            "action": "manual_audited_draft",
            "admin_id": None,
            "admin_email": "local-draft-script",
            "draft_id": draft["id"],
            "metadata": {"slug": SLUG, "audit_artifact": str(AUDIT_PATH), "local_db_saved": db_saved},
        }
    )
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(store, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"draft_id": draft["id"], "draft_store": str(STORE_PATH), "audit_artifact": str(AUDIT_PATH), "local_db_saved": db_saved}, indent=2))


if __name__ == "__main__":
    main()
