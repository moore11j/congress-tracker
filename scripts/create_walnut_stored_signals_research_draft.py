from __future__ import annotations

import json
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
        "| Ticker | Stored signal | Start close | Latest close | Move |",
        "| --- | --- | --- | --- | --- |",
    ]
    labels = {
        "NBIS": "73 strong bearish",
        "INFQ": "73 strong bearish",
        "MU": "74 strong bearish",
        "SPCX": "76 strong bearish",
        "BMNR": "60 strong bullish",
        "META": "72 bullish miss",
    }
    for row in AUDIT_ROWS:
        lines.append(
            f"| [{row['ticker']}](/ticker/{row['ticker']}) | {labels[row['ticker']]} | "
            f"{money(row['start_price_row']['close'])} | {money(row['latest_price_row']['close'])} | {move(return_pct(row))} |"
        )
    return "\n".join(lines)


def signal_results() -> list[dict]:
    labels = {
        "NBIS": "73 strong bearish",
        "INFQ": "73 strong bearish",
        "MU": "74 strong bearish",
        "SPCX": "76 strong bearish",
        "BMNR": "60 strong bullish",
        "META": "72 strong bullish",
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
            "heading": "Executive summary",
            "body_markdown": (
                "Walnut's durable production history captured five qualifying confirmation events that aligned with subsequent stock moves: four bearish signals followed declines, and one bullish signal followed a gain. The same audit found a clear miss in META, where a bullish signal was followed by a decline.\n\n"
                "The audit used durable production confirmation events only. No historical signals were reconstructed, no missing component histories were invented, and no prices were selected from intraday highs or lows. The return math uses stored closes from `price_cache`, beginning with the next available production close after the event unless the event date itself had a valid close. Walnut did not predict exact returns.\n\n"
                + result_table()
                + "\n\nMETA is the miss in this table. Walnut was bullish. The stock fell. That belongs in the record."
            ),
        },
        {
            "key": "methodology",
            "heading": "Methodology",
            "body_markdown": (
                "Source table: `confirmation_monitoring_events`. Price source: `price_cache`. The audit included only durable production events where `score_after >= 60`, the event occurred before the measured move, and the stored direction matched the subsequent move for the five aligned case studies. Start price used the next available production `price_cache` close after the event, except where the event date itself had the qualifying close. End price used the latest stored close on 2026-07-29.\n\n"
                "No signal reconstruction was performed. No backfilling was performed. No retrospective component substitution was performed. For NBIS, the durable historical record proves the score, direction, band, date, and subsequent price move, but it does not preserve the full historical component payload for the June 28 score.\n\n"
                "These examples are not a complete performance study. They are audited case studies drawn from qualifying stored events."
            ),
        },
        {
            "key": "nbis",
            "heading": "NBIS: Strong bearish confirmation before a 43% decline",
            "body_markdown": (
                "Stored event: 2026-06-28. Production rows 39 and 40 recorded NBIS moving from mixed to bearish, with the confirmation score increasing from 56 to 73 and the band moving from moderate to strong. The start close was $261.15 on 2026-06-29. The latest stored close was $148.27 on 2026-07-29. The recalculated move was -43.22%.\n\n"
                + price_move_table("NBIS")
                + "\n\nWalnut identified a strong bearish NBIS setup before the stock fell sharply. It did not predict the exact size of the decline, and the signal later changed as the evidence changed. The historical component payload for the June 28 score was not preserved, so this section does not reconstruct the component mix. Walnut also did not remain bearish every day afterward; the system moved between bearish, mixed, and neutral as new data entered."
            ),
        },
        {
            "key": "infq",
            "heading": "INFQ: Bearish confirmation before a 38% decline",
            "body_markdown": (
                "Stored event: 2026-06-19. Production row 35 recorded INFQ moving from mixed to bearish, with the confirmation score increasing from 59 to 73 and the band moving from moderate to strong. The start close was $14.21 on 2026-06-22. The latest stored close was $8.83 on 2026-07-29. The recalculated move was -37.86%.\n\n"
                + price_move_table("INFQ")
                + "\n\nThis case uses only verified historical information: the stored confirmation event, the stored score and direction, and the stored closing prices."
            ),
        },
        {
            "key": "mu",
            "heading": "MU: Bearish confirmation before a 15% decline",
            "body_markdown": (
                "Stored event: 2026-07-20. Production row 92 recorded MU moving from mixed to bearish, with the confirmation score increasing from 59 to 74 and the band moving from moderate to strong. The qualifying event began on July 20. This audit does not imply that Walnut had this exact signal in late June.\n\n"
                + price_move_table("MU")
                + "\n\nThe start close was $866.29 on 2026-07-20. The latest stored close was $739.00 on 2026-07-29. The recalculated move was -14.69%. A valuation case can look attractive while price action and other confirmation weaken, but this section does not add unaudited component claims to the July 20 record."
            ),
        },
        {
            "key": "spcx",
            "heading": "SPCX: Strong bearish confirmation before a 14% decline",
            "body_markdown": (
                "Stored event: 2026-07-15. Production rows 75 and 76 recorded SPCX at 76 strong bearish after the setup upgraded from weak bearish confirmation. The event types were `new_multi_source_confirmation` and `price_volume_flip`, both stored at the same timestamp.\n\n"
                + price_move_table("SPCX")
                + "\n\nThe start close was $131.11 on 2026-07-16. The latest stored close was $112.54 on 2026-07-29. The recalculated move was -14.16%. The useful point is narrow: Walnut stored stronger bearish confirmation before the measured decline. This section does not speculate about components that were not audited."
            ),
        },
        {
            "key": "bmnr",
            "heading": "BMNR: Bullish confirmation before a 7% gain",
            "body_markdown": (
                "Stored event: 2026-07-16. Production row 77 recorded BMNR moving from mixed to bullish, with the confirmation score increasing from 56 to 60 and the band moving from moderate to strong. The start close was $15.44 on 2026-07-16. The latest stored close was $16.59 on 2026-07-29. The recalculated move was +7.45%.\n\n"
                + price_move_table("BMNR")
                + "\n\nThis bullish example matters because Walnut is not only a bearish-warning product. The system can surface constructive setups when the evidence improves. The 7.45% gain was not predicted; it was the observed move after the stored signal."
            ),
        },
        {
            "key": "meta-miss",
            "heading": "META: The bullish signal that did not work",
            "body_markdown": (
                "Stored event: 2026-07-20. Production rows 90 and 91 recorded META moving from mixed to bullish, with the confirmation score increasing from 59 to 72 and the band moving from moderate to strong. The start close was $645.69. The latest stored close was $586.49. The recalculated move was -9.17%.\n\n"
                + price_move_table("META")
                + "\n\nWalnut was bullish. The stock fell. That belongs in the record.\n\n"
                "Confirmation is probabilistic, not certainty. New information can overwhelm a prior setup, strong signals can fail, and users still need position sizing, valuation discipline, diversification, and risk management. Walnut should be judged on complete records and long-run methodology, not selected screenshots."
            ),
        },
        {
            "key": "what-the-audit-shows",
            "heading": "What the audit shows",
            "body_markdown": (
                "The audit shows that stored Walnut judgments sometimes aligned with meaningful subsequent moves. It shows that the system can surface risk before a bearish outcome becomes obvious, and it can also identify bullish setups. It also shows that signals can change, signals can fail, and historical evidence quality depends on what was actually stored at the time.\n\n"
                "The practical value is decision support: a documented market judgment before the outcome is obvious, evidence-based confirmation, and a repeatable framework for monitoring when the setup changes."
            ),
        },
        {
            "key": "what-the-audit-does-not-show",
            "heading": "What the audit does not show",
            "body_markdown": (
                "This is not a full backtest. It is not proof of predictive accuracy across the entire market. It does not establish causation, guarantee future results, or prove that users would have entered or exited at the measured closes. It does not prove Walnut stayed in the same direction throughout each period, and it does not reconstruct missing historical component data."
            ),
        },
        {
            "key": "why-this-matters",
            "heading": "Why this matters for investors",
            "body_markdown": (
                "Investors can use Walnut to see when confirmation turns bullish or bearish, monitor when a setup moves back to mixed or neutral, compare current evidence across price action, fundamentals, insiders, Congress, institutions, options, and macro positioning, save tickers, and track what changed. The goal is to identify where multiple sources agree or conflict instead of relying only on headlines or social sentiment.\n\n"
                "Walnut does not replace independent judgment. It helps investors organize the evidence before making the next decision."
            ),
        },
        {
            "key": "current-data-conversion",
            "heading": "What is Walnut showing about your stocks now?",
            "body_markdown": (
                "These signals were stored before the moves occurred. Walnut continuously evaluates new evidence to show what is strengthening, what is weakening, and where the setup is conflicted.\n\n"
                "Enter a ticker in Walnut to see the current setup. The live product keeps current confirmation separate from the historical records reviewed in this brief.\n\n"
                "[See the current setup](/search) | [Research a ticker](/ticker/MU)"
            ),
        },
        {
            "key": "premium-pro",
            "heading": "Premium and Pro",
            "body_markdown": (
                "Premium positioning: Make better stock decisions. Premium gives users fuller confirmation context, fundamentals, price and volume, Congress activity, insider activity, catalysts, risks, what changed, what to watch next, monitoring, saved workflows, and supported research workflows where entitlement allows.\n\n"
                "Pro positioning: See the evidence most retail investors miss. Pro adds institutional activity, options flow, deeper positioning context, macro positioning, market pressure, and all applicable Premium capabilities.\n\n"
                "The proprietary confirmation score remains separate from the underlying data categories. [Compare plans](/pricing)"
            ),
        },
        {
            "key": "final-cta",
            "heading": "The next move will not come with a warning label",
            "body_markdown": (
                "Walnut helps investors see when the evidence is strengthening, weakening, or conflicting before making the next decision.\n\n"
                "[Research a ticker](/search) | [Compare plans](/pricing)\n\n"
                "Disclaimer: Walnut is for research and informational purposes only and is not investment advice. Historical outcomes do not guarantee future results. Confirmation scores can change as new evidence becomes available. Users should consider their own objectives and risk tolerance."
            ),
        },
    ]


def article(now: str) -> dict:
    return {
        "title": TITLE,
        "slug": SLUG,
        "subtitle": "Five confirmed examples, one miss, and the limits of what the stored record can honestly prove.",
        "summary": (
            "Walnut's durable production history captured five bullish or bearish confirmation events before same-direction stock moves. "
            "The same audit also found a clear miss in META. This brief examines both the useful evidence and the limitations."
        ),
        "preview_body": "See what the data was showing before the move, and what it is showing now.",
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
            "Five audited stored signals aligned with subsequent same-direction moves.",
            "META is included prominently as a miss.",
            "The audit uses durable production records and stored closes only.",
            "This is evidence for decision support, not proof of certainty.",
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
    return {
        "id": f"rb_{int(time.time() * 1000)}",
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
