"""One-score candidate; research only until independently validated."""
from __future__ import annotations

import hashlib
import math

LAMBDAS = (0.0, 1.0, 2.0, 4.0, 8.0)
VERSION = "confirmation_integrated_risk_research_v1"


def issuer_partition(cik: str) -> str:
    if not cik or not cik.isdigit() or len(cik) != 10:
        raise ValueError("A normalized SEC issuer identity is required")
    bucket = int(hashlib.sha256(f"integrated-risk-v1|{cik}".encode()).hexdigest()[:8], 16) % 100
    return "development" if bucket < 80 else "evaluation"


def adjusted_score(score: int, direction: str, predicted_loss: float | None,
                   weight: float, *, usable: bool = True) -> dict:
    if isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 100:
        raise ValueError("Original score must be an integer from 0 to 100")
    if not math.isfinite(weight) or weight < 0:
        raise ValueError("Risk weight must be finite and nonnegative")
    out = {"score": score, "version": VERSION, "status": "direction_not_supported"}
    if direction != "bullish":
        return out
    if (not usable or predicted_loss is None or isinstance(predicted_loss, bool)
            or not math.isfinite(predicted_loss) or predicted_loss < 0):
        out["status"] = "unknown_input_fallback"
        return out
    out["score"] = min(score, math.floor(max(0., score - weight * predicted_loss) + .5))
    out["status"] = "adjusted" if weight else "unchanged_control"
    return out


def select(rows: list[dict], weight: float) -> list[dict]:
    """Ranking reads inputs only; integer ties deliberately ignore outcomes."""
    def order(row):
        value = adjusted_score(row["score"], row["direction"], row.get("predicted_loss"), weight)
        return -value["score"], row["ticker"], row["id"]
    return sorted(rows, key=order)[:math.ceil(len(rows) / 4)]
