"""Develop a risk deduction against real score snapshots, without production writes."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import urllib.error
from collections import Counter, defaultdict
from datetime import datetime, timezone, date
from pathlib import Path

import joblib
import numpy as np
import sklearn
from threadpoolctl import threadpool_limits

from analyze_historical_panel import price_features
from downside_selection import tree, fit_tree
from fetch_sec_fundamentals_pilot import fetch
from integrated_risk_score import LAMBDAS, adjusted_score, issuer_partition, select
from rank_calibration_model import add_cross_sectional_ranks, date_weights
from risk_reserved_confirmation import risk_matrix, verify_freeze
from sec_filing_features import extract_series, build_features

BASE = Path("frontend/test-results/confirmation-research")
PILOT = BASE / "sec-filing-pilot-2026-09-15"
RESERVED = BASE / "risk-reserved-confirmation-2026-09-15"
OUT = BASE / "integrated-risk-2026-09-15"
PROTOCOL = Path("docs/confirmation-integrated-risk-protocol-2026-09-15.md")
SCRIPTS = Path("backend/scripts/research")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_bytes())


def save_new(path, value):
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, allow_nan=False)


def verify(manifest):
    for name, digest in manifest["sha256"].items():
        if sha(Path(name)) != digest:
            raise ValueError("Frozen input changed: " + name)


def freeze():
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "freeze.json"
    if path.exists():
        value = read(path)
        verify(value)
        return value
    previous = verify_freeze(RESERVED)
    rows = read(PILOT / "prepared.json")["groups"]["development"]
    fit = [r for r in rows if r["outcomes"]["30"]["target_date"] < "2026-01-01"]
    pn, fn = sorted(fit[0]["ranks"]), sorted(fit[0]["fundamentals"])
    weights = date_weights(fit)
    losses = np.maximum(-np.array([r["outcomes"]["30"]["raw_return"] for r in fit]), 0)
    with threadpool_limits(limits=4):
        model = fit_tree(tree(), risk_matrix(fit, pn, fn), losses, weights)
    joblib.dump({"model": model, "price_names": pn, "fundamental_names": fn}, OUT / "model.joblib")
    files = [PROTOCOL, Path(__file__), SCRIPTS / "integrated_risk_score.py",
             SCRIPTS / "test_integrated_risk_score.py", BASE / "cohort.json",
             BASE / "ledger-trailing-prices.json", BASE / "recent-provider-prices.json",
             OUT / "model.joblib"]
    value = {"frozen_at_utc": datetime.now(timezone.utc).isoformat(),
             "fit_rows": len(fit), "fit_issuers": len({r["ticker"] for r in fit}),
             "latest_training_target": max(r["outcomes"]["30"]["target_date"] for r in fit),
             "versions": {"python": sys.version, "numpy": np.__version__, "sklearn": sklearn.__version__},
             "sha256": {**previous["sha256"], **{str(p): sha(p) for p in files}}}
    save_new(path, value)
    print("Frozen July risk model:", value["fit_rows"], "training rows; latest target", value["latest_training_target"], flush=True)
    return value


def decision_date(row, calendar):
    stamp = datetime.fromisoformat(row["calculated_at"].replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Calculation timestamp lacks timezone")
    # Cohort is August/September 2026, when the regular NYSE close is 20:00 UTC.
    if not "2026-08-01" <= stamp.date().isoformat() <= "2026-09-30":
        raise ValueError("Snapshot outside the audited daylight-saving-time range")
    days = [d for d in calendar if datetime.fromisoformat(d + "T20:00:00+00:00") <= stamp
            and d <= row["market_date"]]
    if not days or (stamp.date() - date.fromisoformat(days[-1])).days > 4:
        return None
    return days[-1]


def price_rows():
    prices = defaultdict(dict)
    for row in read(BASE / "ledger-trailing-prices.json")["rows"]:
        if row[6] == "fmp:historical-price-eod/full+corporate_actions" and date.fromisoformat(row[1]).weekday() < 5:
            prices[row[0]][row[1]] = row
    for row in read(BASE / "recent-provider-prices.json")["rows"]:
        if (row[6] == "massive:grouped-daily-adjusted" and row[7] == "split_adjusted_price_return"
                and row[2] == row[3] and date.fromisoformat(row[1]).weekday() < 5):
            prices[row[0]].setdefault(row[1], row)
    return prices


def prior_universe(prices, calendar, asof):
    past = [d for d in calendar if d <= asof][-253:]
    if len(past) != 253:
        return {}
    rows = []
    for ticker, history in sorted(prices.items()):
        if ticker in {"SPY", "QQQ"} or any(d not in history for d in past):
            continue
        close = np.array([history[d][2] for d in past], dtype=float)
        volume = np.array([history[d][5] for d in past], dtype=float)
        features = price_features(close, volume)
        if features is None:
            continue
        features["momentum_12_minus_1"] = (1 + features["momentum_252"]) / (1 + features["momentum_20"]) - 1
        dollars = close[-60:] * volume[-60:]
        features["dollar_volume60"] = float(np.nanmean(dollars)) if np.isfinite(dollars).any() else float("nan")
        rows.append({"ticker": ticker, "features": features,
                     "massive_history_bars": sum(history[d][6] == "massive:grouped-daily-adjusted" for d in past)})
    if len(rows) < 50:
        return {}
    add_cross_sectional_ranks(rows)
    return {r["ticker"]: {**r, "rank_universe_count": len(rows)} for r in rows}


def prepare():
    freeze()
    if (OUT / "predictions.json").exists():
        raise ValueError("Predictions already saved; do not overwrite")
    cohort = [r for r in read(BASE / "cohort.json")["events"] if "30" in r.get("outcomes", {})]
    mapping = {r["ticker"].upper(): str(r["cik_str"]).zfill(10) for r in read(PILOT / "company-tickers.json").values()}
    prices = price_rows()
    calendar = sorted(prices["SPY"])
    days = {r["id"]: decision_date(r, calendar) for r in cohort}
    universes = {}
    for d in sorted(set(days.values()) - {None}):
        universes[d] = prior_universe(prices, calendar, d)
        print("Prior-input universe", d, len(universes[d]), flush=True)
    needed = sorted({mapping[r["ticker"]] for r in cohort if r["ticker"] in mapping
                     and r["ticker"] in universes.get(days[r["id"]], {})})
    (OUT / "facts").mkdir(exist_ok=True)
    audit, series, fact_hashes = [], {}, {}
    for i, cik in enumerate(needed):
        cached = next((folder / "facts" / (cik + ".json") for folder in [PILOT, RESERVED, OUT]
                       if (folder / "facts" / (cik + ".json")).exists()), None)
        path = cached or OUT / "facts" / (cik + ".json")
        try:
            data = read(path) if cached else fetch("https://data.sec.gov/api/xbrl/companyfacts/CIK" + cik + ".json", path)
            status = "ok" if data.get("facts", {}).get("us-gaap") else "no_us_gaap"
            fact_hashes[str(path)] = sha(path)
            if status == "ok":
                series[cik] = extract_series(data)
        except urllib.error.HTTPError as exc:
            if exc.code in (403, 429):
                raise SystemExit(f"SEC access/rate limit {exc.code}; stopped")
            status = f"http_{exc.code}"
        except (urllib.error.URLError, TimeoutError) as exc:
            status = type(exc).__name__
        audit.append({"cik": cik, "status": status, "cached": cached is not None})
        if (i + 1) % 20 == 0:
            print("SEC inputs", i + 1, "/", len(needed), flush=True)
    artifact = joblib.load(OUT / "model.joblib")
    output, feature_rows, covered_ix = [], [], []
    reasons = Counter()
    for r in cohort:
        asof, cik = days[r["id"]], mapping.get(r["ticker"])
        prior = universes.get(asof, {}).get(r["ticker"])
        row = {k: r[k] for k in ["id", "ticker", "score", "direction", "entry_date", "security_id", "methodology_id"]}
        row.update(cik=cik, partition=issuer_partition(cik) if cik else None, decision_date=asof, predicted_loss=None)
        reason = "missing_prior_prices" if prior is None else "missing_issuer_mapping" if cik is None else "missing_company_facts" if cik not in series else None
        if reason is None:
            financials, usable, refs = build_features(series[cik], asof)
            if not usable:
                reason = "no_current_financials"
            else:
                if any(x["filed"] >= asof for x in refs):
                    raise AssertionError("Future filing leaked")
                covered_ix.append(len(output))
                feature_rows.append({**prior, "fundamentals": financials})
                row.update(filing_provenance=refs, rank_universe_count=prior["rank_universe_count"],
                           massive_history_bars=prior["massive_history_bars"])
        row["input_status"] = reason or "covered"
        reasons[row["input_status"]] += 1
        output.append(row)
    if feature_rows:
        with threadpool_limits(limits=4):
            predictions = artifact["model"].predict(risk_matrix(feature_rows, artifact["price_names"], artifact["fundamental_names"]))
        for index, prediction in zip(covered_ix, predictions):
            output[index]["predicted_loss"] = float(prediction)
    save_new(OUT / "predictions.json", output)
    save_new(OUT / "preparation.json", {"created_at_utc": datetime.now(timezone.utc).isoformat(),
              "coverage": dict(reasons), "covered_by_direction": dict(Counter(r["direction"] for r in output if r["predicted_loss"] is not None)),
              "fetch_audit": audit, "sha256": {**fact_hashes, str(OUT / "predictions.json"): sha(OUT / "predictions.json")}})
    print("Coverage", dict(reasons), flush=True)


def attach_outcomes(rows):
    events = {r["id"]: r for r in read(BASE / "cohort.json")["events"]}
    result = []
    for row in rows:
        original = events[row["id"]]
        if any(row[k] != original[k] for k in ["score", "direction", "ticker", "entry_date", "security_id"]):
            raise ValueError("Opening snapshot changed")
        o = original["outcomes"]["30"]
        if not all(math.isfinite(o[k]) for k in ["raw_return", "excess_return"]):
            raise ValueError("Nonfinite outcome")
        result.append({**row, "raw_return": o["raw_return"], "excess_return": o["excess_return"], "public_correct": o["correct"]})
    return result


def metrics(rows):
    if not rows:
        return {name: None for name in ["hit_rate", "downside", "large_loss_rate", "raw_return", "excess_return", "public_correct"]}
    raw = np.array([r["raw_return"] for r in rows])
    return {"hit_rate": float(np.mean(raw > 0)), "downside": float(np.maximum(-raw, 0).mean()),
            "large_loss_rate": float(np.mean(raw <= -10)), "raw_return": float(raw.mean()),
            "excess_return": float(np.mean([r["excess_return"] for r in rows])),
            "public_correct": float(np.mean([r["public_correct"] for r in rows]))}


def compare(rows, weight):
    groups = defaultdict(list)
    for r in rows:
        groups[r["entry_date"]].append(r)
    result = {"events": len(rows), "issuers": len({r["cik"] or "security:" + str(r["security_id"]) for r in rows}), "dates": len(groups)}
    for name, coefficient in [("baseline", 0.), ("integrated", weight)]:
        daily, pool = [], []
        for d, group in sorted(groups.items()):
            chosen = select(group, coefficient)
            pool.extend(chosen)
            daily.append({"date": d, "n": len(chosen), **metrics(chosen)})
        result[name] = {"n_selected": len(pool), "selected_ids": [r["id"] for r in pool], "daily": daily,
                        **{k: float(np.mean([r[k] for r in daily])) if daily else None for k in metrics([])}}
    result["improvements"] = {k: None if not rows else (result["baseline"][k] - result["integrated"][k] if k in {"downside", "large_loss_rate"}
                                                      else result["integrated"][k] - result["baseline"][k]) for k in metrics([])}
    return result


def choose_weight(rows):
    candidates = {str(w): compare(rows, w) for w in LAMBDAS}
    b = candidates["0.0"]
    adequate = b["events"] >= 100 and b["issuers"] >= 50 and b["dates"] >= 4
    eligible = []
    if adequate:
        for w in LAMBDAS[1:]:
            c = candidates[str(w)]; gain = c["improvements"]
            if (gain["hit_rate"] >= -.02 and gain["raw_return"] >= -.5
                    and gain["large_loss_rate"] >= 0 and gain["downside"] > 0):
                eligible.append((c["integrated"]["downside"], w))
    weight = min(eligible)[1] if eligible else 0.
    return {"weight": weight, "adequate_development_sample": adequate,
            "status": "exploratory_candidate" if weight else "no_nonzero_candidate", "candidates": candidates}


def verify_preparation():
    verify(read(OUT / "freeze.json"))
    verify(read(OUT / "preparation.json"))


def calibrate():
    verify_preparation()
    if (OUT / "selection.json").exists():
        raise ValueError("Calibration already frozen")
    rows = [r for r in read(OUT / "predictions.json") if r["partition"] == "development"
            and r["direction"] == "bullish" and r["predicted_loss"] is not None]
    selection = choose_weight(attach_outcomes(rows))
    selection.update(selected_at_utc=datetime.now(timezone.utc).isoformat(),
                     sha256={str(OUT / p): sha(OUT / p) for p in ["predictions.json", "freeze.json", "preparation.json"]})
    save_new(OUT / "selection.json", selection)
    print(json.dumps({k: v for k, v in selection.items() if k not in {"candidates", "sha256"}}, indent=2), flush=True)


def evaluate():
    verify_preparation()
    selected = read(OUT / "selection.json"); verify(selected)
    if (OUT / "results.json").exists():
        raise ValueError("Completed comparison exists; inspect it instead of retuning")
    rows = attach_outcomes(read(OUT / "predictions.json")); w = selected["weight"]
    bull = [r for r in rows if r["direction"] == "bullish"]
    covered = [r for r in bull if r["predicted_loss"] is not None]
    evaluation = [r for r in covered if r["partition"] == "evaluation"]
    first = {}
    for r in sorted(evaluation, key=lambda r: (r["entry_date"], r["id"])):
        first.setdefault(r["cik"], r)
    sections = {"evaluation_issuers": compare(evaluation, w),
                "evaluation_first_per_issuer": compare(list(first.values()), w),
                "evaluation_fmp_only": compare([r for r in evaluation if r["massive_history_bars"] == 0], w),
                "all_covered_exploratory": compare(covered, w), "full_bullish_with_fallback": compare(bull, w)}
    thresholds = {}
    for label, subset in [("evaluation", evaluation), ("full_with_fallback", bull)]:
        thresholds[label] = {}
        for threshold in (40, 60, 80):
            a = [r for r in subset if r["score"] >= threshold]
            b = [r for r in subset if adjusted_score(r["score"], r["direction"], r["predicted_loss"], w)["score"] >= threshold]
            thresholds[label][str(threshold)] = {"baseline": {"n": len(a), **metrics(a)}, "integrated": {"n": len(b), **metrics(b)}}
    assignments = [{"id": r["id"], "original_score": r["score"], **adjusted_score(r["score"], r["direction"], r["predicted_loss"], w)} for r in rows]
    unsupported = [r for r in rows if r["direction"] != "bullish"]
    assert all(adjusted_score(r["score"], r["direction"], r["predicted_loss"], w)["score"] == r["score"] for r in unsupported)
    report = {"evaluated_at_utc": datetime.now(timezone.utc).isoformat(), "weight": w,
              "status": "exploratory_only_insufficient_independent_dates", "live_score_changed": False,
              "selection_status": selected["status"], "comparisons": sections, "thresholds": thresholds,
              "unchanged_nonbullish": len(unsupported), "adjustment_status": dict(Counter(r["status"] for r in assignments)),
              "changed_scores": sum(r["score"] != r["original_score"] for r in assignments),
              "selection_sha256": sha(OUT / "selection.json"), "preparation": read(OUT / "preparation.json")["coverage"]}
    save_new(OUT / "score-assignments.json", assignments)
    save_new(OUT / "results.json", report)
    save_new(OUT / "manifest.json", {"sha256": {str(p): sha(p) for p in OUT.glob("*.json")},
                                    "protocol_sha256": sha(PROTOCOL)})
    print(json.dumps({"weight": w, "primary": sections["evaluation_issuers"], "status": report["status"]}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "calibrate", "evaluate"])
    args = parser.parse_args()
    {"prepare": prepare, "calibrate": calibrate, "evaluate": evaluate}[args.action]()
