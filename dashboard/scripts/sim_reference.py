"""Python reference results for the dashboard's JS simulations (same samples, same parameters).

Writes dashboard/scripts/sim_reference.json; scripts/verify_sim.mjs runs the JS algorithms
(src/sim/algorithms.js) on the same samples and compares.

Usage (repository root):  python dashboard/scripts/sim_reference.py
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

DASH = Path(__file__).resolve().parents[1]
DATA = DASH / "src" / "data"
FEATURES = ["trip_distance", "pickup_hour", "pickup_dow", "PULocationID", "DOLocationID",
            "extra", "mta_tax", "tolls_amount", "improvement_surcharge", "cbd_congestion_fee"]
LEAKY = ["tolls_amount", "cbd_congestion_fee"]
DEFAULTS = dict(maxPassengers=6, maxMiles=100, minDuration=1, maxDuration=180, maxSpeed=80)
CLEAN_CASES = [
    {"name": "varsayılan", "params": {}, "includeFlex": True},
    {"name": "sıkı eşik", "params": {"maxMiles": 30, "maxDuration": 120, "maxSpeed": 50}, "includeFlex": True},
    {"name": "Flex Fare hariç", "params": {}, "includeFlex": False},
]
GBT_CASES = [{"nTrees": 30, "maxDepth": 3, "learningRate": 0.1, "minLeaf": 20, "includeLeaky": True},
             {"nTrees": 20, "maxDepth": 2, "learningRate": 0.2, "minLeaf": 20, "includeLeaky": False}]


def load(name: str) -> pd.DataFrame:
    doc = json.loads((DATA / "samples" / f"{name}.json").read_text(encoding="utf-8"))
    return pd.DataFrame(doc["data"], columns=doc["columns"])


def rules(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    d = df["trip_duration_min"]
    pc = df["passenger_count"]
    return pd.DataFrame({
        "R1": pc.notna() & ((pc == 0) | (pc > p["maxPassengers"])),
        "R2": (df["trip_distance"] <= 0) | (df["trip_distance"] > p["maxMiles"]),
        "R3": (df["fare_amount"] <= 0) | (df["total_amount"] <= 0),
        "R4": (d < 0) | ((d >= 0) & (d <= p["minDuration"])) | (d > p["maxDuration"]),
        "R5": df["avg_speed_mph"].notna() & (df["avg_speed_mph"] > p["maxSpeed"]),
        "R6": (df["tpep_pickup_datetime"] < "2026-01-01 00:00:00") | (df["tpep_pickup_datetime"] >= "2026-06-01 00:00:00"),
        "R7": df["RatecodeID"] == 99,
        "R8": df["PULocationID"].isin([264, 265]) | df["DOLocationID"].isin([264, 265]),
        "R10": df["cbd_congestion_fee"].notna() & (df["cbd_congestion_fee"] < 0),
    })


def cleaning(raw: pd.DataFrame) -> list[dict]:
    out = []
    for case in CLEAN_CASES:
        p = {**DEFAULTS, **case["params"]}
        data = raw if case["includeFlex"] else raw[raw["payment_type"] != 0]
        f = rules(data, p)
        cols = list(f.columns)
        per = []
        for k, c in enumerate(cols):
            earlier = f[cols[:k]].any(axis=1) if k else pd.Series(False, index=f.index)
            others = f[[x for x in cols if x != c]].any(axis=1)
            per.append({"rule": c, "isolated": int(f[c].sum()), "sequential": int((f[c] & ~earlier).sum()),
                        "unique": int((f[c] & ~others).sum())})
        out.append({**case, "rows": len(data), "removed": int(f.any(axis=1).sum()), "rules": per})
    return out


def model_data(clean: pd.DataFrame, include_leaky: bool):
    feats = [f for f in FEATURES if include_leaky or f not in LEAKY]
    tr, te = clean[clean["is_train"]], clean[~clean["is_train"]]
    return (tr[feats].fillna(0).to_numpy(), tr["trip_duration_min"].to_numpy(),
            te[feats].fillna(0).to_numpy(), te["trip_duration_min"].to_numpy())


def scores(y, p) -> dict:
    return {"rmse": float(np.sqrt(mean_squared_error(y, p))), "mae": float(mean_absolute_error(y, p)),
            "r2": float(r2_score(y, p))}


def main() -> None:
    raw, clean = load("raw_sample"), load("clean_sample")
    zones = pd.DataFrame(json.loads((DATA / "zones.json").read_text(encoding="utf-8")))
    ref = {"cleaning": cleaning(raw)}

    ref["hourly"] = {int(k): int(v) for k, v in clean.groupby("pickup_hour").size().items()}
    daily = clean.groupby(clean["tpep_pickup_datetime"].str[:10]).size().sort_index()
    avg7 = daily.rolling(7).mean()
    ref["daily"] = [{"date": d, "trips": int(v), "avg7": None if pd.isna(a) else float(a)}
                    for (d, v), a in zip(daily.items(), avg7)]
    top = (clean.sort_values(["PULocationID", "total_amount"], ascending=[True, False], kind="stable")
           .groupby("PULocationID").head(3))
    ref["top3"] = {"rows": len(top), "sum": float(top["total_amount"].sum())}
    j = clean.merge(zones[["LocationID", "Borough"]], left_on="PULocationID", right_on="LocationID")
    ref["borough"] = {b: {"trips": len(g), "avgTotal": float(g["total_amount"].mean())}
                      for b, g in j.groupby("Borough")}

    Xtr, ytr, Xte, yte = model_data(clean, True)
    ols = LinearRegression().fit(Xtr, ytr)
    ref["ols"] = {**scores(yte, ols.predict(Xte)), "intercept": float(ols.intercept_),
                  "coef": [float(c) for c in ols.coef_]}
    ref["gbt"] = []
    for case in GBT_CASES:
        Xtr, ytr, Xte, yte = model_data(clean, case["includeLeaky"])
        m = GradientBoostingRegressor(n_estimators=case["nTrees"], max_depth=case["maxDepth"],
                                      learning_rate=case["learningRate"], min_samples_leaf=case["minLeaf"],
                                      criterion="squared_error", random_state=0).fit(Xtr, ytr)
        ref["gbt"].append({**case, **scores(yte, m.predict(Xte))})
    (DASH / "scripts" / "sim_reference.json").write_text(json.dumps(ref, ensure_ascii=False, indent=1), encoding="utf-8")
    print("referans yazıldı:", {k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in ref.items()})


if __name__ == "__main__":
    main()
