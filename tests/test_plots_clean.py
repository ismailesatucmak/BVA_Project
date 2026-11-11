import matplotlib

matplotlib.use("Agg")

import numpy as np
import pandas as pd

from src import plots_clean as pc


def _impact():
    rows = [{"rule": r, "description": f"kural {r}", "isolated": 100 * i, "sequential": 80 * i,
             "sequential_pct": 0.1 * i} for i, r in enumerate(pc.RULE_ORDER, start=1)]
    rows += [{"rule": "TOTAL", "description": "", "isolated": None, "sequential": 500, "sequential_pct": 5.0},
             {"rule": "NAIVE", "description": "", "isolated": None, "sequential": 2500, "sequential_pct": 25.0}]
    return pd.DataFrame(rows)


def _sample(n=500, with_context=False):
    rng = np.random.default_rng(0)
    df = pd.DataFrame({
        "trip_distance": rng.lognormal(0.5, 0.8, n),
        "fare_amount": rng.lognormal(2.7, 0.5, n),
        "trip_duration_min": rng.lognormal(2.5, 0.5, n),
    })
    if with_context:
        df["ctx_fare"] = rng.random(n) < 0.05
    return df


def test_rule_impact_renders(tmp_path):
    assert pc.plot_rule_impact(_impact(), tmp_path).stat().st_size > 0


def test_before_after_renders(tmp_path):
    assert pc.plot_before_after(_sample(), _sample(), tmp_path).stat().st_size > 0


def test_fare_vs_distance_renders(tmp_path):
    assert pc.plot_fare_vs_distance(_sample(with_context=True), tmp_path).stat().st_size > 0


def test_heatmap_renders(tmp_path):
    rules = [r for r in pc.RULE_ORDER if r != "R9"]
    rates = pd.DataFrame([{"dimension": "VendorID", "value": v, "rows": 1e6, **{r: 1.0 for r in rules}}
                          for v in ("1", "2", "6", "7")])
    assert pc.plot_rule_rates_heatmap(rates, tmp_path).stat().st_size > 0


def test_rule_vs_stat_renders(tmp_path):
    comp = pd.DataFrame([{"metric": "fare_amount", "rule": "R3", "stat_method": "IQR", "rule_flagged": 10,
                          "stat_flagged": 20, "both": 5, "rule_only": 5, "stat_only": 15}])
    assert pc.plot_rule_vs_stat(comp, tmp_path).stat().st_size > 0
