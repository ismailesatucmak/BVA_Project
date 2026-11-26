"""Birim testleri — src/ml_analysis.py ve ml_duration.merge_results (Öğrenci 4 ekleri)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src import ml_analysis as ma
from src import ml_duration as ml


def _sample(n: int = 2_000, seed: int = 0) -> pd.DataFrame:
    """ml_duration örneklemiyle aynı kolonlara sahip sentetik tablo."""
    rng = np.random.default_rng(seed)
    dist = rng.uniform(0.3, 15, n)
    pu = rng.integers(1, 8, n)
    do = rng.integers(1, 8, n)
    hour = rng.integers(0, 24, n)
    df = pd.DataFrame({
        ml.TARGET_COL: 3 + 3.5 * dist + np.where(pu == 3, 6, 0) + rng.normal(0, 1, n),
        "trip_distance": dist, "pickup_hour": hour, "pickup_dow": rng.integers(1, 8, n),
        "extra": 0.0, "mta_tax": 0.5, "tolls_amount": np.where(dist > 10, 6.94, 0.0),
        "improvement_surcharge": 1.0, "cbd_congestion_fee": 0.0,
        "PULocationID": pu, "DOLocationID": do,
        "PU_code": pu - 1, "DO_code": do - 1,
    })
    df[ml.SPLIT_COL] = rng.uniform(size=n) < ml.TRAIN_RATIO
    return df


def test_feature_sets_drop_end_of_trip_fees():
    assert set(ma.FEATURE_SETS["tam"]) == set(ml.NUMERIC_FEATURES)
    for fs in ("binis_ani", "bolge_zaman"):
        assert "tolls_amount" not in ma.FEATURE_SETS[fs]
        assert "cbd_congestion_fee" not in ma.FEATURE_SETS[fs]
    assert "trip_distance" not in ma.FEATURE_SETS["bolge_zaman"]


def test_validation_mask_is_deterministic_and_about_20pct():
    train = _sample(5_000)
    a, b = ma.validation_mask(train), ma.validation_mask(train)
    assert (a == b).all()
    assert 0.17 < a.mean() < 0.23


def test_grid_contains_main_experiment_setting():
    default = {"max_iter": ml.GBT_MAX_ITER, "max_depth": ml.GBT_MAX_DEPTH,
               "learning_rate": ml.GBT_LEARNING_RATE}
    assert default in ma.grid()
    assert len(ma.grid()) == np.prod([len(v) for v in ma.PARAM_GRID.values()])


def test_ablation_full_beats_zone_time_only():
    res = ma.run_ablation(_sample()).set_index("feature_set")
    assert res.loc["tam", "rmse"] < res.loc["bolge_zaman", "rmse"]
    assert list(res["n_features"]) == [10, 5, 4]


def test_tuning_never_selects_on_test(monkeypatch):
    monkeypatch.setattr(ma, "PARAM_GRID", {"max_iter": [5, 20], "max_depth": [3],
                                           "learning_rate": [0.1]})
    df = _sample(3_000)
    seen = []
    real = ma._fit_eval

    def spy(model, X_fit, y_fit, X_eval, y_eval):
        seen.append(len(X_eval))
        return real(model, X_fit, y_fit, X_eval, y_eval)

    monkeypatch.setattr(ma, "_fit_eval", spy)
    res, best, pred = ma.run_tuning(df, df)
    n_train = int(df[ml.SPLIT_COL].sum())
    n_val = int(ma.validation_mask(df[df[ml.SPLIT_COL]]).sum())
    assert seen[:2] == [n_val, n_val]  # arama yalnızca doğrulama payında
    assert len(pred) == len(df) - n_train
    assert set(res["stage"]) == {"validation", "test_default", "test_tuned"}
    assert best["max_iter"] == 20


def test_error_table_groups_and_metrics():
    test = pd.DataFrame({ml.TARGET_COL: [10.0, 20.0, 30.0], "trip_distance": [0.5, 3.0, 25.0],
                         "pickup_hour": [8, 8, 9], "PULocationID": [1, 2, 2]})
    zones = pd.DataFrame({"LocationID": [1, 2], "Borough": ["EWR", "Queens"]})
    out = ma.error_table(test, np.array([12.0, 20.0, 27.0]), zones)
    hour8 = out[(out["dimension"] == "pickup_hour") & (out["group"] == "8")].iloc[0]
    assert hour8["n"] == 2
    assert hour8["bias"] == pytest.approx(1.0)
    assert hour8["rmse"] == pytest.approx(np.sqrt(2.0), abs=1e-4)  # tablo 4 haneye yuvarlanır
    assert set(out[out["dimension"] == "distance_bin"]["group"]) == {"0–1", "2–5", "20+"}
    assert set(out[out["dimension"] == "Borough"]["group"]) == {"EWR", "Queens"}


def test_make_plots_writes_two_figures(tmp_path):
    ablation = pd.DataFrame({"feature_set": list(ma.FEATURE_SETS),
                             "label": list(ma.FEATURE_SET_LABEL.values()),
                             "rmse": [5.5, 5.6, 7.9], "r2": [0.79, 0.78, 0.55]})
    tuning = pd.DataFrame([{**g, "stage": "validation", "rmse": 5.0 + i * 0.1}
                           for i, g in enumerate(ma.grid())]
                          + [{"stage": "test_default", "rmse": 5.6},
                             {"stage": "test_tuned", "rmse": 5.3}])
    errors = pd.DataFrame({"dimension": ["distance_bin", "pickup_hour", "Borough"],
                           "group": ["0–1", "8", "Queens"], "n": [1, 1, 1],
                           "mae": [1.0, 2.0, 3.0], "bias": [0.1, -0.2, 0.3]})
    paths = ma.make_plots(ablation, tuning, errors, tmp_path)
    assert all(p.exists() and p.stat().st_size > 0 for p in paths)


def test_merge_results_replaces_only_rerun_size():
    old = [{"framework": fw, "model": name, "size_label": lbl, "rmse": "old"}
           for lbl in ("100K", "1M") for fw, _, name in ml.EXPERIMENTS]
    new = [{"framework": "PySpark", "model": "GBTRegressor", "size_label": "1M", "rmse": "new"},
           {"framework": "PySpark", "model": "GBTRegressor", "size_label": "17M", "rmse": "new"}]
    merged = ml.merge_results(old, new)
    assert len(merged) == len(old) + 1
    assert [r["size_label"] for r in merged][-1] == "17M"
    changed = [r for r in merged if r["rmse"] == "new"]
    assert {r["size_label"] for r in changed} == {"1M", "17M"}
