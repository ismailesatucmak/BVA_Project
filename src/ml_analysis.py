"""S4 ek analizleri — özellik sızıntısı, hiperparametre araması, hata analizi (Öğrenci 4).

``ml_duration`` iki çerçeveyi aynı ayarlarla karşılaştırır. Bu modül, o deneyin örneklemleri
(``data/.../s4_samples/{1M,5M}``) üzerinde modelin kendisini inceler:

  1. **Özellik sızıntısı (ablasyon):** ``tolls_amount`` ve ``cbd_congestion_fee`` yolculuk
     sonunda kesinleşir; biniş anında bilinmez. Model üç özellik setiyle yeniden eğitilir:
       - ``tam``         : ana deneydeki 8 sayısal + 2 bölge özelliği
       - ``binis_ani``   : ücret kalemleri çıkarılmış (mesafe, saat, gün, bölgeler)
       - ``bolge_zaman`` : yalnızca bölgeler + saat + gün (mesafe de yok)
     ``trip_distance`` taksimetreden gelir; ``binis_ani`` setinde planlanan rota mesafesinin
     yerine geçtiği varsayılır (navigasyon uygulamaları biniş anında bunu bilir).
  2. **Hiperparametre araması:** doğrulama kümesi yalnızca eğitim satırlarından, satır
     içeriğinin hash'iyle ayrılır (%20). Test kümesi seçimde **hiç** kullanılmaz; en iyi ayar
     5M eğitim kümesinin tamamıyla yeniden eğitilip yalnızca bir kez test edilir.
  3. **Hata analizi:** ayarlanmış modelin 5M test hataları mesafe dilimi, biniş saati ve
     biniş ilçesine göre (TLC bölge tablosuyla join) özetlenir.

Model: scikit-learn ``HistGradientBoostingRegressor``. Aynı algoritma ailesinin Spark
karşılığı (GBTRegressor) tek makinede ~40× yavaş olduğu için arama sklearn ile yapılır
(ana deneyde RMSE farkı ≤ 0,08 dk).

Çalıştırma:
    python -m src.ml_analysis              # 3 analiz + Şekil 24–25 (~5 dk)
    python -m src.ml_analysis --plots-only
"""
from __future__ import annotations

import argparse
import logging
import time
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

from src import config
from src import ml_duration as ml

log = logging.getLogger(__name__)

FEATURE_SETS: dict[str, list[str]] = {
    "tam": ml.NUMERIC_FEATURES,
    "binis_ani": ["trip_distance", "pickup_hour", "pickup_dow"],
    "bolge_zaman": ["pickup_hour", "pickup_dow"],
}
FEATURE_SET_LABEL = {
    "tam": "Tam (8 sayısal + bölgeler)",
    "binis_ani": "Biniş anı (ücret kalemleri yok)",
    "bolge_zaman": "Yalnız bölge + zaman (mesafe yok)",
}
VALIDATION_RATIO = 0.2
TUNING_SIZE = "1M"
FINAL_SIZE = "5M"
# Arama ızgarası: ana deneydeki ayar (50 ağaç, derinlik 5, lr 0,1) ızgaranın içinde
PARAM_GRID = {
    "max_iter": [50, 200],
    "max_depth": [5, 8, None],
    "learning_rate": [0.1, 0.2],
}
NONE_DEPTH_LEAVES = 63  # max_depth=None iken ağaç büyüklüğü yaprak sayısıyla sınırlanır
DISTANCE_BINS = [0, 1, 2, 5, 10, 20, np.inf]
DISTANCE_LABELS = ["0–1", "1–2", "2–5", "5–10", "10–20", "20+"]

ABLATION_CSV = config.RESULTS_DIR / "s4_ablation.csv"
TUNING_CSV = config.RESULTS_DIR / "s4_tuning.csv"
ERROR_CSV = config.RESULTS_DIR / "s4_error_analysis.csv"


# ------------------------------------------------------------------ veri
def load_sample(size_label: str, samples_dir: Path = ml.SAMPLES_DIR) -> pd.DataFrame:
    df = pd.read_parquet(samples_dir / size_label)
    for c in ml.CATEGORICAL_FEATURES:
        df[c] = df[c].astype("int64")
    return df


def validation_mask(train: pd.DataFrame, ratio: float = VALIDATION_RATIO) -> np.ndarray:
    """Eğitim satırlarından deterministik doğrulama bayrağı (satır içeriğinin hash'i)."""
    cols = [ml.TARGET_COL] + ml.NUMERIC_FEATURES + ml.LOCATION_COLS
    h = pd.util.hash_pandas_object(train[cols], index=False).to_numpy()
    return (h % 1_000_000) < ratio * 1_000_000


def make_model(feature_set: str, max_iter=ml.GBT_MAX_ITER, max_depth=ml.GBT_MAX_DEPTH,
               learning_rate=ml.GBT_LEARNING_RATE):
    from sklearn.ensemble import HistGradientBoostingRegressor

    n_num = len(FEATURE_SETS[feature_set])
    return HistGradientBoostingRegressor(
        max_iter=max_iter, max_depth=max_depth, learning_rate=learning_rate,
        min_samples_leaf=ml.GBT_MIN_LEAF,
        max_leaf_nodes=NONE_DEPTH_LEAVES if max_depth is None else None,
        max_bins=ml.N_LOCATION_CODES,
        categorical_features=[n_num + i for i in range(len(ml.CATEGORICAL_FEATURES))],
        early_stopping=False, random_state=ml.SAMPLE_SEED,
    )


def features(df: pd.DataFrame, feature_set: str) -> pd.DataFrame:
    return df[FEATURE_SETS[feature_set] + ml.CATEGORICAL_FEATURES]


def _fit_eval(model, X_fit, y_fit, X_eval, y_eval) -> tuple[dict, float, np.ndarray]:
    t0 = time.perf_counter()
    model.fit(X_fit, y_fit)
    seconds = time.perf_counter() - t0
    pred = model.predict(X_eval)
    return ml.sklearn_metrics(y_eval, pred), seconds, pred


# ------------------------------------------------------------------ 1) ablasyon
def run_ablation(df: pd.DataFrame, size_label: str = FINAL_SIZE) -> pd.DataFrame:
    is_train = df[ml.SPLIT_COL].to_numpy(dtype=bool)
    y = df[ml.TARGET_COL].to_numpy()
    rows = []
    for fs in FEATURE_SETS:
        X = features(df, fs)
        m, sec, _ = _fit_eval(make_model(fs), X[is_train], y[is_train], X[~is_train], y[~is_train])
        rows.append({"feature_set": fs, "label": FEATURE_SET_LABEL[fs], "size_label": size_label,
                     "n_features": X.shape[1], **{k: round(v, 4) for k, v in m.items()},
                     "train_time_s": round(sec, 2)})
        log.info("  ablasyon %-12s RMSE=%.3f R²=%.4f", fs, m["rmse"], m["r2"])
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ 2) arama
def grid() -> list[dict]:
    keys = list(PARAM_GRID)
    return [dict(zip(keys, vals)) for vals in product(*PARAM_GRID.values())]


def run_tuning(tune_df: pd.DataFrame, final_df: pd.DataFrame, feature_set: str = "binis_ani"
               ) -> tuple[pd.DataFrame, dict, np.ndarray]:
    """Izgarayı 1M eğitiminin doğrulama payında seçer; en iyisini 5M'de bir kez test eder."""
    train = tune_df[tune_df[ml.SPLIT_COL]]
    val = validation_mask(train)
    X, y = features(train, feature_set), train[ml.TARGET_COL].to_numpy()
    rows = []
    for params in grid():
        m, sec, _ = _fit_eval(make_model(feature_set, **params), X[~val], y[~val], X[val], y[val])
        rows.append({**params, "stage": "validation", "size_label": TUNING_SIZE,
                     "rmse": round(m["rmse"], 4), "mae": round(m["mae"], 4),
                     "r2": round(m["r2"], 4), "train_time_s": round(sec, 2)})
        log.info("  arama %s val RMSE=%.3f (%.1f sn)", params, m["rmse"], sec)
    res = pd.DataFrame(rows)
    best = res.sort_values("rmse").iloc[0]
    best_params = {
        "max_iter": int(best["max_iter"]),
        "max_depth": None if pd.isna(best["max_depth"]) else int(best["max_depth"]),
        "learning_rate": float(best["learning_rate"]),
    }

    is_train = final_df[ml.SPLIT_COL].to_numpy(dtype=bool)
    Xf, yf = features(final_df, feature_set), final_df[ml.TARGET_COL].to_numpy()
    final_rows = []
    for stage, params in (("test_default", {}), ("test_tuned", best_params)):
        m, sec, pred = _fit_eval(make_model(feature_set, **params), Xf[is_train], yf[is_train],
                                 Xf[~is_train], yf[~is_train])
        full = {"max_iter": ml.GBT_MAX_ITER, "max_depth": ml.GBT_MAX_DEPTH,
                "learning_rate": ml.GBT_LEARNING_RATE, **params}
        final_rows.append({**full, "stage": stage, "size_label": FINAL_SIZE,
                           "rmse": round(m["rmse"], 4), "mae": round(m["mae"], 4),
                           "r2": round(m["r2"], 4), "train_time_s": round(sec, 2)})
        log.info("  %s %s RMSE=%.3f", stage, params, m["rmse"])
    tuned_pred = pred  # son döngü = ayarlanmış model
    return pd.concat([res, pd.DataFrame(final_rows)], ignore_index=True), best_params, tuned_pred


# ------------------------------------------------------------------ 3) hata analizi
def error_table(test: pd.DataFrame, pred: np.ndarray, zones: pd.DataFrame) -> pd.DataFrame:
    """Boyut (mesafe dilimi / saat / ilçe) × grup başına n, RMSE, MAE, ortalama sapma."""
    d = pd.DataFrame({
        "y": test[ml.TARGET_COL].to_numpy(), "pred": pred,
        "distance_bin": pd.cut(test["trip_distance"].to_numpy(), DISTANCE_BINS,
                               labels=DISTANCE_LABELS, right=False),
        "pickup_hour": test["pickup_hour"].to_numpy(),
        "PULocationID": test["PULocationID"].to_numpy(),
    })
    d = d.merge(zones[["LocationID", "Borough"]], left_on="PULocationID",
                right_on="LocationID", how="left")
    d["err"] = d["pred"] - d["y"]
    out = []
    for dim in ("distance_bin", "pickup_hour", "Borough"):
        g = d.groupby(dim, observed=True)
        t = pd.DataFrame({
            "n": g.size(),
            "rmse": g["err"].apply(lambda e: float(np.sqrt(np.mean(e ** 2)))),
            "mae": g["err"].apply(lambda e: float(np.mean(np.abs(e)))),
            "bias": g["err"].mean(),
            "mean_true": g["y"].mean(),
        }).reset_index().rename(columns={dim: "group"})
        t["mape_pct"] = 100 * t["mae"] / t["mean_true"]
        t.insert(0, "dimension", dim)
        t["group"] = t["group"].astype(str)
        out.append(t)
    return pd.concat(out, ignore_index=True).round(4)


# ------------------------------------------------------------------ grafikler
def make_plots(ablation: pd.DataFrame, tuning: pd.DataFrame, errors: pd.DataFrame,
               out_dir: Path = config.FIGURES_DIR) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_dir.mkdir(parents=True, exist_ok=True)
    blue, grey, ink = "#2a78d6", "#a3a29d", "#52514e"

    # Şekil 24: ablasyon + arama
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Şekil 24 · Özellik sızıntısı ve hiperparametre araması "
                 "(scikit-learn HistGradientBoosting)", fontsize=12)
    ab = ablation.iloc[::-1]
    axes[0].barh(ab["label"], ab["rmse"], color=[blue if f == "binis_ani" else grey
                                                   for f in ab["feature_set"]])
    for y, (v, r2) in enumerate(zip(ab["rmse"], ab["r2"])):
        axes[0].text(v, y, f"  {v:.2f} dk · R² {r2:.3f}".replace(".", ","), va="center",
                     fontsize=9, color=ink)
    axes[0].set_xlabel("Test RMSE (dk, 5M örneklem)")
    axes[0].set_title("Özellik seti → doğruluk")
    axes[0].set_xlim(0, ab["rmse"].max() * 1.35)

    val = tuning[tuning["stage"] == "validation"].sort_values("rmse").reset_index(drop=True)
    labels = [f"{int(r.max_iter)} ağaç · d={'∞' if pd.isna(r.max_depth) else int(r.max_depth)}"
              f" · lr={r.learning_rate:g}".replace(".", ",") for r in val.itertuples()]
    colors = [blue if i == 0 else grey for i in range(len(val))]
    axes[1].barh(labels[::-1], val["rmse"][::-1], color=colors[::-1])
    axes[1].set_xlim(val["rmse"].min() * 0.97, val["rmse"].max() * 1.01)
    axes[1].set_xlabel(f"Doğrulama RMSE (dk, {TUNING_SIZE} eğitim kümesinin %20'si)")
    t_def = tuning[tuning["stage"] == "test_default"]["rmse"].iloc[0]
    t_tun = tuning[tuning["stage"] == "test_tuned"]["rmse"].iloc[0]
    axes[1].set_title(f"Izgara araması · {FINAL_SIZE} test: varsayılan {t_def:.3f} → "
                      f"en iyi ayar {t_tun:.3f} dk".replace(".", ","), fontsize=10)
    axes[1].tick_params(axis="y", labelsize=8)
    for ax in axes:
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    fig.tight_layout()
    p24 = out_dir / "24_s4_ablation_tuning.png"
    fig.savefig(p24, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Şekil 25: hata analizi
    fig, axes = plt.subplots(1, 3, figsize=(17, 5))
    fig.suptitle("Şekil 25 · Hata analizi: ayarlanmış model, 5M test kümesi "
                 "(çubuk = MAE, nokta = ortalama sapma)", fontsize=12)
    titles = {"distance_bin": "Mesafe dilimi (mil)", "pickup_hour": "Biniş saati",
              "Borough": "Biniş ilçesi"}
    for ax, dim in zip(axes, titles):
        e = errors[errors["dimension"] == dim].copy()
        if dim == "pickup_hour":
            e = e.sort_values("group", key=lambda s: s.astype(int))
        elif dim == "Borough":
            e = e.sort_values("n", ascending=False)
        x = np.arange(len(e))
        ax.bar(x, e["mae"], color=blue, label="MAE (dk)")
        ax.plot(x, e["bias"], "o", color="#eb6834", label="Ortalama sapma (tahmin − gerçek)")
        ax.axhline(0, color=ink, linewidth=0.8)
        ax.set_xticks(x)
        ticks = (e["group"] + "\n(n=" + e["n"].map(lambda v: f"{int(v):,}".replace(",", "."))
                 + ")") if dim == "Borough" else e["group"]
        ax.set_xticklabels(ticks, rotation=45 if dim == "Borough" else 0,
                           ha="right" if dim == "Borough" else "center", fontsize=8)
        ax.set_title(titles[dim])
        ax.set_ylabel("dakika")
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    axes[0].legend(fontsize=8, frameon=False)
    fig.tight_layout()
    p25 = out_dir / "25_s4_error_analysis.png"
    fig.savefig(p25, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return [p24, p25]


# ------------------------------------------------------------------ ana akış
def run_all() -> None:
    final = load_sample(FINAL_SIZE)
    log.info("1) Özellik sızıntısı ablasyonu (%s)", FINAL_SIZE)
    ablation = run_ablation(final)
    ablation.to_csv(ABLATION_CSV, index=False)

    log.info("2) Hiperparametre araması (%s doğrulama → %s test)", TUNING_SIZE, FINAL_SIZE)
    tuning, best, pred = run_tuning(load_sample(TUNING_SIZE), final)
    tuning.to_csv(TUNING_CSV, index=False)
    log.info("   en iyi ayar: %s", best)

    log.info("3) Hata analizi")
    test = final[~final[ml.SPLIT_COL].to_numpy(dtype=bool)]
    zones = pd.read_csv(config.ZONE_LOOKUP_PATH, keep_default_na=False)
    error_table(test, pred, zones).to_csv(ERROR_CSV, index=False)


def load_saved() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (pd.read_csv(ABLATION_CSV), pd.read_csv(TUNING_CSV),
            pd.read_csv(ERROR_CSV, keep_default_na=False))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="S4 ek analizleri")
    parser.add_argument("--plots-only", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [S4+] %(message)s")
    if not args.plots_only:
        run_all()
    for p in make_plots(*load_saved()):
        log.info("kaydedildi: %s", p)


if __name__ == "__main__":
    main()
