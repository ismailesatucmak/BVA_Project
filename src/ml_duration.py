"""NYC Taxi yolculuk süresi tahmini — PySpark MLlib vs scikit-learn (Öğrenci 4).

Deney tasarımı (adil karşılaştırma):
  * Örneklem: temiz veriden 100K / 1M / 5M kayıt, S1'in xxhash64 anahtarıyla
    (``build_sizes.with_sample_key``) → makineden bağımsız ve iç içe (100K ⊂ 1M ⊂ 5M).
  * Train/test ayrımı da satır hash'iyle yapılır (%80 / %20) ve örneklemle birlikte
    diske yazılır → PySpark ve scikit-learn **aynı satırlarda** eğitilir ve test edilir.
  * Özellikler iki tarafta aynıdır: 8 sayısal kolon + biniş/iniş bölgesi.
    Bölge kodlaması yalnızca eğitim kümesinden öğrenilir: en sık 254 bölge 0..253,
    geri kalan / görülmemiş bölgeler 254 ("diğer"). 255 kod, scikit-learn
    HistGradientBoosting'in kategorik sınırına (≤ 255) uyar.
      - Doğrusal modeller: bölge kodu one-hot (son kategori düşürülür) — iki tarafta aynı matris.
      - Ağaç modelleri: bölge kodu kategorik özellik (Spark: nominal metadata, maxBins=256;
        sklearn: ``categorical_features``).
  * Hiperparametreler eşittir: OLS (düzenlileştirme yok); GBT 50 ağaç, derinlik 5,
    öğrenme oranı 0,1, yaprakta en az 20 kayıt, erken durdurma yok.
  * Her (çerçeve × boyut × model) ayrı bir alt süreçte ölçülür. Bellek, süreç ağacının
    (Python + Spark JVM + Python işçileri) tepe RSS'idir (``src.mem_probe.PeakRSS``),
    eğitim sırasında 50 ms aralıkla örneklenir. Veri yükleme süreye dahil değildir.

Kural: Spark oturumu her zaman get_spark() ile açılır.
Kural: Türetilmiş kolonlar (trip_duration_min, pickup_hour, pickup_dow) quality.py'den gelir.
Kural: Flex Fare kayıtlarında null olan kolonlar (passenger_count, RatecodeID vb.) feature olmaz.
Kural: fare_amount, total_amount, tip_amount, tpep_dropoff_datetime data leakage nedeniyle dışlanır.

Çalıştırma:
    python -m src.ml_duration                 # örneklemler + 12 ölçüm + grafikler
    python -m src.ml_duration --sizes 100K 1M # yalnızca seçilen boyutlar
    python -m src.ml_duration --plots-only    # kayıtlı CSV'lerden grafikler (Spark gerekmez)
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

from src.config import CLEAN_DIR, EXTERNAL_DATA_ROOT, FIGURES_DIR, RESULTS_DIR

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Sabitler
# ------------------------------------------------------------------
SAMPLE_SIZES = [100_000, 1_000_000, 5_000_000]
SAMPLE_LABELS = ["100K", "1M", "5M"]
SAMPLE_SEED = 42
TARGET_COL = "trip_duration_min"
TRAIN_RATIO = 0.8
TEST_RATIO = 0.2
SPLIT_COL = "is_train"

# Features: null içermeyen ve data leakage olmayan kolonlar
# passenger_count, RatecodeID, congestion_surcharge, Airport_fee → Flex Fare'de null, dışla
# fare_amount, total_amount, tip_amount → data leakage
# tpep_dropoff_datetime → data leakage (target'tan türetiliyor)
# Not: tolls_amount ve cbd_congestion_fee rota bilgisi taşır ve yolculuk sonunda kesinleşir;
# gerçek zamanlı (biniş anında) tahminde bilinmezler → raporda sınırlama olarak belirtilir.
NUMERIC_FEATURES = [
    "trip_distance",
    "pickup_hour",
    "pickup_dow",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "cbd_congestion_fee",
]
LOCATION_COLS = ["PULocationID", "DOLocationID"]
CATEGORICAL_FEATURES = ["PU_code", "DO_code"]  # LOCATION_COLS'un kodlanmış hâli
N_LOCATION_CODES = 255  # sklearn HistGradientBoosting kategorik sınırı
OTHER_CODE = N_LOCATION_CODES - 1  # nadir / eğitimde görülmemiş bölge

# Model hiperparametreleri (iki çerçevede aynı)
LR_MAX_ITER = 100
GBT_MAX_ITER = 50
GBT_MAX_DEPTH = 5
GBT_LEARNING_RATE = 0.1
GBT_MIN_LEAF = 20
GBT_MAX_BINS = 256  # Spark: ≥ kategori sayısı (255) olmalı

# (çerçeve, model anahtarı, rapordaki model adı)
EXPERIMENTS = [
    ("PySpark", "lr", "LinearRegression"),
    ("PySpark", "gbt", "GBTRegressor"),
    ("scikit-learn", "lr", "LinearRegression"),
    ("scikit-learn", "gbt", "HistGradientBoosting"),
]
WORKER_TIMEOUT_S = 3 * 3600
FEATURE_IMPORTANCE_SIZE = "1M"  # özellik önemi + tahmin örneği bu boyuttaki Spark GBT'den
PRED_SAMPLE_ROWS = 5_000

SAMPLES_DIR = EXTERNAL_DATA_ROOT / "s4_samples"
COMPARISON_CSV = RESULTS_DIR / "s4_comparison.csv"
IMPORTANCE_CSV = RESULTS_DIR / "s4_feature_importance.csv"
PRED_SAMPLE_CSV = RESULTS_DIR / "s4_pred_sample.csv"

RESULT_FIELDS = [
    "framework", "model", "size_label", "size_n", "n_train", "n_test",
    "rmse", "mae", "r2", "train_time_s", "predict_time_s",
    "rss_before_fit_mb", "peak_rss_mb", "peak_jvm_mb", "error",
]


def _model_name(framework: str, model_type: str) -> str:
    for fw, key, name in EXPERIMENTS:
        if fw == framework and key == model_type:
            return name
    raise ValueError(f"Bilinmeyen deney: {framework}/{model_type}")


# ------------------------------------------------------------------
# Örnekleme, ayrım ve bölge kodlaması (Spark)
# ------------------------------------------------------------------

def _sample_data(df, n: int):
    """~n kayıtlık, makineden bağımsız ve iç içe örneklem (S1'in hash anahtarı)."""
    from src.build_sizes import sample_threshold, select_size, with_sample_key

    total = df.count()
    if n >= total:
        return df
    return select_size(with_sample_key(df), sample_threshold(n, total))


def _add_split(df):
    """Satır içeriğinin hash'iyle deterministik train/test bayrağı ekler."""
    from pyspark.sql import functions as F

    row_hash = F.xxhash64(F.lit("train_test_split"), *[F.col(c) for c in df.columns])
    key = F.pmod(row_hash, F.lit(10**9)) / F.lit(10**9)
    return df.withColumn(SPLIT_COL, key < F.lit(TRAIN_RATIO))


def fit_location_codes(train_df) -> dict[str, dict[int, int]]:
    """Her bölge kolonu için eğitimdeki en sık OTHER_CODE bölgeye 0..OTHER_CODE-1 kodu verir."""
    from pyspark.sql import functions as F

    mapping = {}
    for col in LOCATION_COLS:
        rows = (
            train_df.groupBy(col).count()
            .orderBy(F.desc("count"), F.asc(col))
            .limit(OTHER_CODE)
            .collect()
        )
        mapping[col] = {int(r[col]): i for i, r in enumerate(rows)}
    return mapping


def with_nominal_metadata(df):
    """Kod kolonlarını double + nominal (255 değerli) metadata ile işaretler.

    VectorAssembler bu metadata'yı vektöre taşır: ağaç modelleri kolonu kategorik görür,
    OneHotEncoder her zaman 255 kategori (son kategori düşürülür → 254 kolon) üretir.
    """
    from pyspark.sql import functions as F

    def meta(name):
        return {"ml_attr": {"type": "nominal", "num_vals": N_LOCATION_CODES, "name": name}}

    others = [c for c in df.columns if c not in CATEGORICAL_FEATURES]
    return df.select(
        *others,
        *[F.col(c).cast("double").alias(c, metadata=meta(c)) for c in CATEGORICAL_FEATURES],
    )


def apply_location_codes(df, mapping: dict[str, dict[int, int]]):
    """Bölge ID'lerini 0..OTHER_CODE koduna çevirir (eşlenmeyen → OTHER_CODE)."""
    from pyspark.sql import functions as F

    for col, code_col in zip(LOCATION_COLS, CATEGORICAL_FEATURES):
        pairs = [F.lit(x) for kv in sorted(mapping[col].items()) for x in kv]
        if pairs:
            code = F.coalesce(F.create_map(*pairs)[F.col(col)], F.lit(OTHER_CODE))
        else:
            code = F.lit(OTHER_CODE)
        df = df.withColumn(code_col, code.cast("int"))
    return with_nominal_metadata(df)


def prepare_sample(df_clean, size_label: str, size_n: int, force: bool = False) -> Path:
    """Örneklemi üretir, ayırır, bölgeleri kodlar ve tek Parquet dosyası olarak yazar."""
    out_dir = SAMPLES_DIR / size_label
    if (out_dir / "_SUCCESS").exists() and not force:
        log.info("  örneklem mevcut: %s", out_dir)
        return out_dir

    sample = _sample_data(df_clean, size_n)
    sample = _add_split(sample).select(
        [TARGET_COL] + NUMERIC_FEATURES + LOCATION_COLS + [SPLIT_COL]
    )
    sample.cache()
    mapping = fit_location_codes(sample.filter(sample[SPLIT_COL]))
    coded = apply_location_codes(sample, mapping)
    coded.coalesce(1).write.mode("overwrite").parquet(str(out_dir))
    sample.unpersist()
    return out_dir


# ------------------------------------------------------------------
# PySpark pipeline
# ------------------------------------------------------------------

def _build_pipeline(model_type: str = "lr"):
    """LinearRegression (lr) veya GBTRegressor (gbt) pipeline'ı.

    Girdi: NUMERIC_FEATURES + CATEGORICAL_FEATURES (nominal metadata'lı, bkz.
    ``apply_location_codes``).
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder, VectorAssembler
    from pyspark.ml.regression import GBTRegressor, LinearRegression

    if model_type == "lr":
        ohe_cols = [f"{c}_ohe" for c in CATEGORICAL_FEATURES]
        encoder = OneHotEncoder(inputCols=CATEGORICAL_FEATURES, outputCols=ohe_cols, dropLast=True)
        assembler = VectorAssembler(inputCols=NUMERIC_FEATURES + ohe_cols, outputCol="features")
        model = LinearRegression(
            featuresCol="features", labelCol=TARGET_COL, predictionCol="prediction",
            maxIter=LR_MAX_ITER, regParam=0.0,
        )
        return Pipeline(stages=[encoder, assembler, model])
    if model_type == "gbt":
        assembler = VectorAssembler(
            inputCols=NUMERIC_FEATURES + CATEGORICAL_FEATURES, outputCol="features"
        )
        model = GBTRegressor(
            featuresCol="features", labelCol=TARGET_COL, predictionCol="prediction",
            maxIter=GBT_MAX_ITER, maxDepth=GBT_MAX_DEPTH, stepSize=GBT_LEARNING_RATE,
            minInstancesPerNode=GBT_MIN_LEAF, maxBins=GBT_MAX_BINS, seed=SAMPLE_SEED,
        )
        return Pipeline(stages=[assembler, model])
    raise ValueError(f"Bilinmeyen model tipi: {model_type}")


def _evaluate(predictions) -> dict[str, float]:
    """RMSE, MAE, R² (Spark)."""
    from pyspark.ml.evaluation import RegressionEvaluator

    return {
        m: RegressionEvaluator(
            labelCol=TARGET_COL, predictionCol="prediction", metricName=m
        ).evaluate(predictions)
        for m in ("rmse", "mae", "r2")
    }


def feature_names() -> list[str]:
    """GBT özellik vektöründeki sıra: sayısallar + bölge kolonları (kategorik)."""
    return NUMERIC_FEATURES + LOCATION_COLS


# ------------------------------------------------------------------
# scikit-learn modelleri (aynı özellikler, aynı hiperparametreler)
# ------------------------------------------------------------------

def build_sklearn_model(model_type: str):
    """scikit-learn karşılığı: lr → one-hot + OLS, gbt → HistGradientBoosting (kategorik)."""
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    if model_type == "lr":
        codes = list(range(N_LOCATION_CODES))
        encoder = OneHotEncoder(
            categories=[codes] * len(CATEGORICAL_FEATURES),
            drop=[OTHER_CODE] * len(CATEGORICAL_FEATURES),  # Spark dropLast ile aynı
            sparse_output=True,
        )
        pre = ColumnTransformer(
            [("num", "passthrough", NUMERIC_FEATURES), ("loc", encoder, CATEGORICAL_FEATURES)]
        )
        return Pipeline([("pre", pre), ("model", LinearRegression())])
    if model_type == "gbt":
        n_num = len(NUMERIC_FEATURES)
        return HistGradientBoostingRegressor(
            max_iter=GBT_MAX_ITER,
            max_depth=GBT_MAX_DEPTH,
            learning_rate=GBT_LEARNING_RATE,
            min_samples_leaf=GBT_MIN_LEAF,
            max_leaf_nodes=None,
            max_bins=N_LOCATION_CODES,
            categorical_features=[n_num + i for i in range(len(CATEGORICAL_FEATURES))],
            early_stopping=False,
            random_state=SAMPLE_SEED,
        )
    raise ValueError(f"Bilinmeyen model tipi: {model_type}")


def sklearn_metrics(y_true, y_pred) -> dict[str, float]:
    import numpy as np
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


# ------------------------------------------------------------------
# Alt süreç işçileri (tek ölçüm = tek süreç)
# ------------------------------------------------------------------

def _result_row(framework, model_type, size_label, n_train, n_test, metrics,
                train_time, predict_time, probe) -> dict:
    return {
        "framework": framework,
        "model": _model_name(framework, model_type),
        "size_label": size_label,
        "size_n": n_train + n_test,
        "n_train": n_train,
        "n_test": n_test,
        "rmse": round(metrics["rmse"], 4),
        "mae": round(metrics["mae"], 4),
        "r2": round(metrics["r2"], 4),
        "train_time_s": round(train_time, 3),
        "predict_time_s": round(predict_time, 3),
        "rss_before_fit_mb": round(probe.start_total_mb, 1),
        "peak_rss_mb": round(probe.peak_total_mb, 1),
        "peak_jvm_mb": round(probe.peak_jvm_mb, 1) if framework == "PySpark" else "",
        "error": "",
    }


def run_spark_worker(sample_dir: Path, size_label: str, model_type: str) -> dict:
    from pyspark.sql import functions as F

    from src.mem_probe import PeakRSS
    from src.spark_session import get_spark

    spark = get_spark(app_name=f"s4-{model_type}-{size_label}")
    try:
        df = with_nominal_metadata(spark.read.parquet(str(sample_dir)))
        train = df.filter(F.col(SPLIT_COL)).drop(SPLIT_COL).cache()
        test = df.filter(~F.col(SPLIT_COL)).drop(SPLIT_COL).cache()
        n_train, n_test = train.count(), test.count()  # veri yükleme ölçüme dahil değil

        pipeline = _build_pipeline(model_type)
        with PeakRSS() as probe:
            t0 = time.perf_counter()
            fitted = pipeline.fit(train)
            train_time = time.perf_counter() - t0

            t1 = time.perf_counter()
            preds = fitted.transform(test).cache()
            preds.count()
            predict_time = time.perf_counter() - t1

        metrics = _evaluate(preds)
        row = _result_row("PySpark", model_type, size_label, n_train, n_test, metrics,
                          train_time, predict_time, probe)

        if model_type == "gbt" and size_label == FEATURE_IMPORTANCE_SIZE:
            _save_importance(fitted.stages[-1].featureImportances.toArray())
            # deterministik, sıralamadan bağımsız örnek (hash sırasıyla ilk N test kaydı)
            pdf = (
                preds.select(TARGET_COL, "prediction")
                .withColumn("_k", F.xxhash64(TARGET_COL, "prediction"))
                .orderBy("_k")
                .limit(PRED_SAMPLE_ROWS)
                .drop("_k")
                .toPandas()
            )
            pdf.to_csv(PRED_SAMPLE_CSV, index=False)
        return row
    finally:
        spark.stop()


def run_sklearn_worker(sample_dir: Path, size_label: str, model_type: str) -> dict:
    import pandas as pd

    from src.mem_probe import PeakRSS

    df = pd.read_parquet(sample_dir)
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].astype("int64")
    is_train = df.pop(SPLIT_COL).to_numpy(dtype=bool)
    y = df.pop(TARGET_COL).to_numpy()
    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    X_train, X_test, y_train, y_test = X[is_train], X[~is_train], y[is_train], y[~is_train]
    del df, X  # yalnızca eğitim/test kopyaları kalsın

    model = build_sklearn_model(model_type)
    with PeakRSS() as probe:
        t0 = time.perf_counter()
        model.fit(X_train, y_train)
        train_time = time.perf_counter() - t0

        t1 = time.perf_counter()
        y_pred = model.predict(X_test)
        predict_time = time.perf_counter() - t1

    return _result_row("scikit-learn", model_type, size_label, len(y_train), len(y_test),
                       sklearn_metrics(y_test, y_pred), train_time, predict_time, probe)


def _save_importance(importances) -> None:
    rows = sorted(zip(feature_names(), importances), key=lambda kv: -kv[1])
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(IMPORTANCE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["feature", "importance"])
        for name, imp in rows:
            writer.writerow([name, round(float(imp), 6)])


def _run_worker_subprocess(framework: str, model_type: str, size_label: str,
                           sample_dir: Path, out_json: Path) -> dict:
    cmd = [
        sys.executable, "-m", "src.ml_duration", "--worker",
        "spark" if framework == "PySpark" else "sklearn",
        "--model", model_type, "--size", size_label,
        "--sample-dir", str(sample_dir), "--out", str(out_json),
    ]
    out_json.unlink(missing_ok=True)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=WORKER_TIMEOUT_S,
                              cwd=Path(__file__).resolve().parent.parent)
        if proc.returncode == 0 and out_json.exists():
            return json.loads(out_json.read_text(encoding="utf-8"))
        tail = (proc.stderr or "").strip().splitlines()
        error = tail[-1] if tail else f"exit {proc.returncode}"
    except subprocess.TimeoutExpired:
        error = f"zaman aşımı > {WORKER_TIMEOUT_S} sn"
    log.warning("  %s/%s/%s başarısız: %s", framework, model_type, size_label, error)
    row = dict.fromkeys(RESULT_FIELDS, "")
    row.update(framework=framework, model=_model_name(framework, model_type),
               size_label=size_label, error=error[:300])
    return row


# ------------------------------------------------------------------
# Orkestrasyon
# ------------------------------------------------------------------

def save_results(rows: list[dict]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(COMPARISON_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    log.info("Karşılaştırma tablosu: %s", COMPARISON_CSV)


def run_all(size_labels: list[str], force_samples: bool = False) -> list[dict]:
    from src.spark_session import get_spark

    sizes = [(lbl, n) for lbl, n in zip(SAMPLE_LABELS, SAMPLE_SIZES) if lbl in size_labels]

    # 1) Örneklemleri hazırla (tek Spark oturumu), sonra oturumu kapat → ölçümlere RAM kalsın
    spark = get_spark(app_name="s4-prepare")
    try:
        df_clean = spark.read.parquet(str(CLEAN_DIR))
        sample_dirs = {}
        for lbl, n in sizes:
            log.info("Örneklem hazırlanıyor: %s", lbl)
            sample_dirs[lbl] = prepare_sample(df_clean, lbl, n, force=force_samples)
    finally:
        spark.stop()

    # 2) Her ölçüm ayrı süreçte
    rows = []
    tmp_dir = SAMPLES_DIR / "_worker_out"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    for lbl, _ in sizes:
        for framework, model_type, name in EXPERIMENTS:
            log.info("Ölçüm: %s / %s / %s", framework, name, lbl)
            out_json = tmp_dir / f"{framework}_{model_type}_{lbl}.json"
            row = _run_worker_subprocess(framework, model_type, lbl, sample_dirs[lbl], out_json)
            if not row["error"]:
                log.info("  RMSE=%.3f R²=%.4f eğitim=%.2f sn tepe RSS=%.0f MB",
                         row["rmse"], row["r2"], row["train_time_s"], row["peak_rss_mb"])
            rows.append(row)
    return rows


# ------------------------------------------------------------------
# Grafikler
# ------------------------------------------------------------------

SERIES_STYLE = {
    ("PySpark", "LinearRegression"): ("#2a78d6", "o", "--"),
    ("PySpark", "GBTRegressor"): ("#2a78d6", "s", "-"),
    ("scikit-learn", "LinearRegression"): ("#eb6834", "o", "--"),
    ("scikit-learn", "HistGradientBoosting"): ("#eb6834", "s", "-"),
}


def make_plots(comparison, importance, pred_sample, out_dir: Path = FIGURES_DIR) -> None:
    """Şekil 18–20. Girdi: s4_comparison / s4_feature_importance / s4_pred_sample DataFrame'leri."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    out_dir.mkdir(parents=True, exist_ok=True)
    ok = comparison[comparison["error"].fillna("") == ""].copy()
    for c in ("size_n", "train_time_s", "peak_rss_mb", "rmse", "r2"):
        ok[c] = pd.to_numeric(ok[c])

    def series(ax, ycol, logy=False):
        for (fw, model), (color, marker, ls) in SERIES_STYLE.items():
            sub = ok[(ok["framework"] == fw) & (ok["model"] == model)].sort_values("size_n")
            if sub.empty:
                continue
            ax.plot(sub["size_n"], sub[ycol], color=color, marker=marker, linestyle=ls,
                    linewidth=2, markersize=7, label=f"{fw} · {model}")
        ax.set_xscale("log")
        if logy:
            ax.set_yscale("log")
        ax.set_xlabel("Örneklem boyutu (kayıt, log)")
        ax.grid(True, which="both", alpha=0.3)

    # Şekil 18: eğitim süresi + tepe bellek
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Şekil 18 · PySpark vs scikit-learn: eğitim süresi ve tepe bellek "
                 "(aynı örneklem, aynı özellikler)", fontsize=12)
    series(axes[0], "train_time_s", logy=True)
    axes[0].set_ylabel("Eğitim süresi (sn, log)")
    axes[0].set_title("Eğitim süresi")
    series(axes[1], "peak_rss_mb")
    axes[1].set_ylabel("Tepe RSS (MB) — Spark: Python + JVM")
    axes[1].set_title("Eğitim sırasında süreç ağacının tepe belleği")
    axes[0].legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "18_s4_scalability.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Şekil 19: doğruluk
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Şekil 19 · Test kümesinde doğruluk: RMSE ve R²", fontsize=12)
    series(axes[0], "rmse")
    axes[0].set_ylabel("RMSE (dakika)")
    axes[0].set_title("RMSE (düşük = iyi)")
    series(axes[1], "r2")
    axes[1].set_ylabel("R²")
    axes[1].set_title("R² (yüksek = iyi)")
    axes[1].legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "19_s4_model_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Şekil 20: özellik önemi + gerçek vs tahmin
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(f"Şekil 20 · PySpark GBTRegressor ({FEATURE_IMPORTANCE_SIZE}): "
                 "özellik önemi ve gerçek vs tahmin", fontsize=12)
    imp = importance.sort_values("importance")
    axes[0].barh(imp["feature"], imp["importance"], color="#2a78d6")
    axes[0].set_xlabel("Önem (toplam = 1)")
    axes[0].set_title("Özellik önemi (her bölge kolonu tek kategorik özellik)")
    if pred_sample is not None and len(pred_sample):
        y_true = pred_sample[TARGET_COL].to_numpy()
        y_pred = pred_sample["prediction"].to_numpy()
        axes[1].scatter(y_true, y_pred, alpha=0.25, s=8, color="#2a78d6")
        lim = float(np.nanmax([y_true.max(), y_pred.max()])) * 1.05
        axes[1].plot([0, lim], [0, lim], "k--", linewidth=1, label="y = x")
        axes[1].set_xlabel("Gerçek süre (dk)")
        axes[1].set_ylabel("Tahmin (dk)")
        axes[1].set_title(f"Gerçek vs tahmin (test kümesinden {len(y_true):,} kayıt)")
        axes[1].legend()
    fig.tight_layout()
    fig.savefig(out_dir / "20_s4_feature_actual_pred.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Şekil 18–20 kaydedildi: %s", out_dir)


def load_saved():
    """Kayıtlı S4 sonuçlarını (karşılaştırma, özellik önemi, tahmin örneği) okur."""
    import pandas as pd

    comparison = pd.read_csv(COMPARISON_CSV, dtype={"error": str})
    comparison["error"] = comparison["error"].fillna("")
    importance = pd.read_csv(IMPORTANCE_CSV)
    pred = pd.read_csv(PRED_SAMPLE_CSV) if PRED_SAMPLE_CSV.exists() else None
    return comparison, importance, pred


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="NYC Taxi süre tahmini (Öğrenci 4)")
    parser.add_argument("--sizes", nargs="+", default=SAMPLE_LABELS, choices=SAMPLE_LABELS)
    parser.add_argument("--plots-only", action="store_true",
                        help="kayıtlı CSV'lerden yalnızca grafikleri üret")
    parser.add_argument("--force-samples", action="store_true",
                        help="örneklemleri yeniden üret")
    # iç kullanım: tek ölçüm işçisi
    parser.add_argument("--worker", choices=["spark", "sklearn"], help=argparse.SUPPRESS)
    parser.add_argument("--model", choices=["lr", "gbt"], help=argparse.SUPPRESS)
    parser.add_argument("--size", help=argparse.SUPPRESS)
    parser.add_argument("--sample-dir", help=argparse.SUPPRESS)
    parser.add_argument("--out", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [S4] %(message)s")

    if args.worker:
        fn = run_spark_worker if args.worker == "spark" else run_sklearn_worker
        row = fn(Path(args.sample_dir), args.size, args.model)
        Path(args.out).write_text(json.dumps(row), encoding="utf-8")
        return
    if not args.plots_only:
        save_results(run_all(args.sizes, force_samples=args.force_samples))
    make_plots(*load_saved())


if __name__ == "__main__":
    main()
