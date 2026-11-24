"""NYC Taxi yolculuk süresi tahmini — PySpark MLlib pipeline.

Öğrenci 4 görevi:
  - LinearRegression (baseline) ve GBTRegressor ile yolculuk süresini tahmin et.
  - 100K, 1M, 5M kayıtlık alt örneklemlerde eğitim/değerlendirme yap.
  - Scikit-learn (Ridge, HistGradientBoostingRegressor) ile hız/bellek kıyaslaması yap.
  - Tüm sonuçları reports/results/s4_*.csv'e, grafikleri reports/figures/18-20_s4_*.png'e kaydet.

Kural: Spark oturumu her zaman get_spark() ile açılır.
Kural: Türetilmiş kolonlar (trip_duration_min, pickup_hour, pickup_dow) quality.py'den gelir.
Kural: Flex Fare kayıtlarında null olan kolonları (passenger_count, RatecodeID vb.) feature olarak kullanma.
Kural: fare_amount, total_amount, tip_amount, tpep_dropoff_datetime data leakage nedeniyle dışlanır.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from pathlib import Path

import psutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "reports" / "results"
FIGURES_DIR = PROJECT_ROOT / "reports" / "figures"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [S4] %(message)s")
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

# Features: null içermeyen ve data leakage olmayan kolonlar
# passenger_count, RatecodeID, congestion_surcharge, Airport_fee → Flex Fare'de null, dışla
# fare_amount, total_amount, tip_amount → data leakage
# tpep_dropoff_datetime → data leakage (target'tan türetiliyor)
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
CATEGORICAL_FEATURES = ["PULocationID", "DOLocationID"]  # StringIndexer + OHE


# ------------------------------------------------------------------
# PySpark Pipeline
# ------------------------------------------------------------------

def _build_pipeline(model_type: str = "lr"):
    """LinearRegression (lr) veya GBTRegressor (gbt) içeren ML pipeline döndürür."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
    from pyspark.ml.regression import GBTRegressor, LinearRegression

    stages = []

    # 1) Kategorik kolonlar: StringIndexer → OneHotEncoder
    indexed_cols = []
    for col in CATEGORICAL_FEATURES:
        idx_col = f"{col}_idx"
        ohe_col = f"{col}_ohe"
        indexer = StringIndexer(
            inputCol=col,
            outputCol=idx_col,
            handleInvalid="keep",  # null/bilinmeyen → "keep" kategorisi
        )
        encoder = OneHotEncoder(inputCol=idx_col, outputCol=ohe_col, handleInvalid="keep")
        stages.extend([indexer, encoder])
        indexed_cols.append(ohe_col)

    # 2) Tüm özellikleri birleştir
    all_features = NUMERIC_FEATURES + indexed_cols
    assembler = VectorAssembler(
        inputCols=all_features,
        outputCol="features",
        handleInvalid="keep",
    )
    stages.append(assembler)

    # 3) Model
    if model_type == "lr":
        model = LinearRegression(
            featuresCol="features",
            labelCol=TARGET_COL,
            predictionCol="prediction",
            maxIter=100,
        )
    elif model_type == "gbt":
        model = GBTRegressor(
            featuresCol="features",
            labelCol=TARGET_COL,
            predictionCol="prediction",
            maxIter=15,
            maxDepth=5,
            seed=SAMPLE_SEED,
        )
    else:
        raise ValueError(f"Bilinmeyen model tipi: {model_type}")

    stages.append(model)
    return Pipeline(stages=stages)


def _sample_data(df, n: int, seed: int = SAMPLE_SEED):
    """Tekrarlanabilir alt örnekleme.

    PySpark'ın dahili randomSplit / sample yöntemi seed verilerek deterministik çalışır.
    build_sizes.py'deki xxhash64 yaklaşımı ⊂ ilişkisi için kullanılır; burada
    sıralama garantisi gerekmiyor, sadece tekrarlanabilirlik yeterli.
    """
    total = df.count()
    if n >= total:
        return df
    frac = min(n / total * 1.05, 1.0)  # %5 marj — count() ile kesin boyut sağlanır
    return df.sample(withReplacement=False, fraction=frac, seed=seed).limit(n)


def _evaluate(predictions):
    """RMSE, MAE, R² hesapla ve dict döndür."""
    from pyspark.ml.evaluation import RegressionEvaluator

    metrics = {}
    for metric in ("rmse", "mae", "r2"):
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COL,
            predictionCol="prediction",
            metricName=metric,
        )
        metrics[metric] = evaluator.evaluate(predictions)
    return metrics


def _ram_mb() -> float:
    """Mevcut Python sürecinin bellek kullanımını MB olarak döndür."""
    return psutil.Process().memory_info().rss / 1024 / 1024


def run_spark_experiments(spark, df_clean):
    """Her boyut × model kombinasyonu için PySpark deneyleri çalıştır.

    Returns:
        list[dict] — Her satır bir deney sonucu.
    """
    results = []

    for size_label, size_n in zip(SAMPLE_LABELS, SAMPLE_SIZES):
        log.info(f"=== Alt örneklem: {size_label} ({size_n:,} kayıt) ===")

        # Sadece gerekli kolonları seç (Flex Fare null kolonlarından kaçın)
        needed_cols = (
            [TARGET_COL]
            + NUMERIC_FEATURES
            + CATEGORICAL_FEATURES
            + ["is_flex_fare"]
        )
        df_sel = df_clean.select(needed_cols).filter(
            # Target null olmamalı
            (df_clean[TARGET_COL].isNotNull())
            # Uzak uç değerleri filtrele (quality.py garantileri: 1 < süre ≤ 180)
            & (df_clean[TARGET_COL] > 0)
        )

        df_sample = _sample_data(df_sel, size_n)
        df_sample.cache()
        actual_n = df_sample.count()
        log.info(f"  Örneklem boyutu (gerçek): {actual_n:,}")

        train_df, test_df = df_sample.randomSplit(
            [TRAIN_RATIO, TEST_RATIO], seed=SAMPLE_SEED
        )
        train_df.cache()
        test_df.cache()

        for model_label, model_type in [("LinearRegression", "lr"), ("GBTRegressor", "gbt")]:
            log.info(f"  Model: {model_label}")
            pipeline = _build_pipeline(model_type)

            # Eğitim
            ram_before = _ram_mb()
            t0 = time.perf_counter()
            pipeline_model = pipeline.fit(train_df)
            train_time = time.perf_counter() - t0
            ram_peak = max(_ram_mb(), ram_before)

            # Tahmin
            t1 = time.perf_counter()
            predictions = pipeline_model.transform(test_df)
            predictions.cache()
            predictions.count()  # materialization
            predict_time = time.perf_counter() - t1

            # Değerlendirme
            metrics = _evaluate(predictions)

            row = {
                "framework": "PySpark",
                "model": model_label,
                "size_label": size_label,
                "size_n": actual_n,
                "rmse": round(metrics["rmse"], 4),
                "mae": round(metrics["mae"], 4),
                "r2": round(metrics["r2"], 4),
                "train_time_s": round(train_time, 2),
                "predict_time_s": round(predict_time, 2),
                "peak_ram_mb": round(ram_peak, 1),
            }
            results.append(row)
            log.info(f"    RMSE={metrics['rmse']:.3f}, R²={metrics['r2']:.3f}, eğitim={train_time:.1f}s")

            predictions.unpersist()

        train_df.unpersist()
        test_df.unpersist()
        df_sample.unpersist()

    return results


def _sklearn_worker_script(size_n: int, parquet_path: str, out_csv: str) -> str:
    """Ayrı subprocess'te çalışacak scikit-learn kıyaslama scripti (string olarak döner)."""
    return f'''
import sys, time, traceback, csv, os
import pandas as pd
import numpy as np
import psutil

def ram_mb():
    return psutil.Process().memory_info().rss / 1024 / 1024

TARGET = "trip_duration_min"
NUMERIC_FEATURES = {NUMERIC_FEATURES!r}
SIZE_N = {size_n}
PARQUET_PATH = {parquet_path!r}
OUT_CSV = {out_csv!r}
SEED = {SAMPLE_SEED}

try:
    # Veriyi oku
    t0 = time.perf_counter()
    df = pd.read_parquet(PARQUET_PATH, columns=NUMERIC_FEATURES + [TARGET])
    df = df.dropna(subset=[TARGET])
    df[NUMERIC_FEATURES] = df[NUMERIC_FEATURES].fillna(0)

    total = len(df)
    if SIZE_N < total:
        df = df.sample(n=SIZE_N, random_state=SEED)

    actual_n = len(df)
    X = df[NUMERIC_FEATURES].values.astype(np.float64)
    y = df[TARGET].values

    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    from sklearn.linear_model import Ridge
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

    rows = []
    for model_name, model in [
        ("Ridge", Ridge(alpha=1.0)),
        ("HistGradientBoosting", HistGradientBoostingRegressor(max_iter=100, max_depth=5, random_state=SEED))
    ]:
        ram_before = ram_mb()
        t_train = time.perf_counter()
        model.fit(X_train, y_train)
        train_time = time.perf_counter() - t_train
        ram_peak = max(ram_mb(), ram_before)

        t_pred = time.perf_counter()
        y_pred = model.predict(X_test)
        predict_time = time.perf_counter() - t_pred

        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae = float(mean_absolute_error(y_test, y_pred))
        r2 = float(r2_score(y_test, y_pred))

        rows.append({{
            "framework": "scikit-learn",
            "model": model_name,
            "size_n": actual_n,
            "rmse": round(rmse, 4),
            "mae": round(mae, 4),
            "r2": round(r2, 4),
            "train_time_s": round(train_time, 2),
            "predict_time_s": round(predict_time, 4),
            "peak_ram_mb": round(ram_peak, 1),
            "error": "",
        }})
        print(f"[sklearn] {{model_name}} n={{actual_n:,}} RMSE={{rmse:.3f}} R2={{r2:.3f}} train={{train_time:.1f}}s", flush=True)

    # CSV'ye yaz (append modu)
    file_exists = os.path.exists(OUT_CSV)
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    sys.exit(0)

except Exception as e:
    # Hataları da CSV'ye yaz
    import csv, os
    err_msg = traceback.format_exc()
    print(f"HATA: {{err_msg}}", file=sys.stderr)
    file_exists = os.path.exists(OUT_CSV)
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["framework","model","size_n","rmse","mae","r2","train_time_s","predict_time_s","peak_ram_mb","error"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({{
            "framework": "scikit-learn", "model": "HATA", "size_n": SIZE_N,
            "rmse": None, "mae": None, "r2": None,
            "train_time_s": None, "predict_time_s": None, "peak_ram_mb": None,
            "error": str(e)[:500],
        }})
    sys.exit(1)
'''


def run_sklearn_comparison(parquet_glob_path: str) -> list[dict]:
    """Scikit-learn kıyaslamasını ayrı subprocess'lerde çalıştır (ana süreci çöküşten koru)."""
    import csv
    import tempfile

    sklearn_csv = str(RESULTS_DIR / "s4_sklearn_comparison.csv")

    # Önceki sonuçları temizle
    Path(sklearn_csv).unlink(missing_ok=True)

    all_rows = []
    for size_n, size_label in zip(SAMPLE_SIZES, SAMPLE_LABELS):
        log.info(f"  scikit-learn kıyaslaması: {size_label}")

        script_code = _sklearn_worker_script(size_n, parquet_glob_path, sklearn_csv)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(script_code)
            script_path = f.name

        try:
            proc = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=600,  # 5M satırda zaman aşımı koruması
            )
            if proc.stdout:
                log.info(f"  stdout: {proc.stdout.strip()}")
            if proc.returncode != 0:
                log.warning(f"  subprocess hata kodu {proc.returncode}: {proc.stderr[:300]}")
        except subprocess.TimeoutExpired:
            log.warning(f"  {size_label} için scikit-learn zaman aşımı (600s) → not olarak kaydedildi")
            # Zaman aşımı kaydı
            with open(sklearn_csv, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["framework","model","size_n","rmse","mae","r2",
                                "train_time_s","predict_time_s","peak_ram_mb","error"],
                )
                writer.writerow({
                    "framework": "scikit-learn",
                    "model": "TIMEOUT",
                    "size_n": size_n,
                    "rmse": None, "mae": None, "r2": None,
                    "train_time_s": None, "predict_time_s": None, "peak_ram_mb": None,
                    "error": f"Zaman aşımı >600s ({size_label})",
                })
        except Exception as exc:
            log.warning(f"  subprocess başlatılamadı: {exc}")
        finally:
            Path(script_path).unlink(missing_ok=True)

    # Sonuçları oku
    if Path(sklearn_csv).exists():
        import csv
        with open(sklearn_csv, encoding="utf-8") as f:
            all_rows = list(csv.DictReader(f))

    return all_rows


def save_results(spark_results: list[dict], sklearn_results: list[dict]) -> None:
    """Deney sonuçlarını CSV olarak kaydet."""
    import csv

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # PySpark sonuçları
    spark_csv = RESULTS_DIR / "s4_spark_results.csv"
    if spark_results:
        fieldnames = list(spark_results[0].keys())
        with open(spark_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(spark_results)
        log.info(f"PySpark sonuçları kaydedildi: {spark_csv}")

    # Karşılaştırma tablosu (hem PySpark hem sklearn)
    combined = []
    for r in spark_results:
        combined.append({
            "framework": r["framework"],
            "model": r["model"],
            "size_label": r["size_label"],
            "size_n": r["size_n"],
            "rmse": r["rmse"],
            "mae": r["mae"],
            "r2": r["r2"],
            "train_time_s": r["train_time_s"],
            "predict_time_s": r["predict_time_s"],
            "peak_ram_mb": r["peak_ram_mb"],
            "error": "",
        })
    for r in sklearn_results:
        size_n = int(r.get("size_n", 0))
        label = {100_000: "100K", 1_000_000: "1M", 5_000_000: "5M"}.get(size_n, str(size_n))
        combined.append({
            "framework": r.get("framework", "scikit-learn"),
            "model": r.get("model", ""),
            "size_label": label,
            "size_n": size_n,
            "rmse": r.get("rmse"),
            "mae": r.get("mae"),
            "r2": r.get("r2"),
            "train_time_s": r.get("train_time_s"),
            "predict_time_s": r.get("predict_time_s"),
            "peak_ram_mb": r.get("peak_ram_mb"),
            "error": r.get("error", ""),
        })

    if combined:
        combined_csv = RESULTS_DIR / "s4_comparison.csv"
        fieldnames = list(combined[0].keys())
        with open(combined_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(combined)
        log.info(f"Karşılaştırma tablosu kaydedildi: {combined_csv}")


def save_feature_importance(spark, df_clean, out_path: Path) -> dict:
    """GBTRegressor'ü tam örneklemde eğit ve feature importance kaydet."""
    import csv

    log.info("Feature importance için 1M örneklemde GBT eğitimi...")
    needed_cols = [TARGET_COL] + NUMERIC_FEATURES + CATEGORICAL_FEATURES
    df_sel = df_clean.select(needed_cols).filter(
        df_clean[TARGET_COL].isNotNull() & (df_clean[TARGET_COL] > 0)
    )
    df_sample = _sample_data(df_sel, 1_000_000)
    df_sample.cache()

    pipeline = _build_pipeline("gbt")
    model = pipeline.fit(df_sample)

    # GBT modeli son stage
    gbt_model = model.stages[-1]
    importances = gbt_model.featureImportances.toArray()

    # Feature isimleri: numerics (n tane) + OHE çıktıları (her PU/DO için çok kolon)
    # VectorAssembler girdileri: NUMERIC_FEATURES + PULocationID_ohe + DOLocationID_ohe
    # OHE kolon sayısını bulmak için pipeline stage'lerini incele
    ohe_sizes = {}
    for stage in model.stages:
        if hasattr(stage, "categorySizes"):
            ohe_sizes[stage.getInputCol()] = stage.categorySizes[0]

    feature_names = list(NUMERIC_FEATURES)
    for cat in CATEGORICAL_FEATURES:
        idx_col = f"{cat}_idx"
        n_cats = ohe_sizes.get(idx_col, 1)
        for i in range(n_cats):
            feature_names.append(f"{cat}_{i}")

    # Boyut uyumsuzluğu olursa kırp/genişlet
    n = min(len(importances), len(feature_names))
    imp_dict = {feature_names[i]: float(importances[i]) for i in range(n)}
    # Topla ve sırala
    sorted_imp = sorted(imp_dict.items(), key=lambda x: -x[1])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["feature", "importance"])
        writer.writeheader()
        for feat, imp in sorted_imp:
            writer.writerow({"feature": feat, "importance": round(imp, 6)})

    log.info(f"Feature importance kaydedildi: {out_path}")
    df_sample.unpersist()
    return dict(sorted_imp)


def get_predictions_sample(spark, df_clean, n_sample: int = 5000) -> tuple:
    """GBT ile 1M örneklemde eğit, test setinden tahminleri döndür (görselleştirme için)."""
    needed_cols = [TARGET_COL] + NUMERIC_FEATURES + CATEGORICAL_FEATURES
    df_sel = df_clean.select(needed_cols).filter(
        df_clean[TARGET_COL].isNotNull() & (df_clean[TARGET_COL] > 0)
    )
    df_sample = _sample_data(df_sel, 1_000_000)
    train_df, test_df = df_sample.randomSplit([TRAIN_RATIO, TEST_RATIO], seed=SAMPLE_SEED)

    pipeline = _build_pipeline("gbt")
    model = pipeline.fit(train_df)
    preds = model.transform(test_df)

    # Pandas'a çek (tüm dağılımdan rastgele örneklem)
    pdf = (
        preds.select(TARGET_COL, "prediction")
        .sample(withReplacement=False, fraction=0.05, seed=SAMPLE_SEED)
        .limit(n_sample)
        .toPandas()
    )
    y_true = pdf[TARGET_COL].values
    y_pred = pdf["prediction"].values
    return y_true, y_pred


def make_plots(spark_results: list[dict], sklearn_rows: list[dict], importance_dict: dict,
               y_true, y_pred) -> None:
    """3 görselleştirme oluştur ve kaydet."""
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import seaborn as sns

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", palette="muted")

    # ----------------------------------------------------------------
    # Figür 18: PySpark vs scikit-learn eğitim süresi ve bellek (boyuta göre)
    # ----------------------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Şekil 18 · PySpark vs scikit-learn: Eğitim Süresi ve Bellek Tüketimi", fontsize=13)

    spark_df = pd.DataFrame(spark_results)
    sklearn_df = pd.DataFrame(sklearn_rows) if sklearn_rows else pd.DataFrame()
    if not sklearn_df.empty and "size_label" not in sklearn_df.columns and "size_n" in sklearn_df.columns:
        size_map = {100_000: "100K", 1_000_000: "1M", 5_000_000: "5M"}
        sklearn_df["size_label"] = sklearn_df["size_n"].astype(int).map(
            lambda v: size_map.get(v, str(v))
        )

    size_order = ["100K", "1M", "5M"]
    x = np.arange(len(size_order))
    width = 0.18

    # --- Eğitim süresi ---
    ax = axes[0]
    for i, (fw, model_lbl, style) in enumerate([
        ("PySpark", "LinearRegression", "steelblue"),
        ("PySpark", "GBTRegressor", "navy"),
        ("scikit-learn", "Ridge", "salmon"),
        ("scikit-learn", "HistGradientBoosting", "firebrick"),
    ]):
        df_src = spark_df if fw == "PySpark" else sklearn_df
        if df_src.empty:
            continue
        sub = df_src[(df_src["framework"] == fw) & (df_src["model"] == model_lbl)]
        vals = []
        for sl in size_order:
            row = sub[sub["size_label"] == sl]
            vals.append(float(row["train_time_s"].values[0]) if len(row) > 0 else 0)
        ax.bar(x + i * width - 1.5 * width, vals, width, label=f"{fw}/{model_lbl}", color=style, alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels(size_order)
    ax.set_xlabel("Veri boyutu")
    ax.set_ylabel("Eğitim süresi (sn)")
    ax.set_title("Eğitim Süresi")
    ax.legend(fontsize=8)

    # --- Bellek ---
    ax = axes[1]
    for i, (fw, model_lbl, style) in enumerate([
        ("PySpark", "LinearRegression", "steelblue"),
        ("PySpark", "GBTRegressor", "navy"),
        ("scikit-learn", "Ridge", "salmon"),
        ("scikit-learn", "HistGradientBoosting", "firebrick"),
    ]):
        df_src = spark_df if fw == "PySpark" else sklearn_df
        if df_src.empty:
            continue
        sub = df_src[(df_src["framework"] == fw) & (df_src["model"] == model_lbl)]
        vals = []
        for sl in size_order:
            row = sub[sub["size_label"] == sl]
            vals.append(float(row["peak_ram_mb"].values[0]) if len(row) > 0 else 0)
        ax.bar(x + i * width - 1.5 * width, vals, width, label=f"{fw}/{model_lbl}", color=style, alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels(size_order)
    ax.set_xlabel("Veri boyutu")
    ax.set_ylabel("Tepe RAM (MB)")
    ax.set_title("Bellek Tüketimi")
    ax.legend(fontsize=8)

    plt.tight_layout()
    out18 = FIGURES_DIR / "18_s4_scalability.png"
    fig.savefig(out18, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Şekil 18 kaydedildi: {out18}")

    # ----------------------------------------------------------------
    # Figür 19: 100K/1M/5M ölçeklerinde LR vs GBT RMSE & R² karşılaştırması
    # ----------------------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Şekil 19 · PySpark LinearRegression vs GBTRegressor: RMSE ve R²", fontsize=13)

    for ax, metric, ylabel in zip(
        axes, ["rmse", "r2"], ["RMSE (dakika)", "R²"]
    ):
        for model_lbl, color in [("LinearRegression", "steelblue"), ("GBTRegressor", "darkorange")]:
            sub = spark_df[spark_df["model"] == model_lbl]
            vals = []
            for sl in size_order:
                row = sub[sub["size_label"] == sl]
                vals.append(float(row[metric].values[0]) if len(row) > 0 else np.nan)
            ax.plot(size_order, vals, "o-", label=model_lbl, color=color, linewidth=2, markersize=8)

        ax.set_xlabel("Veri boyutu")
        ax.set_ylabel(ylabel)
        ax.set_title(ylabel)
        ax.legend()

    plt.tight_layout()
    out19 = FIGURES_DIR / "19_s4_model_comparison.png"
    fig.savefig(out19, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Şekil 19 kaydedildi: {out19}")

    # ----------------------------------------------------------------
    # Figür 20: Feature importance + Gerçek vs Tahmin
    # ----------------------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Şekil 20 · GBTRegressor: Özellik Önemi ve Gerçek vs Tahmin", fontsize=13)

    # Feature importance (top 15)
    ax = axes[0]
    if importance_dict:
        top_feats = list(importance_dict.items())[:15]
        feat_names = [f[0] for f in top_feats]
        feat_vals = [f[1] for f in top_feats]
        colors = sns.color_palette("Blues_r", len(feat_names))
        ax.barh(feat_names[::-1], feat_vals[::-1], color=colors[::-1])
        ax.set_xlabel("Önem Skoru")
        ax.set_title("Özellik Önem Düzeyleri (İlk 15)")
    else:
        ax.text(0.5, 0.5, "Veri yok", ha="center", va="center")

    # Gerçek vs Tahmin scatter
    ax = axes[1]
    if y_true is not None and y_pred is not None:
        # 2000 nokta yeter
        idx = np.random.default_rng(42).choice(len(y_true), min(2000, len(y_true)), replace=False)
        ax.scatter(y_true[idx], y_pred[idx], alpha=0.3, s=10, color="steelblue")
        lim = max(y_true[idx].max(), y_pred[idx].max()) * 1.05
        ax.plot([0, lim], [0, lim], "r--", linewidth=1.5, label="Mükemmel tahmin")
        ax.set_xlabel("Gerçek Süre (dk)")
        ax.set_ylabel("Tahmin Edilen Süre (dk)")
        ax.set_title("Gerçek vs Tahmin (GBT, 1M)")
        ax.legend()
    else:
        ax.text(0.5, 0.5, "Tahmin verisi yok", ha="center", va="center")

    plt.tight_layout()
    out20 = FIGURES_DIR / "20_s4_feature_actual_pred.png"
    fig.savefig(out20, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Şekil 20 kaydedildi: {out20}")


def main() -> None:
    """Ana akış: Spark başlat → veri oku → deneyleri çalıştır → kaydet."""
    from src.spark_session import get_spark
    from src.config import CLEAN_DIR

    log.info("Spark oturumu başlatılıyor...")
    spark = get_spark(app_name="s4-ml-duration")

    log.info(f"Temiz veri okunuyor: {CLEAN_DIR}")
    df_clean = spark.read.parquet(str(CLEAN_DIR))
    total_rows = df_clean.count()
    log.info(f"Toplam kayıt: {total_rows:,}")

    # 1) PySpark deneyleri
    log.info("=== PySpark Deneyleri Başlıyor ===")
    spark_results = run_spark_experiments(spark, df_clean)

    # 2) scikit-learn kıyaslaması (subprocess)
    log.info("=== scikit-learn Kıyaslaması Başlıyor ===")
    # Parquet klasörünü pandas'ın okuyabileceği tek yola çevir
    # (pandas read_parquet klasörü partition ile okuyabiliyor)
    sklearn_rows = run_sklearn_comparison(str(CLEAN_DIR))

    # 3) Kaydet
    save_results(spark_results, sklearn_rows)

    # 4) Feature importance
    imp_path = RESULTS_DIR / "s4_feature_importance.csv"
    importance_dict = save_feature_importance(spark, df_clean, imp_path)

    # 5) Gerçek vs Tahmin
    log.info("Gerçek vs Tahmin verisi alınıyor...")
    try:
        y_true, y_pred = get_predictions_sample(spark, df_clean)
    except Exception as e:
        log.warning(f"Tahmin örneği alınamadı: {e}")
        y_true, y_pred = None, None

    # 6) Grafikler
    log.info("=== Grafikler Oluşturuluyor ===")
    make_plots(spark_results, sklearn_rows, importance_dict, y_true, y_pred)

    spark.stop()
    log.info("=== S4 ML Pipeline Tamamlandı ===")


def run_plots_only() -> None:
    """Kayıtlı CSV sonuçlarından grafikleri üret (yeniden eğitim yapmaz)."""
    import csv
    from src.config import CLEAN_DIR
    from src.spark_session import get_spark

    comp_csv = RESULTS_DIR / "s4_comparison.csv"
    imp_csv = RESULTS_DIR / "s4_feature_importance.csv"

    if not comp_csv.exists() or not imp_csv.exists():
        raise FileNotFoundError("Önce ana pipeline çalıştırılıp CSV'ler üretilmeli.")

    with open(comp_csv, encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    spark_results = [r for r in all_rows if r["framework"] == "PySpark"]
    sklearn_rows = [r for r in all_rows if r["framework"] == "scikit-learn"]

    with open(imp_csv, encoding="utf-8") as f:
        importance_dict = {r["feature"]: float(r["importance"]) for r in csv.DictReader(f)}

    spark = get_spark(app_name="s4-ml-plots")
    df_clean = spark.read.parquet(str(CLEAN_DIR))
    log.info("Gerçek vs Tahmin verisi alınıyor (1M GBT)...")
    try:
        y_true, y_pred = get_predictions_sample(spark, df_clean)
    except Exception as e:
        log.warning(f"Tahmin örneği alınamadı: {e}")
        y_true, y_pred = None, None
    finally:
        spark.stop()

    make_plots(spark_results, sklearn_rows, importance_dict, y_true, y_pred)
    log.info("Grafikler kaydedildi ✓")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Yolculuk Süresi ML Pipeline (Öğrenci 4)")
    parser.add_argument(
        "--plots-only",
        action="store_true",
        help="Kayıtlı CSV sonuçlarını kullanarak yalnızca grafikleri üret",
    )
    args = parser.parse_args()
    if args.plots_only:
        run_plots_only()
    else:
        main()
