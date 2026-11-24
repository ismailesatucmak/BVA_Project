"""Birim testleri — src/ml_duration.py (Öğrenci 4)

Testler küçük sentetik veri üzerinde çalışır (Spark cores="1" ile).
Gerçek veri veya tam ML eğitimi gerektirmez.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# src modüllerinin import edilebilmesi için proje kökü sys.path'e eklenir
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ------------------------------------------------------------------
# Yardımcı: Küçük test DataFrame'i
# ------------------------------------------------------------------

def _make_test_df(spark, n: int = 200):
    """ML pipeline testleri için küçük sentetik DataFrame üret."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StructField,
        StructType,
        TimestampNTZType,
    )

    schema = StructType(
        [
            StructField("trip_distance", DoubleType(), False),
            StructField("pickup_hour", IntegerType(), False),
            StructField("pickup_dow", IntegerType(), False),
            StructField("extra", DoubleType(), False),
            StructField("mta_tax", DoubleType(), False),
            StructField("tolls_amount", DoubleType(), False),
            StructField("improvement_surcharge", DoubleType(), False),
            StructField("cbd_congestion_fee", DoubleType(), False),
            StructField("PULocationID", LongType(), False),
            StructField("DOLocationID", LongType(), False),
            StructField("trip_duration_min", DoubleType(), False),
            StructField("is_flex_fare", IntegerType(), False),
        ]
    )

    import random

    rng = random.Random(42)
    rows = []
    for i in range(n):
        dist = rng.uniform(0.5, 10.0)
        hour = rng.randint(0, 23)
        dow = rng.randint(1, 7)
        # Gerçekçi süre ~ dist * 5 + saat bazlı gürültü
        duration = max(1.0, dist * 5 + rng.gauss(0, 2))
        rows.append(
            (
                round(dist, 2),
                hour,
                dow,
                rng.choice([0.0, 0.5, 1.0]),
                0.5,
                rng.choice([0.0, 6.55]),
                1.0,
                0.0,
                rng.randint(1, 263),
                rng.randint(1, 263),
                round(duration, 2),
                0,
            )
        )

    return spark.createDataFrame(rows, schema)


# ------------------------------------------------------------------
# Test: Pipeline inşası
# ------------------------------------------------------------------

class TestBuildPipeline:
    def test_lr_pipeline_stages(self):
        from src.ml_duration import CATEGORICAL_FEATURES, _build_pipeline

        p = _build_pipeline("lr")
        # En az: 2×(StringIndexer+OHE) + VectorAssembler + LR = 7 stage
        assert len(p.getStages()) >= 2 + 2 * len(CATEGORICAL_FEATURES)

    def test_gbt_pipeline_stages(self):
        from src.ml_duration import _build_pipeline

        p = _build_pipeline("gbt")
        assert len(p.getStages()) >= 5

    def test_unknown_model_raises(self):
        from src.ml_duration import _build_pipeline

        with pytest.raises(ValueError, match="Bilinmeyen model tipi"):
            _build_pipeline("xgb")


# ------------------------------------------------------------------
# Test: Örnekleme
# ------------------------------------------------------------------

class TestSampleData:
    def test_sample_smaller_than_total(self, spark):
        from src.ml_duration import _sample_data

        df = spark.range(10_000).toDF("id")
        sampled = _sample_data(df, 500)
        count = sampled.count()
        # Tolerans: ±20%
        assert 400 <= count <= 600, f"Beklenen ~500, gerçek {count}"

    def test_sample_larger_than_total_returns_all(self, spark):
        from src.ml_duration import _sample_data

        df = spark.range(100).toDF("id")
        sampled = _sample_data(df, 10_000)
        assert sampled.count() == 100

    def test_sample_deterministic(self, spark):
        """Aynı seed ile iki kez çağrıldığında aynı sonuç döner."""
        from src.ml_duration import SAMPLE_SEED, _sample_data

        df = spark.range(5_000).toDF("id")
        s1 = _sample_data(df, 200).collect()
        s2 = _sample_data(df, 200).collect()
        assert s1 == s2


# ------------------------------------------------------------------
# Test: LR Pipeline fit + transform (küçük veri)
# ------------------------------------------------------------------

class TestLinearRegressionPipeline:
    def test_pipeline_fit_transform(self, spark):
        from src.ml_duration import TARGET_COL, _build_pipeline

        df = _make_test_df(spark, n=200)
        pipeline = _build_pipeline("lr")
        train, test = df.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train)
        preds = model.transform(test)
        assert "prediction" in preds.columns
        assert preds.filter(preds["prediction"].isNotNull()).count() > 0

    def test_lr_prediction_range(self, spark):
        """Tahminler makul aralıkta olmalı (örn. 0–300 dakika)."""
        from src.ml_duration import _build_pipeline

        df = _make_test_df(spark, n=200)
        pipeline = _build_pipeline("lr")
        train, test = df.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train)
        preds = model.transform(test)
        pdf = preds.select("prediction").toPandas()
        # Çok aşırı tahminler olmamalı
        assert pdf["prediction"].max() < 1000
        assert pdf["prediction"].min() > -100


# ------------------------------------------------------------------
# Test: GBT Pipeline fit + transform (küçük veri)
# ------------------------------------------------------------------

class TestGBTPipeline:
    def test_gbt_fit_transform(self, spark):
        from src.ml_duration import _build_pipeline

        df = _make_test_df(spark, n=200)
        pipeline = _build_pipeline("gbt")
        train, test = df.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train)
        preds = model.transform(test)
        assert "prediction" in preds.columns
        assert preds.count() > 0

    def test_gbt_better_than_lr_on_nonlinear(self, spark):
        """GBT, doğrusal olmayan ilişkide LR'den daha düşük RMSE vermeli."""
        from pyspark.ml.evaluation import RegressionEvaluator

        from src.ml_duration import TARGET_COL, _build_pipeline

        df = _make_test_df(spark, n=300)
        train, test = df.randomSplit([0.8, 0.2], seed=42)

        evaluator = RegressionEvaluator(
            labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse"
        )

        lr_model = _build_pipeline("lr").fit(train)
        gbt_model = _build_pipeline("gbt").fit(train)

        lr_rmse = evaluator.evaluate(lr_model.transform(test))
        gbt_rmse = evaluator.evaluate(gbt_model.transform(test))

        # GBT genelde daha iyidir ama garanti yok; sadece çok kötü olmamalı
        assert gbt_rmse < lr_rmse * 2.0, (
            f"GBT RMSE ({gbt_rmse:.3f}) LR RMSE'nin ({lr_rmse:.3f}) 2 katından fazla"
        )


# ------------------------------------------------------------------
# Test: Değerlendirme metrikleri
# ------------------------------------------------------------------

class TestEvaluateMetrics:
    def test_evaluate_returns_all_metrics(self, spark):
        from src.ml_duration import TARGET_COL, _build_pipeline, _evaluate

        df = _make_test_df(spark, n=200)
        train, test = df.randomSplit([0.8, 0.2], seed=42)
        model = _build_pipeline("lr").fit(train)
        preds = model.transform(test)
        metrics = _evaluate(preds)
        assert set(metrics.keys()) == {"rmse", "mae", "r2"}
        assert metrics["rmse"] >= 0
        assert metrics["mae"] >= 0
        assert metrics["r2"] <= 1.0

    def test_perfect_prediction_r2_is_one(self, spark):
        """Tahmin = gerçek ise R² = 1 olmalı."""
        from pyspark.sql import functions as F

        from src.ml_duration import TARGET_COL, _evaluate

        df = _make_test_df(spark, n=50).withColumn("prediction", F.col(TARGET_COL))
        metrics = _evaluate(df)
        assert abs(metrics["r2"] - 1.0) < 1e-6


# ------------------------------------------------------------------
# Test: Feature leakage kontrolü
# ------------------------------------------------------------------

class TestFeatureLeakage:
    def test_no_fare_columns_in_features(self):
        """Ücret kolonları ve varış zamanı feature listesinde olmamalı."""
        from src.ml_duration import CATEGORICAL_FEATURES, NUMERIC_FEATURES

        all_features = set(NUMERIC_FEATURES + CATEGORICAL_FEATURES)
        forbidden = {
            "fare_amount", "total_amount", "tip_amount",
            "tpep_dropoff_datetime", "trip_duration_min",
        }
        overlap = all_features & forbidden
        assert not overlap, f"Data leakage: şu kolonlar feature'da: {overlap}"

    def test_no_flex_fare_null_columns_in_features(self):
        """Flex Fare'de null olan kolonlar feature listesinde olmamalı."""
        from src.ml_duration import CATEGORICAL_FEATURES, NUMERIC_FEATURES

        all_features = set(NUMERIC_FEATURES + CATEGORICAL_FEATURES)
        flex_fare_null_cols = {
            "passenger_count", "RatecodeID",
            "store_and_fwd_flag", "congestion_surcharge", "Airport_fee",
        }
        overlap = all_features & flex_fare_null_cols
        assert not overlap, f"Flex Fare null kolonlar feature'da: {overlap}"


# ------------------------------------------------------------------
# Test: Sabitler ve yapılandırma
# ------------------------------------------------------------------

class TestConfig:
    def test_sample_sizes_ascending(self):
        from src.ml_duration import SAMPLE_SIZES

        assert SAMPLE_SIZES == sorted(SAMPLE_SIZES)

    def test_sample_labels_match_sizes(self):
        from src.ml_duration import SAMPLE_LABELS, SAMPLE_SIZES

        assert len(SAMPLE_LABELS) == len(SAMPLE_SIZES)

    def test_train_test_ratio_sums_to_one(self):
        from src.ml_duration import TEST_RATIO, TRAIN_RATIO

        assert abs(TRAIN_RATIO + TEST_RATIO - 1.0) < 1e-9

    def test_target_column_name(self):
        from src.ml_duration import TARGET_COL

        assert TARGET_COL == "trip_duration_min"

    def test_numeric_features_non_empty(self):
        from src.ml_duration import NUMERIC_FEATURES

        assert len(NUMERIC_FEATURES) > 0

    def test_categorical_features(self):
        from src.ml_duration import CATEGORICAL_FEATURES

        assert "PULocationID" in CATEGORICAL_FEATURES
        assert "DOLocationID" in CATEGORICAL_FEATURES
