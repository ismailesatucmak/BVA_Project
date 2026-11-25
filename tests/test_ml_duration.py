"""Birim testleri — src/ml_duration.py (Öğrenci 4) ve src/mem_probe.py.

Testler küçük sentetik veri üzerinde çalışır (Spark cores="1" ile).
Gerçek veri veya tam ML eğitimi gerektirmez.
"""

from __future__ import annotations

import random

import numpy as np
import pandas as pd
import pytest

from src import ml_duration as ml


# ------------------------------------------------------------------
# Yardımcı: küçük sentetik veri
# ------------------------------------------------------------------

def _make_rows(n: int = 300, seed: int = 42) -> pd.DataFrame:
    rng = random.Random(seed)
    rows = []
    for _ in range(n):
        dist = rng.uniform(0.5, 10.0)
        pu = rng.choice([1, 2, 3, 4, 5, 6])
        do = rng.choice([1, 2, 3, 4, 5, 6])
        duration = max(1.0, dist * 4 + (5 if pu == 3 else 0) + rng.gauss(0, 1))
        rows.append({
            "trip_distance": round(dist, 2),
            "pickup_hour": rng.randint(0, 23),
            "pickup_dow": rng.randint(1, 7),
            "extra": rng.choice([0.0, 0.5, 1.0]),
            "mta_tax": 0.5,
            "tolls_amount": rng.choice([0.0, 6.55]),
            "improvement_surcharge": 1.0,
            "cbd_congestion_fee": rng.choice([0.0, 0.75]),
            "PULocationID": pu,
            "DOLocationID": do,
            ml.TARGET_COL: round(duration, 3),
        })
    return pd.DataFrame(rows)


def _coded_spark_df(spark, n: int = 300):
    """Sentetik veri → bölge kodları (eğitim kümesinden) eklenmiş Spark DataFrame."""
    sdf = spark.createDataFrame(_make_rows(n))
    mapping = ml.fit_location_codes(sdf)
    return ml.apply_location_codes(sdf, mapping)


# ------------------------------------------------------------------
# Pipeline inşası
# ------------------------------------------------------------------

class TestBuildPipeline:
    def test_lr_pipeline_stages(self, spark):
        stages = ml._build_pipeline("lr").getStages()
        assert [type(s).__name__ for s in stages] == [
            "OneHotEncoder", "VectorAssembler", "LinearRegression"
        ]

    def test_gbt_pipeline_uses_aligned_hyperparameters(self, spark):
        gbt = ml._build_pipeline("gbt").getStages()[-1]
        assert gbt.getMaxIter() == ml.GBT_MAX_ITER
        assert gbt.getMaxDepth() == ml.GBT_MAX_DEPTH
        assert gbt.getStepSize() == ml.GBT_LEARNING_RATE
        assert gbt.getMaxBins() >= ml.N_LOCATION_CODES

    def test_unknown_model_raises(self, spark):
        with pytest.raises(ValueError, match="Bilinmeyen model tipi"):
            ml._build_pipeline("xgb")
        with pytest.raises(ValueError, match="Bilinmeyen model tipi"):
            ml.build_sklearn_model("xgb")


# ------------------------------------------------------------------
# Örnekleme ve train/test ayrımı
# ------------------------------------------------------------------

class TestSampleAndSplit:
    def test_sample_size_is_approximate(self, spark):
        df = spark.range(10_000).toDF("id")
        count = ml._sample_data(df, 500).count()
        assert 400 <= count <= 600, f"Beklenen ~500, gerçek {count}"

    def test_sample_larger_than_total_returns_all(self, spark):
        df = spark.range(100).toDF("id")
        assert ml._sample_data(df, 10_000).count() == 100

    def test_samples_are_deterministic_and_nested(self, spark):
        df = spark.range(5_000).toDF("id")
        small = {r.id for r in ml._sample_data(df, 200).collect()}
        again = {r.id for r in ml._sample_data(df, 200).collect()}
        large = {r.id for r in ml._sample_data(df, 1_000).collect()}
        assert small == again
        assert small <= large

    def test_split_is_deterministic_with_expected_ratio(self, spark):
        df = ml._add_split(spark.range(5_000).toDF("id"))
        first = {r.id for r in df.filter(ml.SPLIT_COL).collect()}
        second = {r.id for r in df.filter(ml.SPLIT_COL).collect()}
        assert first == second
        assert 0.75 <= len(first) / 5_000 <= 0.85


# ------------------------------------------------------------------
# Bölge kodlaması
# ------------------------------------------------------------------

class TestLocationCodes:
    def test_most_frequent_location_gets_code_zero(self, spark):
        sdf = spark.createDataFrame(
            pd.DataFrame({"PULocationID": [7, 7, 7, 3, 3, 9], "DOLocationID": [1, 2, 2, 2, 1, 2]})
        )
        mapping = ml.fit_location_codes(sdf)
        assert mapping["PULocationID"] == {7: 0, 3: 1, 9: 2}
        assert mapping["DOLocationID"] == {2: 0, 1: 1}

    def test_unseen_location_maps_to_other(self, spark):
        mapping = {"PULocationID": {7: 0}, "DOLocationID": {2: 0}}
        sdf = spark.createDataFrame(pd.DataFrame({"PULocationID": [7, 99], "DOLocationID": [2, 2]}))
        rows = ml.apply_location_codes(sdf, mapping).orderBy("PULocationID").collect()
        assert [r.PU_code for r in rows] == [0.0, float(ml.OTHER_CODE)]

    def test_codes_carry_nominal_metadata(self, spark):
        df = _coded_spark_df(spark, n=20)
        for c in ml.CATEGORICAL_FEATURES:
            attr = df.schema[c].metadata["ml_attr"]
            assert attr["type"] == "nominal"
            assert attr["num_vals"] == ml.N_LOCATION_CODES

    def test_gbt_sees_locations_as_categorical(self, spark):
        """Ağaç modeli bölgeyi kategorik görmezse maxBins < 255 hata vermez."""
        from pyspark.ml.feature import VectorAssembler

        df = _coded_spark_df(spark, n=50)
        assembled = VectorAssembler(
            inputCols=ml.NUMERIC_FEATURES + ml.CATEGORICAL_FEATURES, outputCol="features"
        ).transform(df)
        nominal = assembled.schema["features"].metadata["ml_attr"]["attrs"]["nominal"]
        assert {a["name"] for a in nominal} == set(ml.CATEGORICAL_FEATURES)


# ------------------------------------------------------------------
# Modeller
# ------------------------------------------------------------------

class TestModels:
    def test_spark_pipelines_fit_and_predict(self, spark):
        df = _coded_spark_df(spark)
        for model_type in ("lr", "gbt"):
            preds = ml._build_pipeline(model_type).fit(df).transform(df)
            assert preds.filter(preds["prediction"].isNull()).count() == 0

    def test_spark_and_sklearn_ols_give_same_predictions(self, spark):
        """Aynı veri + aynı kodlama → iki çerçevenin OLS tahminleri aynı olmalı (adil kıyas)."""
        df = _coded_spark_df(spark)
        spark_pred = (
            ml._build_pipeline("lr").fit(df).transform(df)
            .select("prediction").toPandas()["prediction"].to_numpy()
        )
        pdf = df.toPandas()
        for c in ml.CATEGORICAL_FEATURES:
            pdf[c] = pdf[c].astype("int64")
        X = pdf[ml.NUMERIC_FEATURES + ml.CATEGORICAL_FEATURES]
        sk_pred = ml.build_sklearn_model("lr").fit(X, pdf[ml.TARGET_COL]).predict(X)
        np.testing.assert_allclose(spark_pred, sk_pred, atol=1e-4)

    def test_sklearn_gbt_beats_mean_baseline(self):
        pdf = _make_rows(400)
        pdf["PU_code"] = pdf["PULocationID"] - 1
        pdf["DO_code"] = pdf["DOLocationID"] - 1
        X = pdf[ml.NUMERIC_FEATURES + ml.CATEGORICAL_FEATURES]
        y = pdf[ml.TARGET_COL].to_numpy()
        pred = ml.build_sklearn_model("gbt").fit(X, y).predict(X)
        assert ml.sklearn_metrics(y, pred)["r2"] > 0.9

    def test_evaluate_returns_all_metrics(self, spark):
        df = _coded_spark_df(spark, n=100)
        metrics = ml._evaluate(ml._build_pipeline("lr").fit(df).transform(df))
        assert set(metrics) == {"rmse", "mae", "r2"}
        assert metrics["rmse"] >= 0 and metrics["mae"] >= 0 and metrics["r2"] <= 1.0

    def test_perfect_prediction_r2_is_one(self, spark):
        from pyspark.sql import functions as F

        df = spark.createDataFrame(_make_rows(50)).withColumn("prediction", F.col(ml.TARGET_COL))
        assert ml._evaluate(df)["r2"] == pytest.approx(1.0)

    def test_sklearn_metrics_match_definition(self):
        m = ml.sklearn_metrics(np.array([1.0, 2.0, 3.0]), np.array([1.0, 2.0, 5.0]))
        assert m["mae"] == pytest.approx(2 / 3)
        assert m["rmse"] == pytest.approx(np.sqrt(4 / 3))

    def test_feature_names_match_gbt_vector_order(self):
        assert ml.feature_names() == ml.NUMERIC_FEATURES + ml.LOCATION_COLS


# ------------------------------------------------------------------
# Leakage ve sabitler
# ------------------------------------------------------------------

class TestFeatureLeakage:
    def test_no_fare_columns_in_features(self):
        features = set(ml.NUMERIC_FEATURES + ml.LOCATION_COLS)
        forbidden = {"fare_amount", "total_amount", "tip_amount",
                     "tpep_dropoff_datetime", ml.TARGET_COL}
        assert not features & forbidden

    def test_no_flex_fare_null_columns_in_features(self):
        features = set(ml.NUMERIC_FEATURES + ml.LOCATION_COLS)
        flex_null = {"passenger_count", "RatecodeID", "store_and_fwd_flag",
                     "congestion_surcharge", "Airport_fee"}
        assert not features & flex_null


class TestConfig:
    def test_sizes_and_labels(self):
        assert ml.SAMPLE_SIZES == sorted(ml.SAMPLE_SIZES)
        assert len(ml.SAMPLE_LABELS) == len(ml.SAMPLE_SIZES) >= 3

    def test_train_test_ratio_sums_to_one(self):
        assert ml.TRAIN_RATIO + ml.TEST_RATIO == pytest.approx(1.0)

    def test_location_codes_fit_sklearn_categorical_limit(self):
        assert ml.N_LOCATION_CODES <= 255
        assert ml.OTHER_CODE == ml.N_LOCATION_CODES - 1


# ------------------------------------------------------------------
# Bellek ölçümü ve grafikler
# ------------------------------------------------------------------

class TestPeakRSS:
    def test_peak_captures_allocation_inside_block(self):
        from src.mem_probe import PeakRSS

        with PeakRSS(interval_s=0.01) as probe:
            block = np.ones(200 * 1024 * 1024 // 8)  # ~200 MB
            block.sum()
            del block
        assert probe.peak_total_mb - probe.start_total_mb > 150

    def test_tree_rss_counts_current_process(self):
        from src.mem_probe import tree_rss_mb

        total, jvm = tree_rss_mb()
        assert total > 0 and jvm >= 0


def test_make_plots_writes_three_figures(tmp_path):
    rows = []
    for fw, key, name in ml.EXPERIMENTS:
        for lbl, n in zip(ml.SAMPLE_LABELS, ml.SAMPLE_SIZES):
            rows.append({"framework": fw, "model": name, "size_label": lbl, "size_n": n,
                         "rmse": 6.0, "r2": 0.7, "train_time_s": n / 1e5,
                         "peak_rss_mb": n / 1e4, "error": ""})
    importance = pd.DataFrame({"feature": ml.feature_names(),
                               "importance": [0.1] * len(ml.feature_names())})
    pred = pd.DataFrame({ml.TARGET_COL: [5.0, 10.0], "prediction": [6.0, 9.0]})
    ml.make_plots(pd.DataFrame(rows), importance, pred, out_dir=tmp_path)
    for name in ("18_s4_scalability.png", "19_s4_model_comparison.png",
                 "20_s4_feature_actual_pred.png"):
        assert (tmp_path / name).stat().st_size > 0
