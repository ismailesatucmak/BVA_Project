import pytest
from pyspark.sql import functions as F

from src import anomaly


@pytest.fixture(scope="module")
def route_frame(spark):
    # A→B güzergâhında 40 normal yolculuk (ücret 20–29) + 1 aşırı pahalı (300 $);
    # C→D'de yalnızca 3 yolculuk (bağlam için yetersiz) → işaretlenmemeli
    rows = [(1, 2, 20.0 + i % 10, 15.0, 2.0) for i in range(40)]
    rows.append((1, 2, 300.0, 15.0, 2.0))
    rows += [(3, 4, 10.0, 10.0, 1.0), (3, 4, 11.0, 10.0, 1.0), (3, 4, 500.0, 10.0, 1.0)]
    schema = "PULocationID int, DOLocationID int, fare_amount double, trip_duration_min double, trip_distance double"
    return spark.createDataFrame(rows, schema)


def test_contextual_flags_route_outlier_only(route_frame):
    flagged = anomaly.add_context_flags(route_frame, min_trips=30).cache()
    hits = flagged.filter("ctx_fare").select("fare_amount").collect()
    assert [r.fare_amount for r in hits] == [300.0]
    # yetersiz bağlamlı güzergâhta 500 $ işaretlenmez
    assert flagged.filter("PULocationID = 3 AND ctx_fare").count() == 0
    assert flagged.filter("trip_duration_min IS NOT NULL AND ctx_duration").count() == 0


def test_bounds_iqr_and_percentile(spark):
    df = spark.createDataFrame([(float(v),) for v in range(1, 101)], "x double")
    bounds = anomaly.global_bounds(df, ["x"])["x"]
    assert bounds["q1"] == pytest.approx(25, abs=1)
    assert bounds["q3"] == pytest.approx(75, abs=1)
    iqr = bounds["q3"] - bounds["q1"]
    assert bounds["iqr_high"] == pytest.approx(bounds["q3"] + 1.5 * iqr)
    assert bounds["p_extreme"] == pytest.approx(100, abs=1)


def test_overlap_counts(spark):
    df = spark.createDataFrame(
        [(True, True), (True, False), (False, True), (False, False)], "rule boolean, stat boolean"
    )
    counts = anomaly.overlap_counts(df, F.col("rule"), F.col("stat"))
    assert counts == {"a": 2, "b": 2, "both": 1, "a_only": 1, "b_only": 1}
