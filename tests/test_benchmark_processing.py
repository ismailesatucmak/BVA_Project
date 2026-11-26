"""Birim testleri — src/benchmark_processing.py (Öğrenci 5)."""
from __future__ import annotations

import pandas as pd
import pytest

from src import benchmark_processing as bp


def _trips() -> pd.DataFrame:
    return pd.DataFrame({
        "tpep_pickup_datetime": pd.to_datetime([
            "2026-01-01 08:10:00", "2026-01-01 08:50:00", "2026-01-02 09:00:00",
            "2026-01-02 09:30:00", "2026-01-03 23:59:00", "2026-01-03 08:00:00",
        ]),
        "PULocationID": [1, 1, 1, 1, 2, 265],
        "payment_type": [1, 2, 1, 0, 1, 1],
        "trip_distance": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "fare_amount": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "total_amount": [15.0, 25.0, 35.0, 45.0, 55.0, 65.0],
    })


def _zones() -> pd.DataFrame:
    return pd.DataFrame({"LocationID": [1, 2, 265], "Borough": ["EWR", "Queens", "N/A"],
                         "Zone": ["a", "b", "c"], "service_zone": ["x", "y", "N/A"]})


class TestPandasOps:
    def test_groupby_hour(self):
        out = bp.pandas_groupby_hour(_trips()).set_index("hour")
        assert out.loc[8, "trips"] == 3
        assert out.loc[9, "revenue"] == pytest.approx(80.0)
        assert out["trips"].sum() == 6

    def test_join_borough_keeps_na_label(self):
        out = bp.pandas_join_borough(_trips(), _zones()).set_index("Borough")
        assert out.loc["EWR", "trips"] == 4
        assert out.loc["N/A", "trips"] == 1  # "N/A" metin olarak korunur, NaN değil

    def test_window_top3_per_zone(self):
        out = bp.pandas_window_top3(_trips())
        assert sorted(out[out["PULocationID"] == 1]["total_amount"]) == [25.0, 35.0, 45.0]
        assert len(out) == 3 + 1 + 1

    def test_zone_file_reader_keeps_na(self, tmp_path):
        path = tmp_path / "z.csv"
        _zones().to_csv(path, index=False)
        assert "N/A" in set(bp.pandas_zones(path)["Borough"])


class TestSparkMatchesPandas:
    """Her işlem için Spark sonucu pandas ile aynı checksum'ı vermeli."""

    def test_all_ops_consistent(self, spark):
        trips, zones = _trips(), _zones()
        s_trips = spark.createDataFrame(trips)
        s_zones = spark.createDataFrame(zones)
        p_ops, s_ops = bp.pandas_ops(zones), bp.spark_ops(s_zones)
        for op in bp.OPERATIONS:
            p_fn, col = p_ops[op]
            s_fn, _ = s_ops[op]
            p = bp.checksum(p_fn(trips), col)
            s = bp.checksum(s_fn(s_trips).toPandas(), col)
            assert p[0] == s[0], op
            assert p[1] == pytest.approx(s[1]), op


class TestChecksumAndSummary:
    def test_checksum(self):
        assert bp.checksum(pd.DataFrame({"v": [1, 2, 3]}), "v") == (3, 6.0)

    def _raw(self):
        recs = []
        for engine, setup, sec in (("pandas", "load", 1.0), ("spark", "startup", 2.0)):
            recs.append({"engine": engine, "size": "1m", "format": "csv", "op": setup,
                         "rep": 1, "status": "ok", "seconds": 5.0, "peak_rss_mb": 100.0,
                         "rss_before_mb": 50.0, "rows": 1000 if engine == "pandas" else None,
                         "result_rows": None, "checksum": None})
            for op in bp.OPERATIONS:
                for rep in (1, 2, 3):
                    recs.append({"engine": engine, "size": "1m", "format": "csv", "op": op,
                                 "rep": rep, "status": "ok", "seconds": sec + rep * 0.1,
                                 "peak_rss_mb": 100.0 + rep, "rss_before_mb": 50.0,
                                 "rows": 1000, "result_rows": 24, "checksum": 1000.0})
        return pd.DataFrame(recs)

    def test_summarize_mean_and_peak(self):
        s = bp.summarize(self._raw())
        row = s[(s["engine"] == "pandas") & (s["op"] == "groupby_hour")].iloc[0]
        assert row["seconds_mean"] == pytest.approx(1.2)
        assert row["peak_rss_mb"] == 103.0
        assert row["n_ok"] == 3
        assert row["throughput_rows_s"] == pytest.approx(1000 / 1.2)

    def test_consistency_passes_and_detects_mismatch(self):
        s = bp.summarize(self._raw())
        assert bp.check_consistency(s) == []
        s.loc[(s["engine"] == "spark") & (s["op"] == "window_top3"), "checksum"] = 999.0
        problems = bp.check_consistency(s)
        assert len(problems) == 1 and "window_top3" in problems[0]

    def test_end_to_end_adds_setup_step(self):
        e2e = bp.end_to_end(bp.summarize(self._raw())).set_index("engine")
        assert e2e.loc["pandas", "setup_s"] == 5.0
        assert e2e.loc["pandas", "total_s"] == pytest.approx(5.0 + 3 * 1.2)
        assert e2e.loc["spark", "total_s"] == pytest.approx(5.0 + 3 * 2.2)
        assert e2e.loc["spark", "rows"] == 1000


def test_spark_cache_setup_is_startup_plus_load():
    recs = []
    for op, sec in (("startup", 4.0), ("load", 2.0), *[(o, 0.5) for o in bp.OPERATIONS]):
        recs.append({"engine": "spark_cache", "size": "1m", "format": "parquet_snappy", "op": op,
                     "rep": 1, "status": "ok", "seconds": sec, "peak_rss_mb": 100.0,
                     "rss_before_mb": 50.0, "rows": 1000, "result_rows": 1, "checksum": 1.0})
    e2e = bp.end_to_end(bp.summarize(pd.DataFrame(recs))).iloc[0]
    assert e2e["setup_s"] == pytest.approx(6.0)
    assert e2e["total_s"] == pytest.approx(6.0 + 1.5)


def test_every_engine_has_worker_and_setup():
    assert set(bp.ENGINES) == set(bp.WORKERS) == set(bp.SETUP_STEPS)


def test_parse_worker_output_ignores_noise():
    out = "log line\n" + bp.RESULT_PREFIX + '{"op": "load", "seconds": 1.5}\nWARN x\n'
    assert bp.parse_worker_output(out) == [{"op": "load", "seconds": 1.5}]
