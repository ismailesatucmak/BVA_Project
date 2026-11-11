import json

import pandas as pd
import pytest

from src import benchmark_io as bio


@pytest.fixture
def tiny_files(tmp_path):
    df = pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(["2026-01-01 00:50:40", "2026-01-02 10:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2026-01-01 01:03:09", "2026-01-02 10:30:00"]),
            "trip_distance": [1.26, 3.5],
            "fare_amount": [12.1, 20.0],
            "store_and_fwd_flag": ["N", None],
        }
    )
    csv = tmp_path / "yellow.csv"
    df.to_csv(csv, index=False, date_format="%Y-%m-%d %H:%M:%S")
    parquet = tmp_path / "yellow.parquet"
    df.to_parquet(parquet, index=False)
    return csv, parquet


def test_csv_and_parquet_reads_produce_same_dtypes(tiny_files):
    csv, parquet = tiny_files
    from_csv = bio.read_pandas(csv, "all")
    from_parquet = bio.read_pandas(parquet, "all")
    assert from_csv.dtypes["tpep_pickup_datetime"].kind == "M"
    assert from_csv.dtypes["tpep_dropoff_datetime"].kind == "M"
    # CSV'deki boş metin NaN, Parquet'teki null None olarak gelir → karşılaştırmadan önce eşitle
    normalize = lambda df: df.astype(object).where(df.notna(), None)  # noqa: E731
    pd.testing.assert_frame_equal(normalize(from_csv), normalize(from_parquet), check_dtype=False)


def test_three_column_mode_reads_only_selected_columns(tiny_files):
    for path in tiny_files:
        df = bio.read_pandas(path, "3cols")
        assert set(df.columns) == set(bio.SELECTED_COLUMNS)
        assert df.dtypes["tpep_pickup_datetime"].kind == "M"


def test_measure_pandas_reports_rows_time_and_memory(tiny_files):
    result = bio.measure_pandas(tiny_files[1], "all")
    assert result["rows"] == 2
    assert result["seconds"] > 0
    assert result["frame_mb"] > 0
    assert result["peak_mb"] >= 0


def test_parse_worker_output_ignores_noise():
    stdout = "spark log line\n" + bio.RESULT_PREFIX + json.dumps({"rows": 5}) + "\nmore noise\n"
    assert bio.parse_worker_output(stdout) == [{"rows": 5}]


def _raw(rows):
    cols = ["engine", "size", "format", "mode", "rep", "seconds", "rows", "status"]
    return pd.DataFrame(rows, columns=cols)


def test_completed_combos_requires_all_ok_repeats():
    raw = _raw(
        [("pandas", "1m", "csv", "all", r, 1.0, 10, "ok") for r in range(1, bio.REPEATS + 1)]
        + [("pandas", "1m", "csv", "3cols", 1, 1.0, 10, "ok")]
    )
    done = bio.completed_combos(raw)
    assert ("pandas", "1m", "csv", "all") in done
    assert ("pandas", "1m", "csv", "3cols") not in done


def test_summarize_mean_std_and_excludes_failures():
    raw = _raw(
        [
            ("pandas", "1m", "csv", "all", 1, 1.0, 10, "ok"),
            ("pandas", "1m", "csv", "all", 2, 3.0, 10, "ok"),
            ("pandas", "1m", "csv", "all", 3, 99.0, 10, "timeout"),
        ]
    ).assign(frame_mb=5.0, peak_mb=8.0, partitions=None)
    summary = bio.summarize(raw)
    row = summary.iloc[0]
    assert row["seconds_mean"] == pytest.approx(2.0)
    assert row["seconds_std"] == pytest.approx(1.4142, rel=1e-3)
    assert row["n_ok"] == 2
    assert row["n_failed"] == 1


def test_summarize_handles_pandas_only_results_without_partitions():
    raw = _raw([("pandas", "1m", "csv", "all", 1, 1.0, 10, "ok")]).assign(frame_mb=5.0, peak_mb=8.0)
    summary = bio.summarize(raw)
    assert summary.iloc[0]["n_ok"] == 1


def test_check_rows_flags_mismatch():
    raw = _raw([("spark", "1m", "csv", "all", 1, 1.0, 9, "ok")])
    expected = {("1m", "csv"): 10}
    with pytest.raises(bio.BenchmarkError, match="1m"):
        bio.check_rows(raw, expected)
