import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src import build_sizes as bs


def test_sample_threshold_is_fraction_of_total():
    assert bs.sample_threshold(1_000, 10_000) == pytest.approx(0.1)


def test_sample_threshold_full_size_keeps_everything():
    assert bs.sample_threshold(None, 10_000) == 1.0


def test_sample_threshold_never_exceeds_one():
    assert bs.sample_threshold(50_000, 10_000) == 1.0


def test_target_path_layout(tmp_path):
    assert bs.target_path(tmp_path, "1m", "csv") == tmp_path / "size_1m" / "csv" / "yellow.csv"
    assert (
        bs.target_path(tmp_path, "full", "parquet_zstd")
        == tmp_path / "size_full" / "parquet_zstd" / "yellow.parquet"
    )


def test_finalize_moves_single_part_and_removes_spark_leftovers(tmp_path):
    spark_out = tmp_path / "_tmp"
    spark_out.mkdir()
    (spark_out / "part-00000-abc.csv").write_text("a\n1\n")
    (spark_out / ".part-00000-abc.csv.crc").write_text("x")
    (spark_out / "_SUCCESS").write_text("")
    dest = tmp_path / "yellow.csv"

    bs.finalize_single_file(spark_out, dest)

    assert dest.read_text() == "a\n1\n"
    assert not spark_out.exists()


def test_finalize_rejects_multiple_parts(tmp_path):
    spark_out = tmp_path / "_tmp"
    spark_out.mkdir()
    (spark_out / "part-00000.csv").write_text("a")
    (spark_out / "part-00001.csv").write_text("b")
    with pytest.raises(bs.BuildError, match="2"):
        bs.finalize_single_file(spark_out, tmp_path / "yellow.csv")


def test_count_rows_csv_excludes_header(tmp_path):
    path = tmp_path / "yellow.csv"
    path.write_text("a,b\n1,2\n3,4\n5,6\n")
    assert bs.count_rows(path) == 3


def test_count_rows_parquet_uses_metadata(tmp_path):
    path = tmp_path / "yellow.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3, 4]}), path)
    assert bs.count_rows(path) == 4


def test_check_consistency_passes_when_formats_agree():
    manifest = pd.DataFrame({"size": ["1m", "1m"], "format": ["csv", "parquet_gzip"], "rows": [10, 10]})
    bs.check_consistency(manifest)


def test_check_consistency_fails_on_row_mismatch():
    manifest = pd.DataFrame({"size": ["1m", "1m"], "format": ["csv", "parquet_gzip"], "rows": [10, 9]})
    with pytest.raises(bs.BuildError, match="1m"):
        bs.check_consistency(manifest)
