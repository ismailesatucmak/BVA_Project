"""Birim testleri — src/scale_synthetic.py (Öğrenci 5)."""
import pytest

from src import benchmark_processing as bp
from src import scale_synthetic as ss


def test_replicate_writes_whole_blocks_in_order(spark):
    df = spark.createDataFrame([(i,) for i in range(5)], ["id"]).coalesce(1)
    out = [r.id for r in ss.replicate(df, 3).coalesce(1).collect()]
    assert out == [0, 1, 2, 3, 4] * 3  # blok blok, satır satır değil


def test_replicate_factor_one_is_identity(spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert ss.replicate(df, 1).count() == 2


def test_replicate_rejects_zero(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError):
        ss.replicate(df, 0)


def test_synthetic_size_is_selectable_but_not_default():
    assert ss.size_label(3) in bp.SYNTHETIC_SIZES
    args = bp.parse_args([])
    assert "x3" not in args.sizes
    assert bp.parse_args(["--sizes", "x3"]).sizes == ["x3"]


def test_spread_partitions_splits_single_row_group_file(spark, tmp_path):
    """Tek row group'lu Parquet 1 bölümde okunur; spread_partitions n dolu bölüme dağıtır."""
    from pyspark.sql import functions as F

    from src.spark_session import spread_partitions

    path = str(tmp_path / "one_rg.parquet")
    spark.range(4_000).coalesce(1).write.parquet(path)
    df = spark.read.parquet(path)

    def non_empty(frame):
        return frame.groupBy(F.spark_partition_id()).count().count()

    assert non_empty(df) == 1
    spread = spread_partitions(df, 4)
    assert non_empty(spread) == 4
    assert spread.count() == 4_000
