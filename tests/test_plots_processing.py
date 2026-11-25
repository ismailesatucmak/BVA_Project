import pandas as pd
import pytest

from src import plots_processing as pp
from src.benchmark_processing import OPERATIONS, SETUP_STEP


def _summary(spark_fixed=10.0):
    """pandas süresi kayıtla doğrusal, Spark sabit + küçük eğim → kesişim var."""
    recs = []
    for rows in (1e5, 1e6, 5e6, 1e7, 1.9e7):
        for fmt in pp.FORMAT_STYLE:
            for engine in ("pandas", "spark"):
                setup = rows / 1e6 if engine == "pandas" else spark_fixed
                recs.append({"engine": engine, "size": str(rows), "format": fmt,
                             "op": SETUP_STEP[engine], "rows": rows, "seconds_mean": setup,
                             "peak_rss_mb": 100 + rows / 2000})
                for op in OPERATIONS:
                    sec = rows / 1e6 if engine == "pandas" else 0.5 + rows / 2e7
                    recs.append({"engine": engine, "size": str(rows), "format": fmt, "op": op,
                                 "rows": rows, "seconds_mean": sec,
                                 "peak_rss_mb": 100 + rows / 1000})
    return pd.DataFrame(recs)


def test_crossover_found_between_sizes():
    from src.benchmark_processing import end_to_end

    x = pp.crossover_rows(end_to_end(_summary()), "csv")
    assert 1e6 < x < 1e7


def test_crossover_none_when_spark_never_faster():
    from src.benchmark_processing import end_to_end

    assert pp.crossover_rows(end_to_end(_summary(spark_fixed=1e6)), "csv") is None


def test_memory_per_row_slope():
    slopes = pp.memory_per_row_kb(_summary())
    assert slopes["csv"] == pytest.approx(1024 / 1000)


@pytest.mark.parametrize("plot", [pp.plot_op_time, pp.plot_end_to_end, pp.plot_memory])
def test_figures_render(tmp_path, plot):
    path = plot(_summary(), tmp_path)
    assert path.exists() and path.stat().st_size > 0
