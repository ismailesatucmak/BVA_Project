import matplotlib

matplotlib.use("Agg")

import pandas as pd
import pytest

from src import plots_io as pio


def test_linear_fit_recovers_line():
    fit = pio.linear_fit([1, 2, 3], [12, 22, 32])
    assert fit.slope == pytest.approx(10)
    assert fit.intercept == pytest.approx(2)


def test_rows_at_limit_inverts_fit():
    fit = pio.LinearFit(slope=10.0, intercept=2.0)
    assert fit.rows_at(102.0) == pytest.approx(10.0)


def _summary():
    rows = {"100k": 1e5, "1m": 1e6, "5m": 5e6, "10m": 1e7, "full": 1.9e7}
    records = []
    for size, n in rows.items():
        for fmt in pio.FORMAT_LABELS:
            for engine in ("pandas", "spark"):
                for mode in ("all", "3cols"):
                    records.append(
                        {
                            "engine": engine, "size": size, "format": fmt, "mode": mode, "rows": n,
                            "seconds_mean": n / 1e6 * (2 if mode == "all" else 1),
                            "peak_mb": n / 2000 if engine == "pandas" else None,
                            "disk_mb": n / 1e4,
                        }
                    )
    return pd.DataFrame(records)


@pytest.mark.parametrize(
    "plot",
    [pio.plot_disk_size, pio.plot_read_time, pio.plot_pandas_memory, pio.plot_column_pruning],
)
def test_figures_render_and_save(tmp_path, plot):
    path = plot(_summary(), tmp_path)
    assert path.exists() and path.stat().st_size > 0


def test_end_to_end_figure_renders(tmp_path):
    path = pio.plot_end_to_end(_summary(), startup_seconds=11.0, out_dir=tmp_path)
    assert path.exists()
