"""S5 grafikleri (Şekil 21–23) — ``reports/results/s5_processing.csv`` üzerinden.

Renk motoru, çizgi stili formatı taşır (renk tek başına kimlik taşımaz):
  pandas = turuncu, Spark = mavi · CSV = kesikli, Parquet = düz.
Palet dataviz doğrulayıcısından geçti (CVD ΔE 24,7; normal görüş ΔE 33,6).

Çalıştırma:  python -m src.plots_processing
"""
from __future__ import annotations

import logging
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import psutil  # noqa: E402

from src import config  # noqa: E402
from src.benchmark_processing import OPERATIONS, SUMMARY_FILE, end_to_end  # noqa: E402

logger = logging.getLogger(__name__)

ENGINE_COLOR = {"pandas": "#eb6834", "spark": "#2a78d6"}
ENGINE_LABEL = {"pandas": "pandas", "spark": "Spark"}
FORMAT_STYLE = {"csv": ("--", "o"), "parquet_snappy": ("-", "s")}
FORMAT_LABEL = {"csv": "CSV", "parquet_snappy": "Parquet"}
OP_TITLE = {
    "groupby_hour": "groupby: saate göre özet",
    "join_borough": "join: bölge tablosu → ilçe",
    "window_top3": "window: bölge başına ilk 3",
}
INK = "#52514e"


def _style(ax, ylabel: str, logy: bool = True) -> None:
    ax.set_xscale("log")
    if logy:
        ax.set_yscale("log")
    ax.set_xlabel("Kayıt sayısı (log)", color=INK)
    ax.set_ylabel(ylabel, color=INK)
    ax.grid(True, which="major", color="#e4e3df", linewidth=0.8)
    ax.tick_params(colors=INK)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def _plot_series(ax, frame: pd.DataFrame, ycol: str) -> None:
    for (engine, fmt), grp in frame.groupby(["engine", "format"]):
        grp = grp.sort_values("rows")
        ls, marker = FORMAT_STYLE[fmt]
        ax.plot(grp["rows"], grp[ycol], color=ENGINE_COLOR[engine], linestyle=ls,
                marker=marker, markersize=6, linewidth=2,
                label=f"{ENGINE_LABEL[engine]} + {FORMAT_LABEL[fmt]}")


def crossover_rows(e2e: pd.DataFrame, fmt: str, col: str = "total_s") -> float | None:
    """Spark'ın pandas'tan hızlı olmaya başladığı kayıt sayısı (log-log doğrusal ara değer).

    Spark hiçbir boyutta öne geçmiyorsa None; en küçük boyutta zaten öndeyse o boyut.
    """
    p = e2e[(e2e["engine"] == "pandas") & (e2e["format"] == fmt)].set_index("rows")[col]
    s = e2e[(e2e["engine"] == "spark") & (e2e["format"] == fmt)].set_index("rows")[col]
    rows = sorted(set(p.index) & set(s.index))
    diff = [math.log(s[r]) - math.log(p[r]) for r in rows]  # < 0 → Spark hızlı
    for i, d in enumerate(diff):
        if d < 0:
            if i == 0:
                return float(rows[0])
            x0, x1 = math.log(rows[i - 1]), math.log(rows[i])
            d0, d1 = diff[i - 1], d
            return float(math.exp(x0 + (x1 - x0) * d0 / (d0 - d1)))
    return None


def plot_op_time(summary: pd.DataFrame, out_dir: Path) -> Path:
    fig, axes = plt.subplots(1, len(OPERATIONS), figsize=(16, 5), sharey=True)
    for ax, op in zip(axes, OPERATIONS):
        _plot_series(ax, summary[summary["op"] == op], "seconds_mean")
        _style(ax, "İşlem süresi (sn, log)" if ax is axes[0] else "")
        ax.set_title(OP_TITLE[op], fontsize=11)
    axes[0].legend(fontsize=9, frameon=False)
    fig.suptitle("Şekil 21 · İşlem süresi vs veri boyutu "
                 "(pandas: bellekteki tablo · Spark: dosyadan okuyarak)", fontsize=12)
    fig.tight_layout()
    path = out_dir / "21_s5_op_time.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_end_to_end(summary: pd.DataFrame, out_dir: Path) -> Path:
    e2e = end_to_end(summary)
    fig, ax = plt.subplots(figsize=(9, 6))
    _plot_series(ax, e2e, "total_s")
    _style(ax, "Uçtan uca süre (sn, log)")
    for fmt in FORMAT_STYLE:
        x = crossover_rows(e2e, fmt)
        if x is not None and x > e2e["rows"].min():
            ax.axvline(x, color=INK, linestyle=FORMAT_STYLE[fmt][0], linewidth=1, alpha=0.6)
            ax.annotate(f"{FORMAT_LABEL[fmt]}: Spark öne geçer ≈ {x / 1e6:.1f}M kayıt",
                        xy=(x, ax.get_ylim()[0]), xytext=(-6, 10 if fmt == "csv" else 26),
                        textcoords="offset points", fontsize=9, color=INK, ha="right")
    ax.legend(fontsize=9, frameon=False, loc="upper left")
    ax.set_title("Şekil 22 · Uçtan uca süre: pandas (yükleme + 3 işlem) vs "
                 "Spark (oturum açılışı + 3 işlem)", fontsize=11)
    fig.tight_layout()
    path = out_dir / "22_s5_end_to_end.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_memory(summary: pd.DataFrame, out_dir: Path) -> Path:
    e2e = end_to_end(summary)
    total_mb = psutil.virtual_memory().total / 1024**2
    fig, ax = plt.subplots(figsize=(9, 6))
    _plot_series(ax, e2e, "peak_rss_mb")
    _style(ax, "Tepe RSS (MB) — Spark: Python + JVM", logy=False)
    peak = e2e["peak_rss_mb"].max()
    ax.set_ylim(0, peak * 1.15)
    ax.legend(fontsize=9, frameon=False, loc="upper left")
    ax.set_title("Şekil 23 · Tepe bellek vs veri boyutu (yükleme ve 3 işlem boyunca)\n"
                 f"en yüksek değer {peak / 1024:.1f} GB".replace(".", ",") + " = fiziksel RAM'in "
                 f"%{100 * peak / total_mb:.0f}'i ({total_mb / 1024:.0f} GB)", fontsize=11)
    fig.tight_layout()
    path = out_dir / "23_s5_memory.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def memory_per_row_kb(summary: pd.DataFrame, engine: str = "pandas") -> dict[str, float]:
    """Format başına tepe RSS'in kayıt sayısına doğrusal eğimi (KB / kayıt)."""
    e2e = end_to_end(summary)
    out = {}
    for fmt, grp in e2e[e2e["engine"] == engine].groupby("format"):
        slope = np.polyfit(grp["rows"], grp["peak_rss_mb"], 1)[0]
        out[fmt] = float(slope * 1024)
    return out


def make_all(out_dir: Path = config.FIGURES_DIR) -> list[Path]:
    summary = pd.read_csv(config.RESULTS_DIR / SUMMARY_FILE)
    out_dir.mkdir(parents=True, exist_ok=True)
    return [plot_op_time(summary, out_dir), plot_end_to_end(summary, out_dir),
            plot_memory(summary, out_dir)]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for p in make_all():
        logger.info("kaydedildi: %s", p)
