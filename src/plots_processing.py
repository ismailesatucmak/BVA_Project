"""S5 grafikleri (Şekil 21–23) — ``reports/results/s5_processing.csv`` üzerinden.

Renk motoru, çizgi stili formatı taşır (renk tek başına kimlik taşımaz):
  pandas = turuncu, Spark = mavi, Spark + cache = yeşil · CSV = kesikli, Parquet = düz.
Palet dataviz doğrulayıcısından geçti (3 renk; en kötü komşu CVD ΔE 23,1, normal görüş 24,0;
yeşilin zemine kontrastı < 3:1 → lejant + çizgi stili + dashboard tablo görünümü).

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
from src.benchmark_processing import LAYOUT_SIZES, OPERATIONS, SUMMARY_FILE, end_to_end  # noqa: E402

logger = logging.getLogger(__name__)

ENGINE_COLOR = {"pandas": "#eb6834", "spark": "#2a78d6", "spark_cache": "#1baf7a"}
ENGINE_LABEL = {"pandas": "pandas", "spark": "Spark", "spark_cache": "Spark + cache"}
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


SYNTHETIC_MIN_ROWS = 2e7  # 19M üstü = sentetik ×3 (src.scale_synthetic)


def _mark_synthetic(ax, frame: pd.DataFrame) -> None:
    """Sentetik bölgeyi (19M üstü) açık gri bant ve etiketle işaretler."""
    big = frame[frame["rows"] > SYNTHETIC_MIN_ROWS]["rows"]
    if big.empty:
        return
    ax.axvspan(SYNTHETIC_MIN_ROWS * 1.2, big.max() * 1.6, color="#f1f0ec", zorder=0)
    ax.text(big.max(), 1.0, "sentetik ×3", transform=ax.get_xaxis_transform(), ha="center",
            va="bottom", fontsize=8, color=INK)


def _plot_series(ax, frame: pd.DataFrame, ycol: str) -> None:
    for (engine, fmt), grp in frame.groupby(["engine", "format"]):
        grp = grp.sort_values("rows")
        ls, marker = FORMAT_STYLE[fmt]
        ax.plot(grp["rows"], grp[ycol], color=ENGINE_COLOR[engine], linestyle=ls,
                marker=marker, markersize=6, linewidth=2,
                label=f"{ENGINE_LABEL[engine]} + {FORMAT_LABEL[fmt]}")


def crossover_rows(e2e: pd.DataFrame, fmt: str, col: str = "total_s",
                   spark_engine: str = "spark") -> float | None:
    """Spark'ın pandas'tan hızlı olmaya başladığı kayıt sayısı (log-log doğrusal ara değer).

    Spark hiçbir boyutta öne geçmiyorsa None; en küçük boyutta zaten öndeyse o boyut.
    """
    p = e2e[(e2e["engine"] == "pandas") & (e2e["format"] == fmt)].set_index("rows")[col]
    s = e2e[(e2e["engine"] == spark_engine) & (e2e["format"] == fmt)].set_index("rows")[col]
    rows = sorted(set(p.index) & set(s.index))
    if not rows:
        return None
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
        _mark_synthetic(ax, summary)
        _style(ax, "İşlem süresi (sn, log)" if ax is axes[0] else "")
        ax.set_title(OP_TITLE[op], fontsize=11)
    axes[0].legend(fontsize=9, frameon=False)
    fig.suptitle("Şekil 21 · İşlem süresi vs veri boyutu "
                 "(pandas ve Spark + cache: bellekteki tablo · Spark: dosyadan okuyarak)", fontsize=12)
    fig.tight_layout()
    path = out_dir / "21_s5_op_time.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_end_to_end(summary: pd.DataFrame, out_dir: Path) -> Path:
    e2e = end_to_end(summary)
    fig, ax = plt.subplots(figsize=(9, 6))
    _plot_series(ax, e2e, "total_s")
    _mark_synthetic(ax, e2e)
    _style(ax, "Uçtan uca süre (sn, log)")
    for fmt in FORMAT_STYLE:
        x = crossover_rows(e2e, fmt)
        if x is not None and x > e2e["rows"].min():
            ax.axvline(x, color=INK, linestyle=FORMAT_STYLE[fmt][0], linewidth=1, alpha=0.6)
            ax.annotate(f"{FORMAT_LABEL[fmt]}: Spark öne geçer ≈ {x / 1e6:.1f}M kayıt".replace(".", ","),
                        xy=(x, ax.get_ylim()[0]), xytext=(-6, 10 if fmt == "csv" else 26),
                        textcoords="offset points", fontsize=9, color=INK, ha="right")
    ax.legend(fontsize=9, frameon=False, loc="upper left")
    ax.set_title("Şekil 22 · Uçtan uca süre: pandas (yükleme + 3 işlem) · Spark (açılış + 3 işlem)"
                 "\n· Spark + cache (açılış + önbelleğe yükleme + 3 işlem)", fontsize=11)
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
    _mark_synthetic(ax, e2e)
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


BREAKDOWN_STEPS = [("startup", "Spark oturum açılışı", "#a3a29d"),
                   ("load", "Yükleme (pandas) / önbelleğe alma (Spark)", "#257de0"),
                   ("groupby_hour", "groupby", "#f47230"),
                   ("join_borough", "join", "#00b894"),
                   ("window_top3", "window", "#f2a600")]
BREAKDOWN_SIZES = {"full": "19M", "x3": "57M (sentetik ×3)"}


def plot_time_breakdown(summary: pd.DataFrame, out_dir: Path) -> Path:
    """Uçtan uca sürenin adımlara dökümü (yatay yığılmış çubuk), 19M ve varsa 57M."""
    sizes = [s for s in BREAKDOWN_SIZES if s in set(summary["size"])]
    if not sizes:  # 19M yoksa en büyük boyut
        sizes = [summary.sort_values("rows")["size"].iloc[-1]]
    fig, axes = plt.subplots(1, len(sizes), figsize=(8 * len(sizes), 5.5), squeeze=False)
    for ax, size in zip(axes[0], sizes):
        sub = summary[summary["size"] == size]
        combos = [(e, f) for e in ENGINE_LABEL for f in FORMAT_LABEL
                  if not sub[(sub["engine"] == e) & (sub["format"] == f)].empty]
        labels = [f"{ENGINE_LABEL[e]} + {FORMAT_LABEL[f]}" for e, f in combos]
        left = np.zeros(len(combos))
        for step, step_label, color in BREAKDOWN_STEPS:
            vals = []
            for e, f in combos:
                v = sub[(sub["engine"] == e) & (sub["format"] == f) & (sub["op"] == step)]["seconds_mean"]
                vals.append(float(v.iloc[0]) if len(v) else 0.0)
            vals = np.array(vals)
            ax.barh(labels, vals, left=left, color=color, label=step_label,
                    edgecolor="white", linewidth=2)
            left += vals
        for y, total in enumerate(left):
            ax.text(total, y, f"  {total:.1f} sn".replace(".", ","), va="center",
                    fontsize=9, color=INK)
        ax.invert_yaxis()
        ax.set_xlim(0, left.max() * 1.18)
        ax.set_xlabel("Süre (sn)", color=INK)
        ax.set_title(BREAKDOWN_SIZES.get(size, size), fontsize=11)
        ax.grid(True, axis="x", color="#e4e3df", linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=9, frameon=False, loc="lower center",
               ncol=len(BREAKDOWN_STEPS), bbox_to_anchor=(0.5, -0.04))
    fig.suptitle("Şekil 26 · Uçtan uca sürenin dökümü: süre nereye gidiyor?", fontsize=12)
    fig.tight_layout()
    path = out_dir / "26_s5_time_breakdown.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def main_curves(summary: pd.DataFrame) -> pd.DataFrame:
    """Boyut eğrileri: dosya düzeni kontrolü (19M ile aynı satır sayısı) hariç."""
    return summary[~summary["size"].isin(LAYOUT_SIZES)]


def memory_per_row_kb(summary: pd.DataFrame, engine: str = "pandas") -> dict[str, float]:
    """Format başına tepe RSS'in kayıt sayısına doğrusal eğimi (KB / kayıt)."""
    e2e = end_to_end(summary)
    out = {}
    for fmt, grp in e2e[e2e["engine"] == engine].groupby("format"):
        slope = np.polyfit(grp["rows"], grp["peak_rss_mb"], 1)[0]
        out[fmt] = float(slope * 1024)
    return out


def make_all(out_dir: Path = config.FIGURES_DIR) -> list[Path]:
    summary = main_curves(pd.read_csv(config.RESULTS_DIR / SUMMARY_FILE))
    out_dir.mkdir(parents=True, exist_ok=True)
    return [plot_op_time(summary, out_dir), plot_end_to_end(summary, out_dir),
            plot_memory(summary, out_dir), plot_time_breakdown(summary, out_dir)]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for p in make_all():
        logger.info("kaydedildi: %s", p)
