"""T1.5 — Okuma/bellek ölçümlerinin grafikleri (reports/figures/*.png).

Renk = format kimliği, tüm grafiklerde aynı: CSV turuncu, Parquet snappy mavi,
gzip su yeşili, zstd sarı (dataviz varsayılan paletinden, renk körlüğü doğrulamasından
geçti). Sarı ve su yeşili açık zeminde düşük kontrastlı olduğu için her seri
işaret şekliyle ve doğrudan etiketle de ayrılır; değerler notebook'ta tablo olarak da var.
"""
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psutil
from matplotlib.ticker import FuncFormatter

FORMAT_LABELS = {
    "csv": "CSV",
    "parquet_snappy": "Parquet (snappy)",
    "parquet_gzip": "Parquet (gzip)",
    "parquet_zstd": "Parquet (zstd)",
}
FORMAT_COLORS = {
    "csv": "#eb6834",
    "parquet_snappy": "#2a78d6",
    "parquet_gzip": "#1baf7a",
    "parquet_zstd": "#eda100",
}
FORMAT_MARKERS = {"csv": "o", "parquet_snappy": "s", "parquet_gzip": "^", "parquet_zstd": "D"}
# Uçtan uca grafikte Spark + CSV. Paletin ilk 3 slotu (mavi, turuncu, su yeşili) tüm
# çiftlerde doğrulanır; mor ve magenta mavi/turuncu ile ayırt edilemedi (validator FAIL).
# O grafikte gzip yok, ayrıca işaret şekli ve etiket farklı.
SPARK_COLOR = "#1baf7a"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
LINE_WIDTH = 2
MARKER_SIZE = 6
DPI = 150
MB_PER_GB = 1024

plt.rcParams.update(
    {
        "font.family": ["Segoe UI", "DejaVu Sans"],
        "font.size": 10,
        "axes.facecolor": SURFACE,
        "figure.facecolor": SURFACE,
        "axes.edgecolor": AXIS,
        "axes.labelcolor": INK_SECONDARY,
        "axes.titlecolor": INK,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.6,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "legend.frameon": False,
        "legend.labelcolor": INK_SECONDARY,
    }
)


@dataclass(frozen=True)
class LinearFit:
    slope: float
    intercept: float

    def predict(self, x):
        return self.slope * np.asarray(x) + self.intercept

    def rows_at(self, limit: float) -> float:
        return (limit - self.intercept) / self.slope


def linear_fit(x, y) -> LinearFit:
    slope, intercept = np.polyfit(np.asarray(x, dtype=float), np.asarray(y, dtype=float), 1)
    return LinearFit(float(slope), float(intercept))


def _rows_label(value, _pos=None) -> str:
    if value == 0:
        return "0"
    if value >= 1e6:
        return f"{value / 1e6:g}M"
    return f"{value / 1e3:g}K"


def _save(fig, out_dir: Path, name: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def _select(summary: pd.DataFrame, engine: str, mode: str) -> pd.DataFrame:
    return summary[(summary["engine"] == engine) & (summary["mode"] == mode)].sort_values("rows")


def _plot_format_lines(ax, data: pd.DataFrame, value: str, label_end: bool = True) -> None:
    for fmt, label in FORMAT_LABELS.items():
        series = data[data["format"] == fmt]
        ax.plot(
            series["rows"], series[value], color=FORMAT_COLORS[fmt], marker=FORMAT_MARKERS[fmt],
            linewidth=LINE_WIDTH, markersize=MARKER_SIZE, label=label,
            markeredgecolor=SURFACE, markeredgewidth=1,
        )


def plot_disk_size(summary: pd.DataFrame, out_dir: Path) -> Path:
    """Full set: format başına disk boyutu (yatay çubuk, değer etiketli)."""
    full = summary[summary["size"] == "full"].drop_duplicates("format").set_index("format")
    formats = list(FORMAT_LABELS)[::-1]
    values = [full.loc[f, "disk_mb"] for f in formats]
    fig, ax = plt.subplots(figsize=(7.5, 3.2))
    ax.barh(
        [FORMAT_LABELS[f] for f in formats], values, height=0.6,
        color=[FORMAT_COLORS[f] for f in formats], edgecolor=SURFACE, linewidth=2,
    )
    csv_mb = full.loc["csv", "disk_mb"]
    for y, (fmt, mb) in enumerate(zip(formats, values)):
        note = "" if fmt == "csv" else f"  (CSV'den {csv_mb / mb:.1f}× küçük)"
        ax.text(mb, y, f"  {mb:,.0f} MB{note}", va="center", color=INK_SECONDARY, fontsize=9)
    ax.set_xlim(0, csv_mb * 1.45)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Disk boyutu (MB)")
    ax.set_title("Disk boyutu — 19M kayıt, format başına")
    return _save(fig, out_dir, "01_disk_size.png")


def plot_read_time(summary: pd.DataFrame, out_dir: Path) -> Path:
    """Okuma süresi vs satır sayısı; pandas ve Spark yan yana, aynı log ölçek."""
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    for ax, engine, title in zip(axes, ("pandas", "spark"), ("pandas", "Spark (oturum açılışı hariç)")):
        _plot_format_lines(ax, _select(summary, engine, "all"), "seconds_mean")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.xaxis.set_major_formatter(FuncFormatter(_rows_label))
        ax.set_xlabel("Satır sayısı (log)")
        ax.set_title(title)
    axes[0].set_ylabel("Tüm kolonları okuma süresi, sn (log)")
    axes[0].legend(loc="upper left")
    fig.suptitle("Okuma süresi veri boyutuyla nasıl büyüyor?", x=0.06, ha="left", fontweight="bold", color=INK)
    fig.tight_layout()
    return _save(fig, out_dir, "02_read_time.png")


def plot_end_to_end(summary: pd.DataFrame, startup_seconds: float, out_dir: Path) -> Path:
    """Spark'ın oturum açılışı dahil edildiğinde pandas ile kesişim noktası."""
    pandas_csv = _select(summary, "pandas", "all").query("format == 'csv'")
    pandas_pq = _select(summary, "pandas", "all").query("format == 'parquet_snappy'")
    spark_csv = _select(summary, "spark", "all").query("format == 'csv'")
    lines = [
        (pandas_csv["rows"], pandas_csv["seconds_mean"], FORMAT_COLORS["csv"], "o", "pandas + CSV"),
        (pandas_pq["rows"], pandas_pq["seconds_mean"], FORMAT_COLORS["parquet_snappy"], "s", "pandas + Parquet (snappy)"),
        (spark_csv["rows"], spark_csv["seconds_mean"] + startup_seconds, SPARK_COLOR, "v", "Spark + CSV (açılış dahil)"),
    ]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for x, y, color, marker, label in lines:
        ax.plot(x, y, color=color, marker=marker, linewidth=LINE_WIDTH, markersize=MARKER_SIZE,
                markeredgecolor=SURFACE, markeredgewidth=1, label=label)
        ax.annotate(f"{y.iloc[-1]:.1f} sn", (x.iloc[-1], y.iloc[-1]), xytext=(6, 0),
                    textcoords="offset points", va="center", color=INK_SECONDARY, fontsize=9)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.xaxis.set_major_formatter(FuncFormatter(_rows_label))
    ax.set_xlabel("Satır sayısı (log)")
    ax.set_ylabel("Uçtan uca okuma süresi, sn (log)")
    ax.set_title(f"Spark'ın {startup_seconds:.1f} sn açılış maliyeti hangi boyutta geri kazanılıyor?")
    ax.legend(loc="upper left")
    return _save(fig, out_dir, "03_end_to_end.png")


def plot_pandas_memory(summary: pd.DataFrame, out_dir: Path) -> Path:
    """pandas tepe belleği + CSV için doğrusal ekstrapolasyon ve RAM sınırı."""
    ram_mb = psutil.virtual_memory().total / 1024**2
    data = _select(summary, "pandas", "all")
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    _plot_format_lines(ax, data, "peak_mb")

    csv = data[data["format"] == "csv"]
    fit = linear_fit(csv["rows"], csv["peak_mb"])
    limit_rows = fit.rows_at(ram_mb)
    x_proj = np.array([csv["rows"].max(), limit_rows])
    ax.plot(x_proj, fit.predict(x_proj), color=FORMAT_COLORS["csv"], linewidth=1.2, linestyle=(0, (4, 3)),
            label="CSV — doğrusal ekstrapolasyon")
    ax.axhline(ram_mb, color=INK_SECONDARY, linewidth=0.9)
    ax.text(limit_rows * 1.03, ram_mb, f"Toplam RAM\n{ram_mb / MB_PER_GB:.1f} GB", va="center",
            ha="left", color=INK_SECONDARY, fontsize=9)
    ax.plot([limit_rows], [ram_mb], marker="o", color=FORMAT_COLORS["csv"], markersize=MARKER_SIZE,
            markeredgecolor=SURFACE, markeredgewidth=1)
    ax.annotate(f"≈ {limit_rows / 1e6:.0f}M kayıtta\nRAM dolar", (limit_rows, ram_mb), xytext=(0, -38),
                textcoords="offset points", ha="center", va="top", color=INK, fontsize=9)
    ax.xaxis.set_major_formatter(FuncFormatter(_rows_label))
    ax.set_yticks(np.arange(0, ram_mb + 1, 2 * MB_PER_GB))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v / MB_PER_GB:.0f} GB"))
    ax.set_xlim(0, limit_rows * 1.18)
    ax.set_ylim(0, ram_mb * 1.08)
    ax.set_xlabel("Satır sayısı")
    ax.set_ylabel("Okuma sırasındaki tepe bellek")
    ax.set_title("pandas tüm tabloyu RAM'e alır: tepe bellek satır sayısıyla doğrusal artar")
    ax.legend(loc="center left", bbox_to_anchor=(0.0, 0.62))
    return _save(fig, out_dir, "04_pandas_memory.png")


def plot_column_pruning(summary: pd.DataFrame, out_dir: Path) -> Path:
    """Full set: 3 kolon okumanın tüm kolonlara göre hızlanma katsayısı (pandas | Spark)."""
    full = summary[summary["size"] == "full"]
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), sharey=True)
    formats = list(FORMAT_LABELS)
    for ax, engine in zip(axes, ("pandas", "spark")):
        rows = full[full["engine"] == engine].set_index(["format", "mode"])["seconds_mean"]
        speedup = [rows[(f, "all")] / rows[(f, "3cols")] for f in formats]
        ax.bar(range(len(formats)), speedup, width=0.6, color=[FORMAT_COLORS[f] for f in formats],
               edgecolor=SURFACE, linewidth=2)
        for i, value in enumerate(speedup):
            ax.text(i, value, f"{value:.1f}×", ha="center", va="bottom", color=INK_SECONDARY, fontsize=9)
        ax.set_xticks(range(len(formats)), [FORMAT_LABELS[f].replace(" (", "\n(") for f in formats])
        ax.axhline(1, color=AXIS, linewidth=0.9)
        ax.grid(axis="x", visible=False)
        ax.set_title(engine if engine == "pandas" else "Spark")
    axes[0].set_ylabel("Hızlanma (tüm kolon / 3 kolon)")
    fig.suptitle("Column pruning: 20 yerine 3 kolon okumak ne kadar hızlandırıyor? (19M kayıt)",
                 x=0.06, ha="left", fontweight="bold", color=INK)
    fig.tight_layout()
    return _save(fig, out_dir, "05_column_pruning.png")
