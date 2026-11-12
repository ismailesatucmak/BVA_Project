"""Mimari şemayı üretir → docs/architecture.png

Çalıştırma:  python scripts/make_architecture.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from src.plots_io import INK, INK_SECONDARY, MUTED, SURFACE  # ortak stil

S1, S2, SHARED, OTHERS = "#2a78d6", "#eb6834", "#52514e", "#898781"
FILL = {S1: "#e3eefb", S2: "#fde6dc", SHARED: "#efeeea", OTHERS: "#f6f6f4"}
W, H = 3.2, 1.05


def box(ax, x, y, title, body, color, w=W, h=H):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                facecolor=FILL[color], edgecolor=color, linewidth=1.6))
    ax.text(x + 0.14, y + h - 0.2, title, fontsize=10, fontweight="bold", color=INK, va="top")
    ax.text(x + 0.14, y + h - 0.5, body, fontsize=8.2, color=INK_SECONDARY, va="top", linespacing=1.35)
    return (x, y, w, h)


def arrow(ax, a, b, label=None, color=MUTED, start="right", end="left"):
    def anchor(r, side):
        x, y, w, h = r
        return {"right": (x + w, y + h / 2), "left": (x, y + h / 2),
                "top": (x + w / 2, y + h), "bottom": (x + w / 2, y)}[side]
    p, q = anchor(a, start), anchor(b, end)
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle="-|>", mutation_scale=12, color=color, linewidth=1.3,
                                 connectionstyle="arc3,rad=0"))
    if label:
        ax.text((p[0] + q[0]) / 2, (p[1] + q[1]) / 2 + 0.1, label, fontsize=7.5, color=MUTED, ha="center")


def main() -> Path:
    fig, ax = plt.subplots(figsize=(15, 9))
    ax.set_xlim(0, 15.2)
    ax.set_ylim(0, 9.2)
    ax.axis("off")
    fig.patch.set_facecolor(SURFACE)

    src = box(ax, 0.2, 3.6, "NYC TLC", "Yellow Taxi 2026-01..05\n5 × Parquet (ZSTD)\n+ zone lookup CSV", SHARED)
    ingest = box(ax, 3.8, 3.6, "src.ingest  (S1)", "indir / atla / doğrula\nşema + kayıt sayısı\n→ data/raw  ·  19,0M kayıt", S1)
    arrow(ax, src, ingest)

    # Öğrenci 1 kolu (üst)
    dic = box(ax, 7.5, 6.9, "src.dictionary_stats  (S1)", "Spark: tip, null, min/max,\nkod dağılımları → veri sözlüğü", S1)
    sizes = box(ax, 7.5, 5.4, "src.build_sizes  (S1)", "xxhash64 iç içe örneklem\n100K·1M·5M·10M·full × CSV +\nParquet snappy/gzip/zstd", S1, h=1.2)
    bench = box(ax, 11.4, 5.4, "src.benchmark_io  (S1)", "pandas vs Spark okuma,\ntepe bellek, 3 tekrar\n→ 85 kombinasyon", S1, h=1.2)
    store = box(ax, 11.4, 6.9, "D:\\BVAVeri\\processed", "20 dosya · 5,2 GB\n(NYC_DATA_ROOT)", S1)
    arrow(ax, ingest, dic, start="top", end="left")
    arrow(ax, ingest, sizes, start="top", end="left")
    arrow(ax, sizes, store, start="right", end="left")
    arrow(ax, store, bench, start="bottom", end="top")

    # Öğrenci 2 kolu (orta)
    prof = box(ax, 7.5, 3.6, "src.profile_data  (S2)", "yüzdelikler + 14 kontrol\n(src.quality, ortak tanım)", S2)
    cln = box(ax, 11.4, 3.6, "src.clean  (S2)", "R1–R10, R9 = window\n17,2M kayıt (%90,6)", S2)
    arrow(ax, ingest, prof)
    arrow(ax, prof, cln, "eşikler")

    anom = box(ax, 7.5, 1.85, "src.anomaly  (S2)", "kural vs IQR/p99.9\nwindow: güzergâh bağlamı\njoin: zone lookup", S2, h=1.2)
    arrow(ax, ingest, anom, start="bottom", end="left")
    clean_store = box(ax, 11.4, 1.85, "data/clean", "Parquet · pickup_year/month\n5 dosya · 479 MB\n+ is_flex_fare, süre, hız", S2, h=1.2)
    arrow(ax, cln, clean_store, start="bottom", end="top")
    arrow(ax, clean_store, anom, start="left", end="right")

    # Diğer öğrenciler (alt)
    for i, (title, body) in enumerate((("Öğrenci 3", "aggregation"), ("Öğrenci 4", "ML modeli"), ("Öğrenci 5", "işleme benchmark\n+ dashboard"))):
        o = box(ax, 7.5 + i * 2.55, 0.15, title, body, OTHERS, w=2.35, h=1.0)
        arrow(ax, clean_store, o, start="bottom", end="top", color=OTHERS)

    box(ax, 0.2, 0.9, "Tüm adımların çıktıları", "reports/results/*.csv (ölçümler)\nreports/figures/01–10\n"
        "notebooks/01, 02 (yorum)\ndocs/ (sözlük, kurallar)", SHARED, h=1.35)

    ax.text(0.2, 9.1, "NYC Taxi Büyük Veri Analitiği — Veri akışı", fontsize=14, fontweight="bold", color=INK, va="top")
    ax.text(0.2, 8.6, "Mavi: Öğrenci 1 (temin, sözlük, format, boyut analizi)   ·   Turuncu: Öğrenci 2 (temizlik, anomali)   ·   "
            "Gri: ortak / diğer öğrenciler.   Tüm ağır işler PySpark 3.5 (yerel, 28 çekirdek).",
            fontsize=8.5, color=INK_SECONDARY, va="top")

    path = ROOT / "docs" / "architecture.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    return path


if __name__ == "__main__":
    print(main())
