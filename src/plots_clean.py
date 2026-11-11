"""T2.5 — Temizlik ve anomali grafikleri (reports/figures/06–10_*.png).

Stil ve palet src/plots_io.py ile ortak. Renkler paletin ilk üç slotundan (mavi, turuncu,
su yeşili — tüm çiftlerde renk körlüğü doğrulamasından geçer); ısı haritası tek tonlu mavi.
"""
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

from src.plots_io import (
    AXIS, INK, INK_SECONDARY, LINE_WIDTH, MUTED, SURFACE, _save,
)

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
METRIC_LABELS = {"trip_distance": "Mesafe (mil)", "fare_amount": "Ücret ($)", "trip_duration_min": "Süre (dk)"}
RULE_ORDER = ["R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", "R10"]


def _suptitle(fig, text: str) -> None:
    fig.suptitle(text, x=0.02, ha="left", fontweight="bold", color=INK)


def plot_rule_impact(impact: pd.DataFrame, out_dir: Path) -> Path:
    """Kural başına tek başına ve sıralı silinen kayıt (yatay çubuk)."""
    rules = impact[impact["rule"].isin(RULE_ORDER)].set_index("rule").loc[RULE_ORDER[::-1]]
    total = impact.set_index("rule").loc["TOTAL"]
    naive = impact.set_index("rule").loc["NAIVE"]
    y = np.arange(len(rules))
    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.barh(y + 0.2, rules["isolated"], height=0.38, color=ORANGE, edgecolor=SURFACE, linewidth=2, label="Tek başına yakaladığı")
    ax.barh(y - 0.2, rules["sequential"], height=0.38, color=BLUE, edgecolor=SURFACE, linewidth=2, label="Sırayla uygulanınca sildiği")
    for yi, (rule, row) in zip(y, rules.iterrows()):
        ax.text(max(row["isolated"], row["sequential"]), yi, f"  {row['sequential']:,.0f}", va="center",
                color=INK_SECONDARY, fontsize=9)
    ax.set_yticks(y, [f"{r}  {rules.loc[r, 'description']}" for r in rules.index], fontsize=9)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: f"{v / 1e3:,.0f}K"))
    ax.set_xlim(0, rules["isolated"].max() * 1.22)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Kayıt sayısı")
    ax.legend(loc="lower right")
    ax.set_title(f"Silinen: {total['sequential']:,.0f} kayıt (%{total['sequential_pct']:.1f})  ·  "
                 f"naif dropna() olsaydı: {naive['sequential']:,.0f} (%{naive['sequential_pct']:.1f})",
                 fontsize=10, fontweight="normal", color=INK_SECONDARY)
    _suptitle(fig, "Temizlik kurallarının etkisi — kurallar örtüşüyor")
    fig.tight_layout()
    return _save(fig, out_dir, "06_rule_impact.png")


def plot_before_after(raw: pd.DataFrame, clean: pd.DataFrame, out_dir: Path) -> Path:
    """Mesafe / ücret / süre dağılımı: ham vs temiz (log-x yoğunluk; ≤0 değerler not edilir)."""
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.9))
    for ax, metric in zip(axes, METRIC_LABELS):
        r, c = raw[metric].dropna(), clean[metric].dropna()
        positive = pd.concat([r[r > 0], c[c > 0]])
        bins = np.logspace(np.log10(positive.quantile(0.0005)), np.log10(positive.max()), 70)
        for data, color, label in ((r, ORANGE, "Ham"), (c, BLUE, "Temiz")):
            ax.hist(data[data > 0], bins=bins, density=True, histtype="step", linewidth=LINE_WIDTH, color=color, label=label)
        ax.set_xscale("log")
        ax.set_yticks([])
        ax.grid(axis="y", visible=False)
        nonpos = (r <= 0).mean() * 100
        ax.set_title(f"{METRIC_LABELS[metric]}", fontsize=11)
        ax.text(0.98, 0.95, f"ham ≤ 0: %{nonpos:.1f}\nham max: {r.max():,.0f}\ntemiz max: {c.max():,.0f}",
                transform=ax.transAxes, ha="right", va="top", color=INK_SECONDARY, fontsize=8.5)
        ax.set_xlabel("log ölçek")
    axes[0].legend(loc="upper left")
    _suptitle(fig, "Temizlik öncesi / sonrası dağılımlar (%1 örneklem) — gövde aynı, kuyruklar kesildi")
    fig.tight_layout()
    return _save(fig, out_dir, "07_before_after.png")


def plot_fare_vs_distance(sample: pd.DataFrame, out_dir: Path) -> Path:
    """Temiz veride ücret–mesafe; güzergâh bağlamına göre ücret anomalileri vurgulu."""
    normal = sample[~sample["ctx_fare"]]
    odd = sample[sample["ctx_fare"]]
    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    ax.scatter(normal["trip_distance"], normal["fare_amount"], s=3, color=BLUE, alpha=0.12, linewidths=0,
               label=f"Normal ({len(normal):,})", rasterized=True)
    ax.scatter(odd["trip_distance"], odd["fare_amount"], s=9, color=ORANGE, alpha=0.8, linewidths=0,
               label=f"Güzergâhına göre anomali ({len(odd):,})", rasterized=True)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_ylim(bottom=1)  # birkaç 0,01 $ "yer tutucu" ücret ekseni gereksiz uzatıyor (bkz. tutarlılık tablosu)
    ax.set_xlabel("Mesafe (mil, log)")
    ax.set_ylabel("Ücret ($, log)")
    ax.legend(loc="upper left", markerscale=3)
    ax.set_title("Ücret–mesafe: anomaliler globalde değil, kendi güzergâhında aykırı (%1 örneklem)")
    return _save(fig, out_dir, "08_fare_vs_distance.png")


def plot_rule_rates_heatmap(rates: pd.DataFrame, out_dir: Path) -> Path:
    """Sağlayıcı × kural: kaydın yüzde kaçı o kurala takılıyor (anomalinin kaynağı)."""
    vendor_names = {"1": "1 · Creative Mobile", "2": "2 · Curb", "6": "6 · Myle", "7": "7 · Helix"}
    v = rates[rates["dimension"] == "VendorID"].copy()
    v["value"] = v["value"].astype(str)
    v = v.set_index("value").loc[["1", "2", "6", "7"]]
    rules = [r for r in RULE_ORDER if r in v.columns]
    data = v[rules].to_numpy(dtype=float)
    cmap = LinearSegmentedColormap.from_list("blue_seq", BLUE_RAMP)
    fig, ax = plt.subplots(figsize=(9, 3.4))
    ax.imshow(np.log1p(data), cmap=cmap, aspect="auto")
    for (i, j), value in np.ndenumerate(data):
        color = "#ffffff" if np.log1p(value) > np.log1p(data.max()) * 0.55 else INK
        ax.text(j, i, f"{value:.1f}" if value else "·", ha="center", va="center", color=color, fontsize=9)
    ax.set_xticks(range(len(rules)), rules)
    ax.set_yticks(range(len(v)), [f"{vendor_names[k]}  ({v.loc[k, 'rows'] / 1e6:.2f}M)" for k in v.index])
    ax.grid(visible=False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(length=0)
    ax.set_title("Kayıtların yüzde kaçı her kurala takılıyor? (sağlayıcı başına, ham veri)")
    fig.text(0.02, -0.04, "Renk log ölçekli; değerler %. Helix'in tamamı R4'e (süre 0) takılıyor: iniş zamanı gönderilmiyor.",
             color=MUTED, fontsize=8.5)
    return _save(fig, out_dir, "09_rule_rates_vendor.png")


def plot_rule_vs_stat(comparison: pd.DataFrame, out_dir: Path) -> Path:
    """Kural ile istatistiksel eşiklerin örtüşmesi: yalnız kural / ikisi / yalnız istatistik."""
    rows = comparison.copy()
    rows["label"] = rows["metric"].map(METRIC_LABELS) + " · " + rows["rule"] + " vs " + rows["stat_method"]
    rows = rows.iloc[::-1].reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(10, 4.2))
    left = np.zeros(len(rows))
    for col, color, label in (("rule_only", BLUE, "Yalnız kural"), ("both", AQUA, "İkisi de"), ("stat_only", ORANGE, "Yalnız istatistik")):
        ax.barh(rows.index, rows[col], left=left, height=0.6, color=color, edgecolor=SURFACE, linewidth=2, label=label)
        left = left + rows[col].to_numpy()
    for i, row in rows.iterrows():
        overlap = row["both"] / max(row["rule_flagged"], 1) * 100
        ax.text(left[i], i, f"  örtüşme: kuralın %{overlap:.1f}'i".replace(".", ","), va="center",
                color=INK_SECONDARY, fontsize=8.5)
    ax.set_yticks(rows.index, rows["label"], fontsize=9)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: f"{v / 1e6:g}M"))
    ax.set_xlim(0, left.max() * 1.35)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("İşaretlenen kayıt (ham veri)")
    ax.legend(loc="lower right")
    ax.set_title("Kural ve istatistik farklı kayıtları yakalıyor: imkânsız ≠ nadir")
    return _save(fig, out_dir, "10_rule_vs_stat.png")
