"""S5 — Proje sonuç dashboard'u: tüm öğrencilerin sonuç CSV'lerinden tek HTML sayfası.

Çıktı ``reports/dashboard/index.html``: sunucu veya ek kütüphane gerektirmez, tarayıcıda
açılır. Veriler sayfaya JSON olarak gömülür; S5 karşılaştırması etkileşimlidir (işlem /
metrik seçimi, üzerine gelince değer, tablo görünümü). Diğer bölümler kayıtlı grafikleri
(``reports/figures``) ve ana sayıları gösterir.

Çalıştırma:  python -m src.dashboard
"""
from __future__ import annotations

import html
import json
import logging
from pathlib import Path

import pandas as pd

from src import config
from src.benchmark_processing import OPERATIONS, SUMMARY_FILE, end_to_end
from src.plots_processing import crossover_rows, memory_per_row_kb

logger = logging.getLogger(__name__)

OUT_DIR = config.PROJECT_ROOT / "reports" / "dashboard"
FIG_REL = "../figures"

FIGURES = {
    "s1": [("01_disk_size.png", "Format başına disk boyutu"),
           ("02_read_time.png", "Okuma süresi vs boyut"),
           ("03_end_to_end.png", "Uçtan uca okuma (Spark açılışı dahil)"),
           ("04_pandas_memory.png", "pandas tepe belleği"),
           ("05_column_pruning.png", "Column pruning")],
    "s2": [("06_rule_impact.png", "Kural başına silinen kayıt"),
           ("07_before_after.png", "Temizlik öncesi / sonrası"),
           ("08_fare_vs_distance.png", "Mesafe–ücret, bağlamsal anomali"),
           ("09_rule_rates_vendor.png", "Sağlayıcı × kural"),
           ("10_rule_vs_stat.png", "Kural vs istatistik")],
    "s3": [("11_s3_hour.png", "Saatlik hacim"), ("12_s3_day.png", "Günlük hacim"),
           ("13_s3_zone.png", "İlk 10 bölge"), ("14_s3_payment.png", "Ödeme türleri"),
           ("15_s3_weekday_hour.png", "Gün × saat"), ("16_s3_pickup_map.png", "Biniş haritası"),
           ("17_s3_zone_concentration.png", "İlçe yoğunlaşması")],
    "s4": [("18_s4_scalability.png", "Eğitim süresi ve bellek"),
           ("19_s4_model_comparison.png", "RMSE ve R²"),
           ("20_s4_feature_actual_pred.png", "Özellik önemi, gerçek vs tahmin")],
    "s5": [("21_s5_op_time.png", "İşlem süresi"), ("22_s5_end_to_end.png", "Uçtan uca süre"),
           ("23_s5_memory.png", "Tepe bellek")],
}


def _read(name: str) -> pd.DataFrame | None:
    path = config.RESULTS_DIR / name
    return pd.read_csv(path) if path.exists() else None


def _fmt_int(x) -> str:
    return f"{int(round(float(x))):,}".replace(",", ".")


def _fmt(x, digits: int = 1) -> str:
    return f"{float(x):,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def collect_kpis() -> list[tuple[str, str, str]]:
    """(başlık, değer, açıklama) üçlüleri."""
    kpis = []
    manifest = _read("size_sets_manifest.csv")
    if manifest is not None:
        full = manifest[manifest["size"] == "full"]["rows"].iloc[0]
        kpis.append(("Ham kayıt", _fmt_int(full), "TLC Yellow Taxi 2026 Ocak–Mayıs"))
    kpis.append(("Temiz kayıt", _fmt_int(config.CLEAN_ROWS), "10 kuraldan sonra (S2)"))
    if manifest is not None:
        kpis.append(("Veri boyutu", f"{manifest['size'].nunique()}",
                     "100K · 1M · 5M · 10M · 19M (S1)"))
    s5 = _read(SUMMARY_FILE)
    if s5 is not None:
        e2e = end_to_end(s5)
        for fmt, label in (("csv", "CSV"), ("parquet_snappy", "Parquet")):
            x = crossover_rows(e2e, fmt)
            value = "öne geçmiyor" if x is None else f"≈ {_fmt(x / 1e6)}M kayıt"
            kpis.append((f"Spark > pandas ({label})", value, "uçtan uca süre, S5"))
    return kpis


def table_html(df: pd.DataFrame, columns: dict[str, str], numeric: set[str] = frozenset()) -> str:
    head = "".join(f"<th{' class=num' if c in numeric else ''}>{html.escape(t)}</th>"
                   for c, t in columns.items())
    body = []
    for _, row in df.iterrows():
        cells = []
        for c in columns:
            v = row[c]
            cls = " class=num" if c in numeric else ""
            cells.append(f"<td{cls}>{html.escape('' if pd.isna(v) else str(v))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def s1_table() -> str:
    io = _read("io_benchmark.csv")
    if io is None:
        return ""
    sel = io[(io["size"] == "full") & (io["mode"] == "all")
             & io["format"].isin(["csv", "parquet_snappy"])].copy()
    sel["motor"] = sel["engine"].map({"pandas": "pandas", "spark": "Spark"})
    sel["format"] = sel["format"].map({"csv": "CSV", "parquet_snappy": "Parquet snappy"})
    sel["disk"] = sel["disk_mb"].map(lambda v: _fmt(v, 0))
    sel["okuma"] = sel["seconds_mean"].map(lambda v: _fmt(v, 1))
    sel["bellek"] = sel["peak_mb"].map(lambda v: "—" if pd.isna(v) else _fmt(v, 0))
    return table_html(sel, {"motor": "Motor", "format": "Format", "disk": "Disk (MB)",
                            "okuma": "Okuma (sn)", "bellek": "pandas tepe (MB)"},
                      {"disk", "okuma", "bellek"})


def s4_table() -> str:
    df = _read("s4_comparison.csv")
    if df is None:
        return ""
    df = df[df["error"].isna()].copy()
    for c, d in (("rmse", 3), ("r2", 3), ("train_time_s", 2), ("peak_rss_mb", 0)):
        df[c] = df[c].map(lambda v, d=d: _fmt(v, d))
    return table_html(df, {"framework": "Çerçeve", "model": "Model", "size_label": "Boyut",
                           "rmse": "RMSE (dk)", "r2": "R²", "train_time_s": "Eğitim (sn)",
                           "peak_rss_mb": "Tepe RSS (MB)"},
                      {"rmse", "r2", "train_time_s", "peak_rss_mb"})


def s5_records() -> list[dict]:
    s5 = _read(SUMMARY_FILE)
    if s5 is None:
        return []
    recs = []
    for r in s5[s5["op"].isin(OPERATIONS)].itertuples():
        recs.append({"engine": r.engine, "format": r.format, "op": r.op, "rows": int(r.rows),
                     "seconds": float(r.seconds_mean), "peak": float(r.peak_rss_mb),
                     "throughput": float(r.throughput_rows_s)})
    for r in end_to_end(s5).itertuples():
        recs.append({"engine": r.engine, "format": r.format, "op": "end_to_end",
                     "rows": int(r.rows), "seconds": float(r.total_s), "peak": float(r.peak_rss_mb),
                     "throughput": float(r.rows / r.total_s)})
    return recs


def s5_notes() -> str:
    s5 = _read(SUMMARY_FILE)
    if s5 is None:
        return ""
    slopes = memory_per_row_kb(s5)
    parts = [f"{'CSV' if k == 'csv' else 'Parquet'}: {_fmt(v, 2)} KB/kayıt" for k, v in slopes.items()]
    return "pandas tepe belleğinin kayıt başına eğimi — " + " · ".join(parts)


def figure_gallery(key: str) -> str:
    items = []
    for name, caption in FIGURES[key]:
        if (config.FIGURES_DIR / name).exists():
            items.append(f'<figure><a href="{FIG_REL}/{name}"><img loading="lazy" '
                         f'src="{FIG_REL}/{name}" alt="{html.escape(caption)}"></a>'
                         f"<figcaption>{html.escape(caption)}</figcaption></figure>")
    return f'<div class="gallery">{"".join(items)}</div>'


def build_html() -> str:
    kpis = "".join(
        f'<div class="kpi"><div class="kpi-label">{html.escape(t)}</div>'
        f'<div class="kpi-value">{html.escape(v)}</div>'
        f'<div class="kpi-note">{html.escape(n)}</div></div>'
        for t, v, n in collect_kpis()
    )
    data = json.dumps(s5_records(), ensure_ascii=False)
    return (TEMPLATE
            .replace("__KPIS__", kpis)
            .replace("__S1_TABLE__", s1_table())
            .replace("__S4_TABLE__", s4_table())
            .replace("__S5_NOTES__", html.escape(s5_notes()))
            .replace("__G_S1__", figure_gallery("s1"))
            .replace("__G_S2__", figure_gallery("s2"))
            .replace("__G_S3__", figure_gallery("s3"))
            .replace("__G_S4__", figure_gallery("s4"))
            .replace("__G_S5__", figure_gallery("s5"))
            .replace("__DATA__", data.replace("</", "<\\/")))


def build(out_dir: Path = OUT_DIR) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "index.html"
    path.write_text(build_html(), encoding="utf-8")
    return path


TEMPLATE = r"""<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NYC Taxi Büyük Veri Analitiği — Sonuç Dashboard'u</title>
<style>
:root {
  color-scheme: light;
  --surface: #fcfcfb; --panel: #ffffff; --border: #e4e3df;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #77766f;
  --grid: #eceae5; --pandas: #eb6834; --spark: #2a78d6;
}
@media (prefers-color-scheme: dark) {
  :root {
    color-scheme: dark;
    --surface: #1a1a19; --panel: #222220; --border: #3a3936;
    --text-primary: #f0efec; --text-secondary: #c3c2b7; --text-muted: #9a998f;
    --grid: #33322f; --pandas: #d95926; --spark: #3987e5;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--surface); color: var(--text-primary);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
header, main { max-width: 1180px; margin: 0 auto; padding: 0 20px; }
header { padding-top: 28px; }
h1 { font-size: 26px; margin: 0 0 4px; }
h2 { font-size: 19px; margin: 36px 0 8px; border-top: 1px solid var(--border); padding-top: 24px; }
p.lead { color: var(--text-secondary); margin: 0 0 16px; max-width: 820px; }
nav { display: flex; flex-wrap: wrap; gap: 6px 16px; margin: 12px 0 4px; }
nav a { color: var(--text-secondary); text-decoration: none; font-size: 14px; }
nav a:hover { color: var(--text-primary); text-decoration: underline; }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 16px 0; }
.kpi { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; }
.kpi-label { color: var(--text-secondary); font-size: 13px; }
.kpi-value { font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums; }
.kpi-note { color: var(--text-muted); font-size: 12px; }
.controls { display: flex; flex-wrap: wrap; gap: 16px; align-items: center; margin: 12px 0; }
.controls label { font-size: 14px; color: var(--text-secondary); }
select { font: inherit; padding: 4px 8px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--panel); color: var(--text-primary); }
.chart-wrap { position: relative; background: var(--panel); border: 1px solid var(--border);
  border-radius: 10px; padding: 8px; }
svg text { fill: var(--text-secondary); font-size: 12px; }
.legend { display: flex; flex-wrap: wrap; gap: 6px 18px; margin: 8px 4px 0; font-size: 13px; color: var(--text-secondary); }
.legend span { display: inline-flex; align-items: center; gap: 6px; }
.tooltip { position: absolute; pointer-events: none; background: var(--panel); border: 1px solid var(--border);
  border-radius: 6px; padding: 6px 8px; font-size: 13px; box-shadow: 0 2px 8px rgba(0,0,0,.12);
  display: none; white-space: nowrap; }
table { border-collapse: collapse; width: 100%; font-size: 14px; margin: 8px 0; }
th, td { text-align: left; padding: 5px 8px; border-bottom: 1px solid var(--border); }
th { color: var(--text-secondary); font-weight: 600; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
details { margin: 10px 0; }
summary { cursor: pointer; color: var(--text-secondary); }
.gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 14px; margin: 12px 0; }
figure { margin: 0; background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 8px; }
figure img { width: 100%; height: auto; display: block; border-radius: 4px; background: #fff; }
figcaption { font-size: 13px; color: var(--text-secondary); margin-top: 6px; }
.note { color: var(--text-muted); font-size: 13px; }
footer { max-width: 1180px; margin: 40px auto; padding: 0 20px; color: var(--text-muted); font-size: 13px; }
</style>
</head>
<body>
<header>
  <h1>NYC Taxi Büyük Veri Analitiği</h1>
  <p class="lead">Araştırma sorusu: <strong>veri büyüdükçe klasik analiz yaklaşımı (tek makinede pandas + CSV)
  ne zaman yetersiz kalıyor?</strong> Bu sayfa beş öğrencinin ölçüm sonuçlarını tek yerde toplar.</p>
  <nav>
    <a href="#s5">S5 · İşleme performansı</a><a href="#s1">S1 · Format ve okuma</a>
    <a href="#s2">S2 · Temizlik</a><a href="#s3">S3 · Aggregation</a><a href="#s4">S4 · ML</a>
  </nav>
  <div class="kpis">__KPIS__</div>
</header>
<main>
<section id="s5">
  <h2>Öğrenci 5 — pandas vs Spark işleme performansı</h2>
  <p class="lead">Her nokta 3 tekrarın ortalaması (1 ısınma atıldı). pandas işlemleri bellekteki tabloda
  çalışır; Spark tembel olduğu için her işlem dosyadan okuyarak çalışır. "Uçtan uca" = pandas yükleme veya
  Spark oturum açılışı + üç işlemin toplamı.</p>
  <div class="controls">
    <label>İşlem <select id="op">
      <option value="end_to_end">Uçtan uca (hazırlık + 3 işlem)</option>
      <option value="groupby_hour">groupby: saate göre özet</option>
      <option value="join_borough">join: bölge tablosu → ilçe</option>
      <option value="window_top3">window: bölge başına ilk 3</option>
    </select></label>
    <label>Metrik <select id="metric">
      <option value="seconds">Süre (sn)</option>
      <option value="peak">Tepe bellek (MB)</option>
      <option value="throughput">Throughput (kayıt/sn)</option>
    </select></label>
  </div>
  <div class="chart-wrap" id="chart-wrap">
    <svg id="chart" role="img" aria-labelledby="chart-title" width="100%" viewBox="0 0 900 420"></svg>
    <div class="legend" id="legend"></div>
    <div class="tooltip" id="tip"></div>
  </div>
  <p class="note" id="chart-title"></p>
  <p class="note">__S5_NOTES__</p>
  <p class="note">Not: tepe bellek, işlemlerin aynı süreçte sırayla (groupby → join → window) çalıştığı ölçümdendir; JVM belleği küçülmediği için işlem başına değer bir üst sınırdır.</p>
  <details><summary>Tablo görünümü</summary><div id="table"></div></details>
  __G_S5__
</section>
<section id="s1">
  <h2>Öğrenci 1 — Veri temini, format dönüşümü, okuma performansı</h2>
  <p class="lead">19M kayıtta tam okuma (S1'in makinesi: 28 çekirdek, 16 GB RAM).</p>
  __S1_TABLE__
  __G_S1__
</section>
<section id="s2">
  <h2>Öğrenci 2 — Temizlik ve anomali analizi</h2>
  <p class="lead">18.999.282 → 17.210.961 kayıt (%90,6 korundu); 10 kural, kural vs istatistiksel anomali.</p>
  __G_S2__
</section>
<section id="s3">
  <h2>Öğrenci 3 — Saat, gün, bölge ve ödeme türü aggregation'ları</h2>
  __G_S3__
</section>
<section id="s4">
  <h2>Öğrenci 4 — Yolculuk süresi tahmini: PySpark MLlib vs scikit-learn</h2>
  <p class="lead">Aynı örneklem, aynı train/test ayrımı, aynı özellikler ve hiperparametreler. Bellek: süreç ağacının
  tepe RSS'i (Spark'ta JVM dahil).</p>
  __S4_TABLE__
  __G_S4__
</section>
</main>
<footer>Üretildi: <code>python -m src.dashboard</code> · Kaynak veriler: <code>reports/results/*.csv</code></footer>
<script id="data" type="application/json">__DATA__</script>
<script>
(() => {
  const DATA = JSON.parse(document.getElementById('data').textContent);
  const SERIES = [
    {engine: 'pandas', format: 'csv', label: 'pandas + CSV', color: 'var(--pandas)', dash: '6 4', shape: 'circle'},
    {engine: 'pandas', format: 'parquet_snappy', label: 'pandas + Parquet', color: 'var(--pandas)', dash: '', shape: 'square'},
    {engine: 'spark', format: 'csv', label: 'Spark + CSV', color: 'var(--spark)', dash: '6 4', shape: 'circle'},
    {engine: 'spark', format: 'parquet_snappy', label: 'Spark + Parquet', color: 'var(--spark)', dash: '', shape: 'square'},
  ];
  const METRIC = {
    seconds: {label: 'Süre (sn, log)', log: true, fmt: v => v.toLocaleString('tr-TR', {maximumFractionDigits: 2}) + ' sn'},
    peak: {label: 'Tepe bellek (MB)', log: false, fmt: v => Math.round(v).toLocaleString('tr-TR') + ' MB'},
    throughput: {label: 'Throughput (kayıt/sn, log)', log: true, fmt: v => Math.round(v).toLocaleString('tr-TR') + ' kayıt/sn'},
  };
  const svg = document.getElementById('chart'), tip = document.getElementById('tip');
  const W = 900, H = 420, M = {l: 78, r: 24, t: 16, b: 48};
  const ns = 'http://www.w3.org/2000/svg';
  const el = (name, attrs, parent) => { const e = document.createElementNS(ns, name);
    for (const k in attrs) e.setAttribute(k, attrs[k]); (parent || svg).appendChild(e); return e; };
  const fmtRows = v => v >= 1e6 ? (v / 1e6).toLocaleString('tr-TR', {maximumFractionDigits: 1}) + 'M' : Math.round(v / 1e3) + 'K';

  function draw() {
    const op = document.getElementById('op').value, metric = document.getElementById('metric').value;
    const m = METRIC[metric];
    const rows = DATA.filter(d => d.op === op);
    svg.innerHTML = '';
    if (!rows.length) { el('text', {x: 20, y: 40}).textContent = 'S5 sonuçları bulunamadı.'; return; }
    const xs = rows.map(d => d.rows), ys = rows.map(d => d[metric]);
    const lx = Math.log10(Math.min(...xs)) - 0.1, hx = Math.log10(Math.max(...xs)) + 0.1;
    let ly, hy;
    if (m.log) { ly = Math.floor(Math.log10(Math.min(...ys))); hy = Math.ceil(Math.log10(Math.max(...ys))); }
    else { ly = 0; hy = Math.max(...ys) * 1.1; }
    const X = v => M.l + (Math.log10(v) - lx) / (hx - lx) * (W - M.l - M.r);
    const Y = v => H - M.b - ((m.log ? Math.log10(v) : v) - ly) / (hy - ly) * (H - M.t - M.b);
    // grid + y axis
    const yticks = [];
    if (m.log) for (let e = ly; e <= hy; e++) yticks.push(10 ** e);
    else { const step = niceStep(hy / 5); for (let v = 0; v <= hy; v += step) yticks.push(v); }
    yticks.forEach(v => {
      el('line', {x1: M.l, x2: W - M.r, y1: Y(v), y2: Y(v), stroke: 'var(--grid)'});
      el('text', {x: M.l - 8, y: Y(v) + 4, 'text-anchor': 'end'}).textContent = m.log ? shortNum(v) : Math.round(v).toLocaleString('tr-TR');
    });
    [...new Set(xs)].sort((a, b) => a - b).forEach(v => {
      el('text', {x: X(v), y: H - M.b + 18, 'text-anchor': 'middle'}).textContent = fmtRows(v);
    });
    el('text', {x: (M.l + W - M.r) / 2, y: H - 8, 'text-anchor': 'middle'}).textContent = 'Kayıt sayısı (log)';
    el('text', {x: 16, y: (M.t + H - M.b) / 2, 'text-anchor': 'middle', transform: `rotate(-90 16 ${(M.t + H - M.b) / 2})`}).textContent = m.label;
    // series
    SERIES.forEach(s => {
      const pts = rows.filter(d => d.engine === s.engine && d.format === s.format).sort((a, b) => a.rows - b.rows);
      if (!pts.length) return;
      el('path', {d: pts.map((p, i) => (i ? 'L' : 'M') + X(p.rows) + ' ' + Y(p[metric])).join(' '),
        fill: 'none', stroke: s.color, 'stroke-width': 2, 'stroke-dasharray': s.dash});
      pts.forEach(p => {
        const cx = X(p.rows), cy = Y(p[metric]);
        const mark = s.shape === 'circle'
          ? el('circle', {cx, cy, r: 4.5, fill: s.color, stroke: 'var(--panel)', 'stroke-width': 2})
          : el('rect', {x: cx - 4.5, y: cy - 4.5, width: 9, height: 9, rx: 1, fill: s.color, stroke: 'var(--panel)', 'stroke-width': 2});
        const hit = el('circle', {cx, cy, r: 12, fill: 'transparent', tabindex: 0});
        const show = () => {
          tip.innerHTML = `<strong>${s.label}</strong><br>${fmtRows(p.rows)} kayıt · ${m.fmt(p[metric])}`;
          tip.style.display = 'block';
          const box = svg.getBoundingClientRect(), k = box.width / W;
          tip.style.left = (cx * k + 14) + 'px'; tip.style.top = (cy * k - 10) + 'px';
        };
        hit.addEventListener('mouseenter', show); hit.addEventListener('focus', show);
        hit.addEventListener('mouseleave', () => tip.style.display = 'none');
        hit.addEventListener('blur', () => tip.style.display = 'none');
      });
    });
    document.getElementById('chart-title').textContent =
      `${document.getElementById('op').selectedOptions[0].text} — ${m.label}`;
    // table view
    const sizes = [...new Set(xs)].sort((a, b) => a - b);
    let t = '<table><thead><tr><th>Seri</th>' + sizes.map(v => `<th class="num">${fmtRows(v)}</th>`).join('') + '</tr></thead><tbody>';
    SERIES.forEach(s => {
      t += `<tr><td>${s.label}</td>` + sizes.map(v => {
        const d = rows.find(r => r.engine === s.engine && r.format === s.format && r.rows === v);
        return `<td class="num">${d ? m.fmt(d[metric]) : '—'}</td>`; }).join('') + '</tr>';
    });
    document.getElementById('table').innerHTML = t + '</tbody></table>';
  }
  function niceStep(x) { const p = 10 ** Math.floor(Math.log10(x)); const f = x / p; return (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * p; }
  function shortNum(v) { return v >= 1e6 ? (v / 1e6) + 'M' : v >= 1e3 ? (v / 1e3) + 'K' : String(v); }
  const legend = document.getElementById('legend');
  SERIES.forEach(s => {
    legend.insertAdjacentHTML('beforeend', `<span><svg width="34" height="12"><line x1="0" y1="6" x2="34" y2="6" stroke="${s.color}" stroke-width="2" stroke-dasharray="${s.dash}"/></svg>${s.label}</span>`);
  });
  document.getElementById('op').addEventListener('change', draw);
  document.getElementById('metric').addEventListener('change', draw);
  draw();
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger.info("Dashboard: %s", build())
