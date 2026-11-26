// 20–30 dk'lık sunum için 7 panel. Her öğrenci panelinde 3–4 kart, 1 kritik kod + gerçek çıktısı,
// en az 1 nicel sonuç ve o öğrencinin simülasyonları. Rakamlar results/*.json ve notebook çıktılarından.
import { notebooks, results as R } from '../lib/data.js'
import { trDec, trInt } from '../lib/format.js'
import { C, endToEndRows } from './charts.js'

const A = (what, how, takeaway) => ({ what, how, takeaway })
const cell = (text, th = false) => ({ text: String(text), th })
const head = (...cols) => [cols.map((t) => ({ text: t }))]
/** Notebook hücresinin gerçek tablo çıktısı. */
const nbTable = (slug, c, o = 0) => () => notebooks[slug].cells[c].outputs[o].tables[0]
const nbStream = (slug, c) => notebooks[slug].cells[c].outputs.find((o) => o.type === 'stream').text.trim()
const ink = '#a9b1c7'

/** Veri sözlüğü: dictionary_stats.csv (19M kaydın tamamından Spark ile). */
function dictionaryTable() {
  const v = (x) => (typeof x === 'number' ? trDec(x, Number.isInteger(x) ? 0 : 2) : x ?? '—')
  return {
    header: head('Kolon', 'Tip', 'Null %', 'Gözlenen min', 'Gözlenen max', 'Farklı değer (yaklaşık)'),
    rows: R.dictionary_stats.map((r) => [cell(r.column, true), cell(r.spark_type), cell(trDec(r.null_pct, 2)), cell(v(r.min)), cell(v(r.max)), cell(trInt(r.approx_distinct))]),
  }
}

/** build_sizes çıktısı: 19M satırın 4 formattaki dosyası (size_sets_manifest.csv). */
function manifestFullTable() {
  const L = { csv: 'CSV', parquet_snappy: 'Parquet snappy', parquet_gzip: 'Parquet gzip', parquet_zstd: 'Parquet zstd' }
  return {
    header: head('Format', 'Satır', 'MB', 'Row group'),
    rows: R.size_sets_manifest.filter((r) => r.size === 'full').map((r) => [cell(L[r.format], true), cell(trInt(r.rows)), cell(trDec(r.mb, 1)), cell(r.row_groups ?? '—')]),
  }
}

/** clean.py çıktısı: ham → silinen → temiz. */
function cleanTotalsTable() {
  const total = R.cleaning_impact.find((r) => r.rule === 'TOTAL')
  const clean = R.cleaning_before_after.find((r) => r.dataset === 'clean').rows
  return {
    header: head('Aşama', 'Kayıt'),
    rows: [[cell('Ham', true), cell(trInt(total.total_rows))], [cell('En az bir kurala takılan', true), cell(trInt(total.sequential))], [cell('Temiz (data/clean)', true), cell(trInt(clean))],
      [cell('Karşılaştırma: dropna() silseydi', true), cell(trInt(R.cleaning_impact.find((r) => r.rule === 'NAIVE').sequential))]],
  }
}

/** Temizlik öncesi/sonrası: cleaning_before_after.csv. */
function beforeAfterTable() {
  const raw = R.cleaning_before_after.find((r) => r.dataset === 'raw')
  const clean = R.cleaning_before_after.find((r) => r.dataset === 'clean')
  const metrics = [['trip_distance', 'Mesafe (mil)'], ['fare_amount', 'Ücret ($)'], ['trip_duration_min', 'Süre (dk)'], ['avg_speed_mph', 'Ortalama hız (mph)']]
  return {
    header: head('Ölçü', 'Ortalama: ham', 'Ortalama: temiz', 'Medyan: ham', 'Medyan: temiz', 'En büyük: ham', 'En büyük: temiz'),
    rows: metrics.map(([k, label]) => [cell(label, true),
      cell(trDec(raw[`${k}_mean`], 2)), cell(trDec(clean[`${k}_mean`], 2)),
      cell(trDec(raw[`${k}_median`], 2)), cell(trDec(clean[`${k}_median`], 2)),
      cell(trDec(raw[`${k}_max`], 2)), cell(trDec(clean[`${k}_max`], 2))]),
  }
}

/** Kural hunisi: cleaning_impact.csv — kurallar sırayla uygulanınca her birinin sildiği kayıt. */
function cleaningFunnel() {
  const rules = R.cleaning_impact.filter((r) => /^R\d+$/.test(r.rule))
  const total = R.cleaning_impact.find((r) => r.rule === 'TOTAL')
  const clean = R.cleaning_before_after.find((r) => r.dataset === 'clean').rows
  const labels = rules.map((r) => `${r.rule} · ${r.description}`)
  return {
    backgroundColor: 'transparent', textStyle: { fontFamily: 'Inter Variable, sans-serif' },
    title: { text: `${trInt(total.total_rows)} ham  →  ${trInt(total.sequential)} silindi (%${trDec(total.sequential_pct, 1)})  →  ${trInt(clean)} temiz`, left: 'center', top: 0, textStyle: { color: '#e8ebf4', fontSize: 15, fontWeight: 600 } },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: '#141b31', borderColor: '#2a3558', textStyle: { color: '#e8ebf4' },
      formatter: (ps) => { const r = rules[ps[0].dataIndex]; return `${labels[ps[0].dataIndex]}<br><b>${trInt(r.sequential)}</b> kayıt (%${trDec(r.sequential_pct, 2)})` } },
    grid: { left: 330, right: 150, top: 40, bottom: 30 },
    xAxis: { type: 'value', name: 'Silinen kayıt', nameLocation: 'middle', nameGap: 26, nameTextStyle: { color: ink }, axisLabel: { color: ink, formatter: (v) => `${trInt(v / 1000)}K` }, splitLine: { lineStyle: { color: '#1d2642' } } },
    yAxis: { type: 'category', inverse: true, data: labels, axisLabel: { color: ink, fontSize: 13 }, axisLine: { lineStyle: { color: '#2a3558' } } },
    series: [{ type: 'bar', barWidth: '60%', data: rules.map((r) => r.sequential), itemStyle: { color: C.taxi, borderRadius: [0, 4, 4, 0] },
      label: { show: true, position: 'right', color: '#e8ebf4', formatter: (p) => `${trInt(p.value)}  (%${trDec(rules[p.dataIndex].sequential_pct, 2)})` } }],
  }
}

/** Sağlayıcı × kural ısı haritası (notebook görseliyle aynı içerik): anomaly_rule_rates.csv, log renk. */
const VENDORS = { 1: 'Creative Mobile', 2: 'Curb', 6: 'Myle', 7: 'Helix' }
function vendorHeatmap() {
  const rows = R.anomaly_rule_rates.filter((r) => r.dimension === 'VendorID').sort((a, b) => a.value - b.value)
  const rules = ['R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R10']
  const ylab = rows.map((r) => `${r.value} · ${VENDORS[r.value]}  (${trDec(r.rows / 1e6, 2)}M)`)
  const data = []
  rows.forEach((r, y) => rules.forEach((k, x) => data.push([x, y, Math.log10(r[k] + 0.01), r[k]])))
  return {
    backgroundColor: 'transparent', textStyle: { fontFamily: 'Inter Variable, sans-serif' },
    tooltip: { position: 'top', backgroundColor: '#141b31', borderColor: '#2a3558', textStyle: { color: '#e8ebf4' }, formatter: (p) => `${ylab[p.value[1]]} · ${rules[p.value[0]]}<br><b>%${trDec(p.value[3], 2)}</b>` },
    grid: { left: 230, right: 30, top: 10, bottom: 40 },
    xAxis: { type: 'category', data: rules, axisLabel: { color: ink, fontSize: 14 }, axisLine: { show: false }, axisTick: { show: false } },
    yAxis: { type: 'category', inverse: true, data: ylab, axisLabel: { color: ink, fontSize: 14 }, axisLine: { show: false }, axisTick: { show: false } },
    visualMap: { show: false, dimension: 2, min: -2, max: 2, inRange: { color: ['#16233f', '#1f4e8c', '#338ced', '#9fd0ff'] } },
    series: [{ type: 'heatmap', data, itemStyle: { borderColor: '#0f1528', borderWidth: 2 },
      label: { show: true, fontSize: 14, color: '#e8ebf4', formatter: (p) => (p.value[3] > 0 ? trDec(p.value[3], 1) : '·') } }],
  }
}

/** S4 metrik tablosu: s4_comparison.csv (17M) + s4_tuning.csv (ayarlı model, 5M test). */
function mlMetricsTable() {
  const rows = R.s4_comparison.filter((r) => r.size_label === '17M')
  const tuned = R.s4_tuning.find((r) => r.stage === 'test_tuned')
  const lin = (m) => m === 'LinearRegression'
  return {
    header: head('Model (17M kayıt)', 'Rol', 'RMSE (dk)', 'MAE (dk)', 'R²', 'Eğitim (sn)', 'Tepe bellek (GB)'),
    rows: [
      ...rows.map((r) => [cell(`${r.framework} · ${r.model}`, true), cell(lin(r.model) ? 'basit referans (doğrusal)' : 'ağaç modeli'), cell(trDec(r.rmse, 3)), cell(trDec(r.mae, 3)), cell(trDec(r.r2, 3)), cell(trDec(r.train_time_s, 1)), cell(trDec(r.peak_rss_mb / 1000, 1))]),
      [cell('scikit-learn · HistGradientBoosting, ayarlı', true), cell(`hiperparametre araması (${tuned.size_label} test)`), cell(trDec(tuned.rmse, 3)), cell(trDec(tuned.mae, 3)), cell(trDec(tuned.r2, 3)), cell(trDec(tuned.train_time_s, 1)), cell('—')],
    ],
  }
}

/** S5 karşılaştırma: uçtan uca süre (s5_processing.csv), tepe bellek, disk boyutu, pandas + CSV'ye göre hızlanma. */
function benchmarkTable() {
  const e2e = endToEndRows(R.s5_processing.filter((r) => r.size !== 'full_rg16'))
  const disk = (size, f) => (size === 'x3' ? R.s5_synthetic_manifest : R.size_sets_manifest).find((r) => r.size === size && r.format === f)?.mb
  const SIZES = [['1m', '1M'], ['5m', '5M'], ['full', '19M'], ['x3', '57M (sentetik)']]
  const COMBOS = [['pandas', 'csv', 'pandas + CSV'], ['pandas', 'parquet_snappy', 'pandas + Parquet'], ['spark', 'csv', 'Spark + CSV'], ['spark', 'parquet_snappy', 'Spark + Parquet'], ['spark_cache', 'csv', 'Spark + cache + CSV'], ['spark_cache', 'parquet_snappy', 'Spark + cache + Parquet']]
  const rows = []
  for (const [size, label] of SIZES) {
    const ref = e2e.find((r) => r.engine === 'pandas' && r.format === 'csv' && r.size === size)
    for (const [e, f, name] of COMBOS) {
      if (e === 'spark_cache' && size !== 'x3') continue
      const r = e2e.find((x) => x.engine === e && x.format === f && x.size === size)
      if (!r) continue
      rows.push([cell(label, true), cell(name), cell(trDec(r.total, 2)), cell(trDec(r.peak / 1000, 2)), cell(trInt(disk(size, f))), cell(`${trDec(ref.total / r.total, 1)}×`)])
    }
  }
  return { header: head('Boyut', 'Motor + format', 'Uçtan uca (sn)', 'Tepe bellek (GB)', 'Disk (MB)', 'Hızlanma (pandas + CSV = 1×)'), rows }
}

/** S3 aggregation boyutları: her boyutun en yüksek değeri. */
function aggregationStats() {
  const hour = [...R.s3_hour].sort((a, b) => b.trip_count - a.trip_count)[0]
  const zone = R.s3_zone[0]
  const card = R.s3_payment.find((r) => r.payment_type === 1)
  return [
    ['Saat', `${hour.pickup_hour}.00`, `${trInt(hour.trip_count)} biniş`],
    ['Gün × saat', 'Perşembe 21.00', 'hafta içi zirvesi (8.830 ort.)'],
    ['Bölge', zone.pickup_zone, `${trInt(zone.trip_count)} biniş`],
    ['Ödeme türü', 'Kredi kartı', `%${trDec(card.share_pct, 2)}`],
  ]
}
function hourTopTable() {
  return { header: head('pickup_hour', 'trip_count'), rows: [...R.s3_hour].sort((a, b) => b.trip_count - a.trip_count).slice(0, 3).map((r) => [cell(r.pickup_hour, true), cell(trInt(r.trip_count))]) }
}

export const PANELS = [
  { id: 'giris', kind: 'intro', short: 'Giriş' },
  {
    id: 'ogrenci-1', kind: 'student', n: 1, short: 'Veri ve formatlar',
    contribution: 'NYC TLC verisini indirip doğruladı, 20 kolonluk veri sözlüğünü çıkardı; veriyi 5 boyutta CSV ve 3 Parquet sıkıştırmasına dönüştürüp disk, okuma süresi ve belleği ölçtü.',
    items: [
      { type: 'code', where: 'src/build_sizes.py:96–101 · CSV / Parquet yazma', code:
`writer = df.coalesce(1).write.mode("overwrite").option("compression", codec)
if spark_format == "csv":
    writer = writer.option("header", True).option("timestampNTZFormat", CSV_TIMESTAMP_FORMAT)
else:
    writer = writer.option("parquet.block.size", PARQUET_BLOCK_BYTES)
writer.format(spark_format).save(str(spark_out.resolve()))`,
        output: { source: 'size_sets_manifest.csv · 01 notebook hücre 1', table: manifestFullTable, text: nbStream('01_data_acquisition_and_formats', 1) } },
      { type: 'table', title: 'Veri sözlüğü: 20 kolon', source: 'reports/results/dictionary_stats.csv', table: dictionaryTable,
        ann: A('Her kolonun tipi, boş oranı ve gözlenen en küçük/en büyük değeri.', 'Her satır bir kolon; değerler 18.999.282 kaydın tamamından hesaplandı.', 'Mesafede 328.522 mil, tarihte 2001 yılı gibi imkânsız değerler var; temizlik bu yüzden şart.') },
      { type: 'chart', key: '01_data_acquisition_and_formats:7:0', title: 'Disk boyutu: CSV vs Parquet',
        ann: A('Aynı verinin CSV ve üç Parquet sıkıştırmasında diskte kapladığı yer.', 'X veri boyutu, Y megabayt (log); her renk bir format.', '19M kayıtta CSV 1.907 MB, Parquet zstd 290 MB (6,6 kat küçük): Parquet kolon bazlı saklayıp tekrar eden değerleri sıkıştırıyor.') },
      { type: 'chart', key: '01_data_acquisition_and_formats:12:0', title: 'Okuma süresi',
        ann: A('pandas ve Spark\'ın dosyayı okuma süresi.', 'İki eksen log; turuncu pandas, mavi Spark, kesikli çizgi CSV.', '19M\'de pandas CSV\'yi 71 sn\'de, Parquet\'i 1,8 sn\'de okuyor; CSV metin olduğu için her satır tek tek çözülüyor.') },
      { type: 'chart', key: '01_data_acquisition_and_formats:21:0', title: 'pandas belleği ve RAM sınırı',
        ann: A('pandas okurken ulaşılan en yüksek bellek.', 'X kayıt sayısı, Y megabayt; çizgi dikleştikçe bellek hızlı artıyor.', '1,9 GB\'lık CSV bellekte 9,4 GB tutuyor; bu hızla 16 GB RAM ~32M kayıtta tükenir. Klasik yaklaşımın asıl sınırı bu.') },
    ],
  },
  {
    id: 'ogrenci-2', kind: 'student', n: 2, short: 'Temizlik ve anomali',
    contribution: '10 kuralla imkânsız kayıtları sildi (18.999.282 → 17.210.961) ve mesafe, ücret, süre anomalilerini kural ve istatistik yöntemleriyle karşılaştırdı.',
    items: [
      { type: 'code', where: 'src/clean.py:32–43 · temizlik kuralları', code:
`RULES: dict[str, tuple[str, tuple[str, ...]]] = {
    "R1": ("passenger_count 0 veya > 6", ("passenger_count_zero", "passenger_count_over_max")),
    "R2": ("trip_distance <= 0 veya > 100 mil", ("distance_zero_or_negative", "distance_over_max")),
    "R3": ("fare_amount <= 0 veya total_amount <= 0", ("fare_zero_or_negative", "total_zero_or_negative")),
    "R4": ("süre <= 1 dk, > 180 dk veya negatif", ("dropoff_before_pickup", "duration_under_min", "duration_over_max")),
    "R5": ("ortalama hız > 80 mph", ("speed_over_max",)),
    "R6": ("pickup 2026-01..05 dışında", ("pickup_outside_period",)),
    "R7": ("RatecodeID = 99", ("ratecode_unknown",)),
    "R8": ("PU/DO LocationID 264 veya 265", ("location_unknown",)),
    "R9": ("tam kopya kayıt (ilki tutulur)", ()),
    "R10": ("cbd_congestion_fee < 0", ("cbd_fee_negative",)),
}`,
        output: { source: 'cleaning_impact.csv + cleaning_before_after.csv', table: cleanTotalsTable } },
      { type: 'echart', title: 'Temizlik hunisi: her kural kaç kaydı eledi?', source: 'reports/results/cleaning_impact.csv', option: cleaningFunnel, height: 440,
        ann: A('Ham veriden temiz veriye, kurallar sırayla uygulanınca her birinin sildiği kayıt.', 'Üstte ham → silinen → temiz toplamı; her çubuk bir kuralın sıralı uygulamada sildiği kayıt ve ham veriye oranı.', 'Toplam %9,4 silindi; en büyük payı RatecodeID = 99 (R7) ve geçersiz mesafe (R2) alıyor, yani hatalar birkaç sistematik kaynaktan geliyor.') },
      { type: 'table', title: 'Temizlik öncesi / sonrası', source: 'reports/results/cleaning_before_after.csv', table: beforeAfterTable,
        ann: A('Mesafe, ücret, süre ve hızın ham ve temiz veride ortalama, medyan ve en büyük değeri.', 'Her satır bir ölçü; ham ve temiz sütunlarını yan yana karşılaştırın.', 'Medyanlar neredeyse aynı (mesafe 1,83 → 1,84 mil) ama ortalama ve en büyük değer düştü: temizlik yalnızca hatalı uç kayıtları kesti.') },
      { type: 'chart', key: '02_cleaning_and_anomalies:23:0', title: 'Hatalar nerede yoğunlaşıyor? (sağlayıcı başına, ham veri)',
        chart: { source: 'anomaly_rule_rates.csv', option: vendorHeatmap, height: 320 },
        ann: A('Her taksimetre sağlayıcısının kayıtlarının yüzde kaçının her kurala takıldığı.', 'Satır sağlayıcı (parantezde kayıt sayısı), sütun kural; değerler %, renk log ölçekli, "·" sıfır.', 'Hatanın kaynağı sağlayıcı: Helix iniş zamanı göndermediği için %100\'ü süre kuralına, Creative Mobile tarife kodu göndermediği için %17,3\'ü R7\'ye takılıyor. Bu sağlayıcı dış ilçelerde yaygın, bu yüzden Bronx kayıtlarının %54\'ü R7 ile siliniyor.') },
      { type: 'sim', id: 'cleaning' },
    ],
  },
  {
    id: 'ogrenci-3', kind: 'student', n: 3, short: 'PySpark aggregation',
    contribution: '17,2M temiz yolculuğu PySpark groupBy/agg ile saat, gün, bölge ve ödeme türüne göre özetledi; 7 günlük window ve bölge join\'i ekledi.',
    items: [
      { type: 'stats', title: 'Aggregation boyutları: tam veride en yüksek değer', source: 's3_hour.csv · s3_pattern.csv · s3_zone.csv · s3_payment.csv', stats: aggregationStats },
      { type: 'code', where: 'src/aggregate_trips.py:14–21 · saate göre groupBy (03 notebook bu fonksiyonu çalıştırıp gösteriyor)', code:
`def hourly_trip_counts(trips: DataFrame) -> DataFrame:
    """Count pickups by New York local hour, busiest first."""
    return (
        trips.groupBy("pickup_hour")
        .count()
        .withColumnRenamed("count", "trip_count")
        .orderBy(F.desc("trip_count"), F.asc("pickup_hour"))
    )`,
        output: { source: 's3_hour.csv · 03 notebook hücre 1', table: hourTopTable, text: nbStream('03_aggregation_ders', 1) } },
      { type: 'sim', id: 'aggregation' },
      { type: 'chart', key: '03_aggregation_ders:11:0', title: 'Hafta günü × saat',
        ann: A('Her hafta günü ve saat için ortalama biniş sayısı.', 'Satır hafta günü, sütun saat; renk sarıya döndükçe biniş artıyor.', 'Hafta içi zirve Perşembe 21.00 (8.830), hafta sonu Cumartesi 18.00 (8.853): her gün en yoğun dilim akşam saatleri.') },
      { type: 'sim', id: 'window', tag: 'İleri analitik işlem · window: rowsBetween(-6, 0) ile 7 günlük ortalama, lag(7) ile geçen haftaya göre değişim' },
      { type: 'chart', key: '03_aggregation_ders:30:0', title: 'En yoğun 10 bölge',
        ann: A('En çok biniş olan 10 TLC bölgesi.', 'Çubuk uzunluğu biniş sayısı; sarı en yüksek.', 'Upper East Side South 815.297 binişle ilk sırada, JFK dördüncü: talep Manhattan\'da ve havalimanlarında toplanıyor.') },
      { type: 'sim', id: 'zonemap', tag: 'İleri analitik işlem · join: bölge sözlüğü birleştirmesi. Ayrı deneyde broadcast 2,806 sn, sort-merge 3,355 sn (ortanca, 4 tur, sonuçlar eşleşti)' },
      { type: 'chart', key: '03_aggregation_ders:41:0', title: 'Ödeme türü',
        ann: A('Ödeme türlerinin yolculuk payı.', 'Her çubuk bir ödeme kodu ve yüzdesi.', 'Kredi kartı %64,96 ile baskın; Flex Fare %25,45 ile ikinci. Flex Fare hata değil, ayrı bir grup olarak tutuldu.') },
    ],
  },
  {
    id: 'ogrenci-4', kind: 'student', n: 4, short: 'ML: süre tahmini',
    contribution: 'Yolculuk süresini tahmin eden doğrusal ve gradyan artırmalı modelleri PySpark MLlib ve scikit-learn ile 100K–17M kayıtta eğitip doğruluk, süre ve belleği karşılaştırdı.',
    items: [
      { type: 'code', where: 'notebooks/04_ml_duration_prediction.ipynb · hücre 4 (canlı, 100K; kısaltılmış) · aynı veride iki çerçeve', code:
`sdf = ml.with_nominal_metadata(spark.read.parquet(str(sample_dir)))
train = sdf.filter(F.col(ml.SPLIT_COL)).drop(ml.SPLIT_COL)
test = sdf.filter(~F.col(ml.SPLIT_COL)).drop(ml.SPLIT_COL)
spark_metrics = ml._evaluate(ml._build_pipeline("lr").fit(train).transform(test))

pdf = pd.read_parquet(sample_dir)
for c in ml.CATEGORICAL_FEATURES:
    pdf[c] = pdf[c].astype("int64")
X = pdf[ml.NUMERIC_FEATURES + ml.CATEGORICAL_FEATURES]; y = pdf[ml.TARGET_COL]; m = pdf[ml.SPLIT_COL]
sk_model = ml.build_sklearn_model("lr").fit(X[m], y[m])
sk_metrics = ml.sklearn_metrics(y[~m], sk_model.predict(X[~m]))`,
        output: { source: 'notebook çıktısı (hücre 4)', table: nbTable('04_ml_duration_prediction', 4) } },
      { type: 'table', title: 'Model metrikleri: RMSE, MAE, R²', source: 'reports/results/s4_comparison.csv + s4_tuning.csv', table: mlMetricsTable,
        ann: A('17M kayıtta dört modelin test metrikleri, eğitim süresi ve belleği; son satır ayarlı model.', 'RMSE ve MAE dakika cinsinden hata (düşük iyi), R² açıklanan varyans (yüksek iyi).', 'Ağaç modelleri doğrusal referansa göre RMSE\'yi 7,16\'dan 5,45 dakikaya (%24) düşürüyor; ayar araması 5M testte 5,04\'e indiriyor.') },
      { type: 'chart', key: '04_ml_duration_prediction:8:1', title: 'Doğruluk veri boyutuyla nasıl değişiyor?',
        ann: A('Dört modelin test hatası (dakika), veri boyutuna göre.', 'Düşük RMSE daha iyi; kesikli doğrusal model, düz çizgi ağaç modeli.', 'Doğrusal model veri büyüse de iyileşmiyor; ağaç modeli etkileşimleri yakaladığı için 5M\'de RMSE\'yi 7,18\'den 5,47 dakikaya indiriyor.') },
      { type: 'chart', key: '04_ml_duration_ders:19:1', title: 'Model neye bakıyor?',
        ann: A('Spark GBT\'nin özellik önemleri ve test kümesinde gerçek–tahmin.', 'Sol: önem payları (toplam 1). Sağ: her nokta bir yolculuk; köşegene yakınlık iyi tahmin.', 'Mesafe (0,47) ve bölgeler (0,31) saat ve günden belirleyici: nereye gidildiği, ne zaman gidildiğinden önemli.') },
      { type: 'chart', key: '04_ml_duration_ders:26:2', title: 'Sızıntı kontrolü ve ayar araması',
        ann: A('Ücret kolonlarını çıkarma testi ve 12 ayarlık hiperparametre araması.', 'Sol: test RMSE; sağ: doğrulama RMSE, sarı seçilen ayar.', 'Ücret kalemleri çıkarılınca hata artmıyor, yani sızıntı yok; seçilen ayar RMSE\'yi 5,536\'dan 5,042\'ye indiriyor.') },
      { type: 'sim', id: 'model' },
    ],
  },
  {
    id: 'ogrenci-5', kind: 'student', n: 5, short: 'Performans karşılaştırması',
    contribution: 'Aynı üç işlemi (groupby, join, window) pandas ve PySpark ile CSV ve Parquet üzerinde 100K–19M ve sentetik 57M kayıtta ölçüp süre ve belleği karşılaştırdı; bu dashboard\'u hazırladı.',
    items: [
      { type: 'code', where: 'notebooks/05_processing_benchmark.ipynb · hücre 3 (canlı, 1M Parquet; kısaltılmış) · aynı işlem, iki motor', code:
`pdf = bp.pandas_load(path)
sdf = bp.spark_frame(spark, path, "parquet_snappy", schema)
rows = []
for op in bp.OPERATIONS:
    p_fn, col = bp.pandas_ops(zones)[op]
    s_fn, _ = bp.spark_ops(s_zones)[op]
    p = bp.checksum(p_fn(pdf), col)
    s = bp.checksum(s_fn(sdf).toPandas(), col)
    rows.append({"işlem": op, "pandas satır": p[0], "Spark satır": s[0],
                 "pandas checksum": round(p[1], 2), "Spark checksum": round(s[1], 2)})`,
        output: { source: 'notebook çıktısı (hücre 3)', table: nbTable('05_processing_benchmark', 3) } },
      { type: 'table', title: 'Karşılaştırma: süre, bellek, disk ve hızlanma', source: 'reports/results/s5_processing.csv + size_sets_manifest.csv + s5_synthetic_manifest.csv', table: benchmarkTable,
        note: '57M (sentetik) için düz Spark ölçümü yok; bu boyutta yalnızca önbellekli Spark (Spark + cache) ölçüldü. Önbellekli satırlarda süreye açılış ve önbelleğe alma dahil.',
        ann: A('Yükleme (Spark\'ta oturum açılışı) ve üç işlemin toplam süresi, tepe bellek ve dosya boyutu; 4 veri büyüklüğü.', 'Her boyutta pandas + CSV 1× kabul edildi; 2× iki kat hızlı, 0,5× iki kat yavaş demek.', '19M\'de pandas + CSV (23,6 sn) Spark + CSV\'nin (22,2 sn) gerisine düşüyor; Parquet ise pandas\'ı 4,5× hızlandırıp 57M\'de bile en hızlı yapıyor.') },
      { type: 'chart', key: '05_processing_ders:7:1', title: 'Uçtan uca süre',
        ann: A('Okuma ve üç işlemin toplam süresi: pandas vs Spark, CSV vs Parquet.', 'İki eksen log; turuncu pandas, mavi Spark, kesikli çizgi CSV.', 'CSV\'de Spark ~16,1M kayıtta pandas\'ı geçiyor; Parquet\'te 19M\'de bile geçemiyor (5,3 vs 13,5 sn), çünkü Parquet pandas\'ın okuma darboğazını kaldırıyor.') },
      { type: 'chart', key: '05_processing_ders:13:1', title: 'İşleme göre süre',
        ann: A('groupby, join ve window işlemlerinin ayrı süreleri.', 'Üç panel; turuncu pandas, mavi Spark.', 'join\'de Spark küçük bölge tablosunu her göreve yayınladığı için önde; window\'da veriyi bölgeye göre taşımak (shuffle) zorunda olduğundan geride.') },
      { type: 'chart', key: '05_processing_benchmark:17:2', title: 'Süre nereye gidiyor? (19M ve 57M)',
        ann: A('Uçtan uca sürenin adımlara dökümü; 57M, 19M\'in üç katı sentetik veri.', 'Yığılmış çubuk: her renk bir adım (açılış, yükleme, groupby, join, window).', '57M\'de pandas + Parquet 21,4 sn ile hâlâ en hızlı, önbellekli Spark 36,8 sn; pandas + CSV\'de sürenin çoğu yüklemeye gidiyor.') },
      { type: 'sim', id: 'partition' },
    ],
    answer: [
      'Klasik yaklaşım (pandas + CSV) ~16M kayıttan sonra Spark\'ın gerisine düşüyor: 19M\'de uçtan uca 23,6 sn, Spark + CSV 22,2 sn. Çünkü darboğaz işlem değil, CSV\'nin tek çekirdekte satır satır çözülmesi.',
      'İkinci ve sert sınır bellek: pandas\'ın belleği veriyle doğrusal büyüyor (57M\'de 9,1 GB) ve RAM dolduğunda pandas hiç çalışamıyor.',
      'Formatı Parquet\'e çevirmek motoru değiştirmekten daha etkili: pandas + Parquet 57M\'de bile 21,4 sn ile önbellekli Spark\'tan (36,8 sn) hızlı.',
    ],
  },
  { id: 'tesekkurler', kind: 'thanks', short: 'Teşekkürler' },
]
