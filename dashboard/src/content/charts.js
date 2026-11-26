// Notebook görsellerinin ECharts karşılıkları. Tüm sayılar results/*.json'dan (reports/results/*.csv).
import { results as R } from '../lib/data.js'
import { trDec, trInt, sizeLabel } from '../lib/format.js'

export const C = { blue: '#338ced', orange: '#e76624', aqua: '#00a888', yellow: '#d28e05', magenta: '#e24d8b', green: '#0aac23', violet: '#9887ef', red: '#eb6263', grey: '#8a93ab', taxi: '#ffc629' }
const ENGINE = { pandas: C.orange, spark: C.blue, spark_cache: C.aqua }
const ENGINE_L = { pandas: 'pandas', spark: 'Spark', spark_cache: 'Spark + cache' }
const FMT_L = { csv: 'CSV', parquet_snappy: 'Parquet', parquet_gzip: 'Parquet gzip', parquet_zstd: 'Parquet zstd' }
const SIZES = ['100k', '1m', '5m', '10m', 'full']
const ink = '#a9b1c7'
const grid = '#1d2642'

const axis = (extra = {}) => ({ axisLine: { lineStyle: { color: '#2a3558' } }, axisLabel: { color: ink, fontSize: 13 }, splitLine: { lineStyle: { color: grid } }, nameTextStyle: { color: ink, fontSize: 13 }, ...extra })
const base = (o) => ({
  backgroundColor: 'transparent', textStyle: { fontFamily: 'Inter Variable, sans-serif' },
  tooltip: { trigger: 'axis', backgroundColor: '#141b31', borderColor: '#2a3558', textStyle: { color: '#e8ebf4' }, valueFormatter: (v) => (typeof v === 'number' ? trDec(v, Math.abs(v) < 10 ? 3 : 1) : v) },
  legend: { top: 0, textStyle: { color: ink, fontSize: 13 }, type: 'scroll' },
  toolbox: { right: 0, top: 0, feature: { dataZoom: { yAxisIndex: 'none', title: { zoom: 'Yakınlaştır', back: 'Geri al' } }, restore: { title: 'Sıfırla' } }, iconStyle: { borderColor: ink } },
  ...o,
})
const logX = (name = 'Kayıt sayısı (log)') => axis({ type: 'log', name, nameLocation: 'middle', nameGap: 30, axisLabel: { color: ink, formatter: (v) => sizeLabel(v) } })
const logY = (name) => axis({ type: 'log', name, nameLocation: 'middle', nameGap: 48 })
const valY = (name) => axis({ type: 'value', name, nameLocation: 'middle', nameGap: 52, scale: true })
const line = (name, data, color, dashed = false, extra = {}) => ({ name, type: 'line', data, symbolSize: 8, lineStyle: { width: 2.5, color, type: dashed ? 'dashed' : 'solid' }, itemStyle: { color }, ...extra })

// ---------------------------------------------------------------- Öğrenci 1
function readTime() {
  const io = R.io_benchmark.filter((r) => r.mode === 'all')
  const series = []
  for (const engine of ['pandas', 'spark']) {
    for (const fmt of ['csv', 'parquet_snappy', 'parquet_gzip', 'parquet_zstd']) {
      const pts = io.filter((r) => r.engine === engine && r.format === fmt).sort((a, b) => a.rows - b.rows)
      series.push(line(`${ENGINE_L[engine]} · ${FMT_L[fmt]}`, pts.map((r) => [r.rows, r.seconds_mean]), ENGINE[engine], fmt === 'csv', { symbol: fmt === 'csv' ? 'circle' : fmt === 'parquet_snappy' ? 'rect' : fmt === 'parquet_gzip' ? 'triangle' : 'diamond' }))
    }
  }
  return base({ grid: { left: 70, right: 30, top: 60, bottom: 50 }, xAxis: logX(), yAxis: logY('Okuma süresi (sn, log)'), series, tooltip: { trigger: 'item', backgroundColor: '#141b31', textStyle: { color: '#e8ebf4' }, formatter: (p) => `${p.seriesName}<br>${sizeLabel(p.value[0])} kayıt · <b>${trDec(p.value[1], 2)} sn</b>` } })
}
function diskSize() {
  const m = R.size_sets_manifest
  const fmts = ['csv', 'parquet_snappy', 'parquet_gzip', 'parquet_zstd']
  const cols = [C.orange, C.blue, C.aqua, C.violet]
  return base({ grid: { left: 70, right: 20, top: 50, bottom: 40 }, xAxis: axis({ type: 'category', data: SIZES.map((s) => (s === 'full' ? '19M' : s.toUpperCase())) }), yAxis: logY('Disk boyutu (MB, log)'),
    series: fmts.map((f, i) => ({ name: FMT_L[f], type: 'bar', data: SIZES.map((s) => m.find((r) => r.size === s && r.format === f)?.mb), itemStyle: { color: cols[i], borderRadius: [4, 4, 0, 0] } })) })
}
function endToEndS1() {
  const io = R.io_benchmark.filter((r) => r.mode === 'all')
  const ok = R.spark_startup.filter((r) => r.status === 'ok')
  const startup = ok.reduce((s, r) => s + r.seconds, 0) / ok.length
  const series = []
  for (const [engine, fmt] of [['pandas', 'csv'], ['pandas', 'parquet_snappy'], ['spark', 'csv'], ['spark', 'parquet_snappy']]) {
    const pts = io.filter((r) => r.engine === engine && r.format === fmt).sort((a, b) => a.rows - b.rows)
    const add = engine === 'spark' ? startup : 0
    series.push(line(`${ENGINE_L[engine]} · ${FMT_L[fmt]}${engine === 'spark' ? ' (açılış dahil)' : ''}`, pts.map((r) => [r.rows, r.seconds_mean + add]), ENGINE[engine], fmt === 'csv'))
  }
  return { ...base({ grid: { left: 70, right: 30, top: 60, bottom: 50 }, xAxis: logX(), yAxis: logY('Süre (sn, log)'), series }), note: `Spark serilerine spark_startup.csv'deki ${ok.length} ölçümün ortalaması (${trDec(startup, 2)} sn) eklendi.` }
}
function pandasMemory() {
  const io = R.io_benchmark.filter((r) => r.mode === 'all' && r.engine === 'pandas')
  return base({ grid: { left: 80, right: 30, top: 60, bottom: 50 }, xAxis: logX(), yAxis: valY('Tepe bellek (MB)'),
    series: ['csv', 'parquet_snappy', 'parquet_gzip', 'parquet_zstd'].map((f, i) => line(FMT_L[f], io.filter((r) => r.format === f).sort((a, b) => a.rows - b.rows).map((r) => [r.rows, r.peak_mb]), [C.orange, C.blue, C.aqua, C.violet][i], f === 'csv')) })
}
function columnPruning() {
  const io = R.io_benchmark.filter((r) => r.size === 'full')
  const combos = []
  for (const e of ['pandas', 'spark']) for (const f of ['csv', 'parquet_snappy', 'parquet_gzip', 'parquet_zstd']) combos.push([e, f])
  const get = (e, f, mode) => io.find((r) => r.engine === e && r.format === f && r.mode === mode)?.seconds_mean
  return base({ grid: { left: 70, right: 20, top: 50, bottom: 70 }, xAxis: axis({ type: 'category', data: combos.map(([e, f]) => `${ENGINE_L[e]}\n${FMT_L[f]}`) }), yAxis: valY('Okuma süresi, 19M (sn)'),
    series: [{ name: '20 kolon', type: 'bar', data: combos.map(([e, f]) => get(e, f, 'all')), itemStyle: { color: C.grey } }, { name: '3 kolon', type: 'bar', data: combos.map(([e, f]) => get(e, f, '3cols')), itemStyle: { color: C.taxi } }] })
}

// ---------------------------------------------------------------- Öğrenci 2
function ruleImpact() {
  const rows = R.cleaning_impact.filter((r) => r.rule !== 'TOTAL')
  return base({ grid: { left: 90, right: 20, top: 50, bottom: 40 }, xAxis: axis({ type: 'category', data: rows.map((r) => r.rule) }), yAxis: valY('Kayıt'),
    series: [['isolated', 'Tek başına', C.grey], ['sequential', 'Sıralı (silinen)', C.taxi], ['unique', 'Yalnız bu kural', C.blue]].map(([k, n, c]) => ({ name: n, type: 'bar', data: rows.map((r) => r[k]), itemStyle: { color: c } })) })
}
function ruleVsStat() {
  const rows = R.anomaly_rule_vs_stat
  const cats = rows.map((r) => `${r.metric}\n${r.stat_method}`)
  return base({ grid: { left: 90, right: 20, top: 50, bottom: 70 }, xAxis: axis({ type: 'category', data: cats }), yAxis: logY('Kayıt (log)'),
    series: [['rule_only', 'Yalnız kural', C.taxi], ['both', 'İkisi de', C.green], ['stat_only', 'Yalnız istatistik', C.blue]].map(([k, n, c]) => ({ name: n, type: 'bar', data: rows.map((r) => Math.max(r[k], 0.9)), itemStyle: { color: c } })) })
}
function ruleRatesHeatmap() {
  const rows = R.anomaly_rule_rates
  const rules = ['R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R10']
  const ylab = rows.map((r) => `${r.dimension === 'VendorID' ? 'Vendor' : 'İlçe'} ${r.value ?? 'boş'}`)
  const data = []
  rows.forEach((r, y) => rules.forEach((k, x) => data.push([x, y, r[k]])))
  return base({ tooltip: { position: 'top', formatter: (p) => `${ylab[p.value[1]]} · ${rules[p.value[0]]}<br><b>%${trDec(p.value[2], 2)}</b>` }, grid: { left: 150, right: 90, top: 30, bottom: 40 },
    xAxis: axis({ type: 'category', data: rules }), yAxis: axis({ type: 'category', data: ylab }),
    visualMap: { min: 0, max: 100, calculable: true, orient: 'vertical', right: 0, top: 'middle', textStyle: { color: ink }, inRange: { color: ['#141b31', '#5b3a12', '#ffc629'] } },
    series: [{ type: 'heatmap', data, label: { show: true, color: '#e8ebf4', fontSize: 11, formatter: (p) => (p.value[2] >= 1 ? trDec(p.value[2], 0) : '') } }] })
}

// ---------------------------------------------------------------- Öğrenci 3
function hourly() {
  const h = [...R.s3_hour].sort((a, b) => a.pickup_hour - b.pickup_hour)
  const peak = Math.max(...h.map((r) => r.trip_count))
  return base({ grid: { left: 80, right: 20, top: 40, bottom: 40 }, xAxis: axis({ type: 'category', data: h.map((r) => `${r.pickup_hour}`), name: 'Biniş saati', nameLocation: 'middle', nameGap: 28 }), yAxis: valY('Biniş (5 ay toplam)'),
    series: [{ name: 'Biniş', type: 'bar', data: h.map((r) => ({ value: r.trip_count, itemStyle: { color: r.trip_count === peak ? C.taxi : C.blue } })) }] })
}
const weekdayOf = (iso) => (new Date(`${iso}T12:00:00Z`).getUTCDay() + 6) % 7 // 0 = Pazartesi
const DAYS_TR = ['Pazartesi', 'Salı', 'Çarşamba', 'Perşembe', 'Cuma', 'Cumartesi', 'Pazar']
function weekdayCounts() {
  const n = Array(7).fill(0)
  R.s3_day.forEach((d) => { n[weekdayOf(d.pickup_date)] += 1 })
  return n
}
function weekdayWeekend() {
  const days = weekdayCounts()
  const wk = Array(24).fill(0); const we = Array(24).fill(0)
  R.s3_pattern.forEach((r) => { const wd = (r.pickup_dow + 5) % 7; (wd >= 5 ? we : wk)[r.pickup_hour] += r.trip_count })
  const nwk = days.slice(0, 5).reduce((a, b) => a + b, 0); const nwe = days[5] + days[6]
  return base({ grid: { left: 80, right: 20, top: 50, bottom: 40 }, xAxis: axis({ type: 'category', data: [...Array(24).keys()].map(String), name: 'Biniş saati', nameLocation: 'middle', nameGap: 28 }), yAxis: valY('Gün başına ortalama biniş'),
    series: [line(`Hafta içi (${nwk} gün)`, wk.map((v) => v / nwk), C.blue), line(`Hafta sonu (${nwe} gün)`, we.map((v) => v / nwe), C.orange)] })
}
function weekdayHeatmap() {
  const days = weekdayCounts()
  const data = R.s3_pattern.map((r) => { const wd = (r.pickup_dow + 5) % 7; return [r.pickup_hour, wd, r.trip_count / days[wd]] })
  const max = Math.max(...data.map((d) => d[2]))
  return base({ tooltip: { position: 'top', formatter: (p) => `${DAYS_TR[p.value[1]]} ${p.value[0]}.00<br><b>${trInt(p.value[2])}</b> ortalama biniş` }, grid: { left: 100, right: 90, top: 20, bottom: 40 },
    xAxis: axis({ type: 'category', data: [...Array(24).keys()].map(String) }), yAxis: axis({ type: 'category', data: DAYS_TR, inverse: true }),
    visualMap: { min: 0, max, calculable: true, right: 0, top: 'middle', textStyle: { color: ink }, inRange: { color: ['#0f1528', '#1f4e8c', '#338ced', '#ffc629'] } },
    series: [{ type: 'heatmap', data }] })
}
function daily() {
  const d = R.s3_day
  return base({ grid: { left: 80, right: 20, top: 50, bottom: 70 }, dataZoom: [{ type: 'inside' }, { type: 'slider', bottom: 10, textStyle: { color: ink } }],
    xAxis: axis({ type: 'category', data: d.map((r) => r.pickup_date) }), yAxis: valY('Günlük biniş'),
    series: [line('Günlük biniş', d.map((r) => r.trip_count), C.blue, false, { symbolSize: 3, lineStyle: { width: 1.5, color: C.blue, opacity: 0.6 } }), line('7 günlük ortalama', d.map((r) => r.trailing_7d_avg), C.taxi)] })
}
function topZones() {
  const z = R.s3_zone.slice(0, 10).reverse()
  return base({ grid: { left: 260, right: 40, top: 20, bottom: 40 }, tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } }, xAxis: valY('Biniş'), yAxis: axis({ type: 'category', data: z.map((r) => `${r.pickup_zone} (${r.PULocationID})`) }),
    series: [{ name: 'Biniş', type: 'bar', data: z.map((r, i) => ({ value: r.trip_count, itemStyle: { color: i === z.length - 1 ? C.taxi : C.blue } })) }] })
}
function queensCumulative() {
  const q = R.s3_zone.filter((r) => r.borough === 'Queens').sort((a, b) => b.trip_count - a.trip_count)
  const total = q.reduce((s, r) => s + r.trip_count, 0)
  let cum = 0
  const pts = q.slice(0, 12).map((r) => { cum += r.trip_count; return [r.pickup_zone, (100 * cum) / total] })
  return base({ grid: { left: 70, right: 20, top: 40, bottom: 110 }, xAxis: axis({ type: 'category', data: pts.map((p) => p[0]), axisLabel: { color: ink, rotate: 35 } }), yAxis: valY('Birikimli pay (%)'),
    series: [line('Queens birikimli biniş payı', pts.map((p) => p[1]), C.taxi, false, { markLine: { symbol: 'none', lineStyle: { color: C.red, type: 'dashed' }, label: { color: ink, formatter: '%80' }, data: [{ yAxis: 80 }] } })] })
}
function payment() {
  const p = [...R.s3_payment].sort((a, b) => b.share_pct - a.share_pct)
  const lab = { 0: 'Flex Fare (0)', 1: 'Kredi kartı (1)', 2: 'Nakit (2)', 3: 'Ücret alınmadı (3)', 4: 'İtiraz (4)' }
  return base({ grid: { left: 170, right: 60, top: 20, bottom: 40 }, tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } }, xAxis: valY('Yolculuk payı (%)'), yAxis: axis({ type: 'category', data: p.map((r) => lab[r.payment_type]).reverse() }),
    series: [{ name: 'Pay (%)', type: 'bar', data: p.map((r) => r.share_pct).reverse(), itemStyle: { color: C.blue }, label: { show: true, position: 'right', color: ink, formatter: (x) => `%${trDec(x.value, 2)}` } }] })
}

// ---------------------------------------------------------------- Öğrenci 4
const S4 = ['100K', '1M', '5M', '17M']
const S4_SERIES = [['PySpark', 'LinearRegression', C.blue, true], ['PySpark', 'GBTRegressor', C.blue, false], ['scikit-learn', 'LinearRegression', C.orange, true], ['scikit-learn', 'HistGradientBoosting', C.orange, false]]
function s4Lines(metric, name, log = false) {
  const c = R.s4_comparison
  return base({ grid: { left: 80, right: 20, top: 60, bottom: 40 }, xAxis: axis({ type: 'category', data: S4, name: 'Örneklem', nameLocation: 'middle', nameGap: 28 }), yAxis: log ? logY(name) : valY(name),
    series: S4_SERIES.map(([fw, m, col, dashed]) => line(`${fw} · ${m}`, S4.map((s) => c.find((r) => r.framework === fw && r.model === m && r.size_label === s)?.[metric]), col, dashed)) })
}
function importanceScatter() {
  const imp = [...R.s4_feature_importance].sort((a, b) => a.importance - b.importance)
  const pts = R.s4_pred_sample.map((r) => [r.trip_duration_min, r.prediction])
  return base({ tooltip: { trigger: 'item' }, grid: [{ left: 190, right: '56%', top: 40, bottom: 40 }, { left: '52%', right: 20, top: 40, bottom: 50 }],
    xAxis: [axis({ type: 'value', gridIndex: 0, name: 'Önem', nameLocation: 'middle', nameGap: 26 }), axis({ type: 'value', gridIndex: 1, name: 'Gerçek süre (dk)', nameLocation: 'middle', nameGap: 28 })],
    yAxis: [axis({ type: 'category', gridIndex: 0, data: imp.map((r) => r.feature) }), axis({ type: 'value', gridIndex: 1, name: 'Tahmin (dk)', nameLocation: 'middle', nameGap: 40 })],
    series: [{ name: 'Özellik önemi', type: 'bar', data: imp.map((r) => r.importance), itemStyle: { color: C.blue } },
      { name: `Gerçek vs tahmin (${trInt(pts.length)} test kaydı)`, type: 'scatter', xAxisIndex: 1, yAxisIndex: 1, data: pts, symbolSize: 3, itemStyle: { color: C.blue, opacity: 0.35 }, large: true,
        markLine: { symbol: 'none', silent: true, lineStyle: { color: ink, type: 'dashed' }, data: [[{ coord: [0, 0] }, { coord: [180, 180] }]] } }] })
}
function ablationTuning() {
  const ab = R.s4_ablation; const val = R.s4_tuning.filter((r) => r.stage === 'validation').sort((a, b) => a.rmse - b.rmse)
  const lbl = (r) => `${r.max_iter} ağaç · d=${r.max_depth ?? '∞'} · lr=${trDec(r.learning_rate, 1)}`
  return base({ tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } }, grid: [{ left: 230, right: '55%', top: 40, bottom: 40 }, { left: '62%', right: 20, top: 40, bottom: 40 }],
    xAxis: [axis({ type: 'value', gridIndex: 0, name: 'Test RMSE (dk, 5M)', nameLocation: 'middle', nameGap: 26 }), axis({ type: 'value', gridIndex: 1, scale: true, name: 'Doğrulama RMSE (dk, 1M)', nameLocation: 'middle', nameGap: 26 })],
    yAxis: [axis({ type: 'category', gridIndex: 0, data: ab.map((r) => r.label).reverse() }), axis({ type: 'category', gridIndex: 1, data: val.map(lbl).reverse(), axisLabel: { color: ink, fontSize: 11 } })],
    series: [{ name: 'Özellik seti', type: 'bar', data: ab.map((r) => r.rmse).reverse(), itemStyle: { color: C.blue } },
      { name: 'Izgara ayarı', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: val.map((r, i) => ({ value: r.rmse, itemStyle: { color: i === 0 ? C.taxi : C.grey } })).reverse() }] })
}
function errorAnalysis() {
  const e = R.s4_error_analysis
  const dims = [['distance_bin', 'Mesafe dilimi (mil)'], ['pickup_hour', 'Biniş saati'], ['Borough', 'Biniş ilçesi']]
  const groups = dims.map(([d]) => { let g = e.filter((r) => r.dimension === d); if (d === 'pickup_hour') g = g.sort((a, b) => Number(a.group) - Number(b.group)); if (d === 'Borough') g = g.sort((a, b) => b.n - a.n); return g })
  return base({ grid: dims.map((_, i) => ({ left: `${4 + i * 33}%`, width: '27%', top: 60, bottom: 70 })),
    title: dims.map(([, t], i) => ({ text: t, left: `${4 + i * 33}%`, top: 28, textStyle: { color: ink, fontSize: 14, fontWeight: 600 } })),
    xAxis: groups.map((g, i) => axis({ type: 'category', gridIndex: i, data: g.map((r) => (i === 2 ? `${r.group}\n(n=${trInt(r.n)})` : String(r.group))), axisLabel: { color: ink, fontSize: 11, rotate: i === 2 ? 30 : 0 } })),
    yAxis: groups.map((_, i) => axis({ type: 'value', gridIndex: i })),
    series: groups.flatMap((g, i) => [{ name: 'MAE (dk)', type: 'bar', xAxisIndex: i, yAxisIndex: i, data: g.map((r) => r.mae), itemStyle: { color: C.blue } }, { name: 'Ortalama sapma (dk)', type: 'scatter', xAxisIndex: i, yAxisIndex: i, data: g.map((r) => r.bias), itemStyle: { color: C.orange }, symbolSize: 9 }]) })
}

// ---------------------------------------------------------------- Öğrenci 5
const S5 = (realOnly = false) => R.s5_processing.filter((r) => r.size !== 'full_rg16' && (!realOnly || r.size !== 'x3'))
const OPS = [['groupby_hour', 'groupby: saate göre özet'], ['join_borough', 'join: bölge → ilçe'], ['window_top3', 'window: bölge başına ilk 3']]
function s5Combos(engines) {
  const out = []
  for (const e of engines) for (const f of ['csv', 'parquet_snappy']) out.push([e, f])
  return out
}
function opTime(engines, realOnly = false) {
  const s = S5(realOnly)
  return base({ tooltip: { trigger: 'item', backgroundColor: '#141b31', textStyle: { color: '#e8ebf4' }, formatter: (p) => `${p.seriesName}<br>${sizeLabel(p.value[0])} · <b>${trDec(p.value[1], 3)} sn</b>` },
    title: OPS.map(([, t], i) => ({ text: t, left: `${5 + i * 32}%`, top: 34, textStyle: { color: ink, fontSize: 14, fontWeight: 600 } })),
    grid: OPS.map((_, i) => ({ left: `${5 + i * 32}%`, width: '27%', top: 70, bottom: 50 })),
    xAxis: OPS.map((_, i) => ({ ...logX(''), gridIndex: i })), yAxis: OPS.map((_, i) => ({ ...logY(i === 0 ? 'İşlem süresi (sn, log)' : ''), gridIndex: i })),
    series: OPS.flatMap(([op], i) => s5Combos(engines).map(([e, f]) => ({ ...line(`${ENGINE_L[e]} + ${FMT_L[f]}`, s.filter((r) => r.engine === e && r.format === f && r.op === op).sort((a, b) => a.rows - b.rows).map((r) => [r.rows, r.seconds_mean]), ENGINE[e], f === 'csv'), xAxisIndex: i, yAxisIndex: i }))) })
}
const SETUP = { pandas: ['load'], spark: ['startup'], spark_cache: ['startup', 'load'] }
export function endToEndRows(rows = S5()) {
  const map = new Map()
  for (const r of rows) {
    const k = `${r.engine}|${r.size}|${r.format}`
    if (!map.has(k)) map.set(k, { engine: r.engine, size: r.size, format: r.format, rows: null, setup: 0, ops: 0, nOps: 0, peak: 0 })
    const m = map.get(k)
    if (r.rows) m.rows = r.rows
    if (SETUP[r.engine].includes(r.op)) m.setup += r.seconds_mean
    if (OPS.some(([o]) => o === r.op)) { m.ops += r.seconds_mean; m.nOps += 1 }
    m.peak = Math.max(m.peak, r.peak_rss_mb)
  }
  return [...map.values()].filter((m) => m.nOps === 3).map((m) => ({ ...m, total: m.setup + m.ops }))
}
function e2e(engines, metric = 'total', name = 'Uçtan uca süre (sn, log)', log = true, realOnly = false) {
  const e = endToEndRows(S5(realOnly))
  return base({ tooltip: { trigger: 'item', backgroundColor: '#141b31', textStyle: { color: '#e8ebf4' }, formatter: (p) => `${p.seriesName}<br>${sizeLabel(p.value[0])}${p.value[0] > 2e7 ? ' (sentetik ×3)' : ''} · <b>${metric === 'peak' ? `${trInt(p.value[1])} MB` : `${trDec(p.value[1], 2)} sn`}</b>` },
    grid: { left: 80, right: 30, top: 60, bottom: 50 }, xAxis: logX(), yAxis: log ? logY(name) : valY(name),
    series: s5Combos(engines).map(([en, f]) => line(`${ENGINE_L[en]} + ${FMT_L[f]}`, e.filter((r) => r.engine === en && r.format === f).sort((a, b) => a.rows - b.rows).map((r) => [r.rows, r[metric]]), ENGINE[en], f === 'csv')) })
}
function breakdown() {
  const s = S5()
  const steps = [['startup', 'Spark oturum açılışı', C.grey], ['load', 'Yükleme / önbelleğe alma', C.blue], ['groupby_hour', 'groupby', C.orange], ['join_borough', 'join', C.aqua], ['window_top3', 'window', C.yellow]]
  const cats = []; const sizes = [['full', '19M'], ['x3', '57M']]
  for (const [sz, sl] of sizes) for (const [e, f] of s5Combos(['pandas', 'spark', 'spark_cache'])) if (s.some((r) => r.size === sz && r.engine === e && r.format === f)) cats.push([sz, e, f, `${sl} · ${ENGINE_L[e]} + ${FMT_L[f]}`])
  return base({ tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } }, grid: { left: 250, right: 30, top: 50, bottom: 40 }, xAxis: valY('Süre (sn)'), yAxis: axis({ type: 'category', data: cats.map((c) => c[3]), inverse: true }),
    series: steps.map(([op, n, col]) => ({ name: n, type: 'bar', stack: 't', itemStyle: { color: col }, data: cats.map(([sz, e, f]) => s.find((r) => r.size === sz && r.engine === e && r.format === f && r.op === op)?.seconds_mean ?? 0) })) })
}

/** Görsel anahtarı (slug:hücre:çıktı) → { title, source, option() } */
export const CHARTS = {
  '01_data_acquisition_and_formats:7:0': { source: 'size_sets_manifest.csv', option: diskSize },
  '01_data_acquisition_and_formats:12:0': { source: 'io_benchmark.csv', option: readTime },
  '01_data_acquisition_and_formats:16:0': { source: 'io_benchmark.csv + spark_startup.csv', option: endToEndS1 },
  '01_data_acquisition_and_formats:21:0': { source: 'io_benchmark.csv', option: pandasMemory },
  '01_data_acquisition_and_formats:24:0': { source: 'io_benchmark.csv', option: columnPruning },
  '02_cleaning_and_anomalies:7:0': { source: 'cleaning_impact.csv', option: ruleImpact },
  '02_cleaning_and_anomalies:13:0': { source: 'anomaly_rule_vs_stat.csv', option: ruleVsStat },
  '02_cleaning_and_anomalies:23:0': { source: 'anomaly_rule_rates.csv', option: ruleRatesHeatmap, height: 520 },
  '03_aggregation_ders:4:0': { source: 's3_hour.csv', option: hourly },
  '03_aggregation_ders:8:0': { source: 's3_pattern.csv + s3_day.csv', option: weekdayWeekend },
  '03_aggregation_ders:11:0': { source: 's3_pattern.csv + s3_day.csv', option: weekdayHeatmap },
  '03_aggregation_ders:17:0': { source: 's3_day.csv', option: daily },
  '03_aggregation_ders:30:0': { source: 's3_zone.csv', option: topZones },
  '03_aggregation_ders:32:0': { source: 's3_zone.csv + taxi_zones (TLC)', map: true },
  '03_aggregation_ders:34:0': { source: 's3_zone.csv', option: queensCumulative },
  '03_aggregation_ders:41:0': { source: 's3_payment.csv', option: payment },
  '04_ml_duration_ders:10:1': { source: 's4_comparison.csv', option: () => s4Lines('r2', 'R²') },
  '04_ml_duration_ders:13:1': { source: 's4_comparison.csv', option: () => s4Lines('train_time_s', 'Eğitim süresi (sn, log)', true) },
  '04_ml_duration_ders:16:1': { source: 's4_comparison.csv', option: () => s4Lines('peak_rss_mb', 'Tepe bellek (MB)') },
  '04_ml_duration_ders:19:1': { source: 's4_feature_importance.csv + s4_pred_sample.csv', option: importanceScatter, height: 460 },
  '04_ml_duration_ders:26:2': { source: 's4_ablation.csv + s4_tuning.csv', option: ablationTuning, height: 460 },
  '04_ml_duration_ders:28:1': { source: 's4_error_analysis.csv', option: errorAnalysis, height: 460 },
  '04_ml_duration_prediction:8:0': { source: 's4_comparison.csv', option: () => s4Lines('train_time_s', 'Eğitim süresi (sn, log)', true) },
  '04_ml_duration_prediction:8:1': { source: 's4_comparison.csv', option: () => s4Lines('rmse', 'RMSE (dk)') },
  '04_ml_duration_prediction:10:1': { source: 's4_feature_importance.csv + s4_pred_sample.csv', option: importanceScatter, height: 460 },
  '04_ml_duration_prediction:12:2': { source: 's4_ablation.csv + s4_tuning.csv', option: ablationTuning, height: 460 },
  '04_ml_duration_prediction:13:1': { source: 's4_error_analysis.csv', option: errorAnalysis, height: 460 },
  '05_processing_benchmark:9:0': { source: 's5_processing.csv', option: () => opTime(['pandas', 'spark', 'spark_cache']), height: 460 },
  '05_processing_benchmark:13:2': { source: 's5_processing.csv', option: () => e2e(['pandas', 'spark', 'spark_cache']) },
  '05_processing_benchmark:14:2': { source: 's5_processing.csv', option: () => e2e(['pandas', 'spark', 'spark_cache'], 'peak', 'Tepe bellek (MB)', false) },
  '05_processing_benchmark:17:2': { source: 's5_processing.csv', option: breakdown, height: 520 },
  '05_processing_ders:7:1': { source: 's5_processing.csv', option: () => e2e(['pandas', 'spark'], 'total', 'Uçtan uca süre (sn, log)', true, true) },
  '05_processing_ders:13:1': { source: 's5_processing.csv', option: () => opTime(['pandas', 'spark'], true), height: 460 },
  '05_processing_ders:16:1': { source: 's5_processing.csv', option: () => e2e(['pandas', 'spark'], 'peak', 'Tepe bellek (MB)', false, true) },
  '05_processing_ders:25:2': { source: 's5_processing.csv', option: breakdown, height: 520 },
}
