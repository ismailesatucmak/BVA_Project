import { useEffect, useMemo, useRef, useState } from 'react'
import { loadSample, results as R } from '../lib/data.js'
import zones from '../data/zones.json'
import { trDec, trInt } from '../lib/format.js'
import { C } from '../content/charts.js'
import { EChart, fullDataZoneCounts, zoneMapOption } from '../components/EChart.jsx'
import SimShell, { Range, Select, Toggle } from './SimShell.jsx'
import {
  CLEAN_DEFAULTS, cleaningImpact, groupCount, partitionedGroupCount, topNPerGroup, boroughJoin,
} from '../sim/algorithms.js'

const cache = {}
function useSample(kind) {
  const [rows, setRows] = useState(cache[kind] || null)
  useEffect(() => {
    if (!rows) loadSample(kind).then((r) => { cache[kind] = r; setRows(r) })
  }, [kind, rows])
  return rows
}
const ax = { axisLabel: { color: '#a9b1c7' }, axisLine: { lineStyle: { color: '#2a3558' } }, splitLine: { lineStyle: { color: '#1d2642' } } }
const tip = { trigger: 'axis', backgroundColor: '#141b31', borderColor: '#2a3558', textStyle: { color: '#e8ebf4' } }
const bar = (cats, series, extra = {}) => ({ backgroundColor: 'transparent', tooltip: tip, legend: { top: 0, textStyle: { color: '#a9b1c7' } }, grid: { left: 70, right: 20, top: 40, bottom: 50 }, xAxis: { type: 'category', data: cats, ...ax }, yAxis: { type: 'value', ...ax }, series, ...extra })
const Stat = ({ label, value, accent }) => (
  <div className="rounded-xl border border-night-700 bg-night-900 px-4 py-3"><div className="text-xs text-ink-faint">{label}</div><div className={`text-2xl font-extrabold num ${accent || 'text-ink'}`}>{value}</div></div>
)
function timed(fn) { const t = performance.now(); const v = fn(); return [v, performance.now() - t] }

// ------------------------------------------------------------------ 1) Temizlik hattı
export function CleaningSim() {
  const raw = useSample('raw')
  const [live, setLive] = useState(false); const [step, setStep] = useState(0)
  const [p, setP] = useState({ maxMiles: 100, maxDuration: 180, minDuration: 1, maxSpeed: 80, maxPassengers: 6 })
  const [flex, setFlex] = useState(true)
  const [res, ms] = useMemo(() => (raw ? timed(() => cleaningImpact(raw, p, { includeFlex: flex })) : [null, 0]), [raw, p, flex])
  const full = R.cleaning_impact.filter((r) => r.rule !== 'TOTAL')
  const set = (k) => (v) => setP((o) => ({ ...o, [k]: v }))
  const rules = res?.rules || []
  const shown = rules.slice(0, step + 1)
  let remaining = res?.rows || 0
  const funnel = shown.map((r) => { remaining -= r.sequential; return remaining })
  return (
    <SimShell title="Temizlik hattı: 10 kural sırayla" sampleN={raw && trInt(raw.length)} steps={rules.length || 9} step={step} setStep={setStep} live={live} setLive={setLive}
      stepLabel={rules[step] && `${rules[step].rule} uygulandı`}
      params={<>
        <Range label="En fazla mesafe (mil)" value={p.maxMiles} min={10} max={200} step={5} onChange={set('maxMiles')} />
        <Range label="En kısa süre (dk)" value={p.minDuration} min={0} max={5} step={0.5} onChange={set('minDuration')} />
        <Range label="En uzun süre (dk)" value={p.maxDuration} min={30} max={300} step={10} onChange={set('maxDuration')} />
        <Range label="En yüksek hız (mph)" value={p.maxSpeed} min={20} max={150} step={5} onChange={set('maxSpeed')} />
        <Range label="En fazla yolcu" value={p.maxPassengers} min={1} max={9} onChange={set('maxPassengers')} />
        <Toggle label="Flex Fare kayıtları dahil" value={flex} onChange={setFlex} />
        <button type="button" className="text-sm text-signal underline" onClick={() => { setP({ maxMiles: 100, maxDuration: 180, minDuration: 1, maxSpeed: 80, maxPassengers: 6 }); setFlex(true) }}>Proje eşiklerine dön (src/config.py)</button>
      </>}
      notebookView={<>
        <div className="mb-3 grid grid-cols-3 gap-3">
          <Stat label="Ham kayıt" value={trInt(R.cleaning_impact[0].total_rows)} />
          <Stat label="Silinen (en az bir kural)" value={trInt(R.cleaning_impact.find((r) => r.rule === 'TOTAL').sequential)} accent="text-warn" />
          <Stat label="Temiz kayıt" value={trInt(R.cleaning_before_after.find((r) => r.dataset === 'clean').rows)} accent="text-taxi" />
        </div>
        <EChart height={340} option={bar(full.map((r) => r.rule), [{ name: 'Sıralı silinen (tam veri)', type: 'bar', data: full.map((r) => r.sequential), itemStyle: { color: C.taxi } }])} />
      </>}
      liveView={res && <>
        <div className="mb-3 grid grid-cols-3 gap-3">
          <Stat label="Örneklem" value={trInt(res.rows)} />
          <Stat label="Silinen (tüm kurallar)" value={`${trInt(res.removed)} (%${trDec((100 * res.removed) / res.rows, 1)})`} accent="text-warn" />
          <Stat label="Hesaplama" value={`${trDec(ms, 1)} ms`} accent="text-signal" />
        </div>
        <EChart height={300} option={bar(rules.map((r) => r.rule), [
          { name: 'Kalan kayıt', type: 'bar', data: rules.map((_, i) => (i <= step ? funnel[i] : null)), itemStyle: { color: C.blue } },
          { name: 'Bu kuralın sildiği (sıralı)', type: 'bar', data: rules.map((r, i) => (i <= step ? r.sequential : null)), itemStyle: { color: C.taxi } }])} />
        <table className="mt-3 w-full text-sm num"><thead className="text-ink-muted"><tr><th className="text-left">Kural</th><th className="text-right">Tek başına</th><th className="text-right">Sıralı</th><th className="text-right">Yalnız bu kural</th></tr></thead>
          <tbody>{shown.map((r) => <tr key={r.rule} className="border-t border-night-700"><td>{r.rule} · {r.description}</td><td className="text-right">{r.isolated}</td><td className="text-right text-taxi">{r.sequential}</td><td className="text-right">{r.unique}</td></tr>)}</tbody></table>
      </>}
      explain="Kurallar src/quality.py ile aynı tanımlı (Python referansıyla birebir doğrulandı). R9 (tam kopya) örneklemde anlamsız olduğundan dışarıda; tam veride de 0 kayıt." />
  )
}

// ------------------------------------------------------------------ 2) Map → shuffle → reduce
const GROUPS = {
  hour: ['Biniş saati', (r) => r.pickup_hour, () => R.s3_hour.map((r) => [r.pickup_hour, r.trip_count])],
  dow: ['Hafta günü (1 = Pazar)', (r) => r.pickup_dow, () => { const m = new Map(); R.s3_pattern.forEach((r) => m.set(r.pickup_dow, (m.get(r.pickup_dow) || 0) + r.trip_count)); return [...m] }],
  payment: ['Ödeme türü', (r) => r.payment_type, () => R.s3_payment.map((r) => [r.payment_type, r.trip_count])],
}
export function AggregationSim() {
  const clean = useSample('clean')
  const [live, setLive] = useState(false); const [step, setStep] = useState(0)
  const [g, setG] = useState('hour'); const [parts, setParts] = useState(4); const [h0, setH0] = useState(0); const [h1, setH1] = useState(23)
  const [label, keyFn, fullFn] = GROUPS[g]
  const rows = useMemo(() => clean?.filter((r) => r.pickup_hour >= h0 && r.pickup_hour <= h1), [clean, h0, h1])
  const [res, ms] = useMemo(() => (rows ? timed(() => partitionedGroupCount(rows, keyFn, parts)) : [null, 0]), [rows, keyFn, parts])
  const single = useMemo(() => rows && groupCount(rows, keyFn), [rows, keyFn])
  const keys = res ? [...res.final.keys()].sort((a, b) => a - b) : []
  const same = res && keys.every((k) => res.final.get(k) === single.get(k))
  const full = fullFn().sort((a, b) => a[0] - b[0])
  const pal = [C.blue, C.orange, C.aqua, C.yellow, C.magenta, C.green, C.violet, C.red]
  const stages = ['1 · Veri bölümlere dağıtıldı', '2 · Map: her bölümde kısmi sayım (partial_count)', '3 · Shuffle + reduce: aynı anahtarlar birleşti']
  let view = null
  if (res) {
    if (step === 0) view = bar(res.parts.map((_, i) => `Bölüm ${i + 1}`), [{ name: 'Satır', type: 'bar', data: res.parts, itemStyle: { color: C.blue } }])
    else if (step === 1) view = bar(keys.map(String), res.partials.slice(0, 8).map((m, i) => ({ name: `Bölüm ${i + 1}`, type: 'bar', stack: 'p', data: keys.map((k) => m.get(k) || 0), itemStyle: { color: pal[i % 8] } })).concat(res.partials.length > 8 ? [{ name: `+${res.partials.length - 8} bölüm`, type: 'bar', stack: 'p', data: keys.map((k) => res.partials.slice(8).reduce((s, m) => s + (m.get(k) || 0), 0)), itemStyle: { color: C.grey } }] : []))
    else view = bar(keys.map(String), [{ name: 'Son sayım', type: 'bar', data: keys.map((k) => res.final.get(k)), itemStyle: { color: C.taxi } }])
  }
  return (
    <SimShell title="groupBy().count(): map → shuffle → reduce" sampleN={rows && trInt(rows.length)} steps={3} step={step} setStep={setStep} live={live} setLive={setLive} stepLabel={stages[step]}
      params={<>
        <Select label="Gruplama kolonu" value={g} onChange={setG} options={Object.entries(GROUPS).map(([k, v]) => [k, v[0]])} />
        <Range label="Bölüm (görev) sayısı" value={parts} min={1} max={20} onChange={setParts} />
        <Range label="Saat aralığı: başlangıç" value={h0} min={0} max={h1} onChange={setH0} />
        <Range label="Saat aralığı: bitiş" value={h1} min={h0} max={23} onChange={setH1} />
      </>}
      notebookView={<EChart height={360} option={bar(full.map((r) => String(r[0])), [{ name: `${label} · tam veri (17.210.961 yolculuk)`, type: 'bar', data: full.map((r) => r[1]), itemStyle: { color: C.blue } }])} />}
      liveView={res && <>
        <div className="mb-3 grid grid-cols-3 gap-3">
          <Stat label="Aşama" value={stages[step].split(' · ')[0]} accent="text-taxi" />
          <Stat label="Bölümlü = tek parça?" value={same ? '✔ Aynı' : '✘ Farklı'} accent={same ? 'text-ok' : 'text-warn'} />
          <Stat label="Hesaplama" value={`${trDec(ms, 1)} ms`} accent="text-signal" />
        </div>
        <div className="mb-1 font-semibold">{stages[step]}</div>
        <EChart height={340} option={view} />
      </>}
      explain="Notebook'taki Spark planı: Parquet taraması → HashAggregate (partial_count) → Exchange hashpartitioning → HashAggregate (son sayım). Bölüm sayısı değişse de sonuç değişmez; yalnızca işin dağılımı değişir." />
  )
}

// ------------------------------------------------------------------ 3) Pencere: 7 günlük ortalama + ilk N
export function WindowSim() {
  const clean = useSample('clean')
  const [live, setLive] = useState(false); const [step, setStep] = useState(0)
  const [k, setK] = useState(7); const [n, setN] = useState(3)
  const daily = useMemo(() => {
    if (!clean) return []
    const c = groupCount(clean, (r) => r.tpep_pickup_datetime.slice(0, 10))
    const days = [...c.keys()].sort()
    return days.map((d, i) => ({ d, v: c.get(d), avg: i >= k - 1 ? days.slice(i - k + 1, i + 1).reduce((s, x) => s + c.get(x), 0) / k : null }))
  }, [clean, k])
  const top = useMemo(() => clean && topNPerGroup(clean, 'PULocationID', 'total_amount', n), [clean, n])
  const cur = Math.min(step, Math.max(daily.length - 1, 0))
  const full = R.s3_day
  const opt = (dates, vals, avgs, mark) => ({ backgroundColor: 'transparent', tooltip: tip, legend: { top: 0, textStyle: { color: '#a9b1c7' } }, grid: { left: 70, right: 20, top: 40, bottom: 60 },
    xAxis: { type: 'category', data: dates, ...ax }, yAxis: { type: 'value', ...ax }, dataZoom: [{ type: 'inside' }],
    series: [{ name: 'Günlük biniş', type: 'line', data: vals, symbolSize: 3, lineStyle: { color: C.blue, width: 1.5 }, itemStyle: { color: C.blue },
      markArea: mark ? { itemStyle: { color: 'rgba(255,198,41,0.18)' }, data: [[{ xAxis: mark[0] }, { xAxis: mark[1] }]] } : undefined },
    { name: `${k} günlük ortalama`, type: 'line', data: avgs, symbol: 'none', lineStyle: { color: C.taxi, width: 2.5 } }] })
  const win = daily.length ? [daily[Math.max(0, cur - k + 1)].d, daily[cur].d] : null
  return (
    <SimShell title="Window: kayan pencere ortalaması ve bölge başına ilk N" sampleN={clean && trInt(clean.length)} steps={daily.length || 1} step={step} setStep={setStep} live={live} setLive={setLive}
      stepLabel={daily[cur] && `${daily[cur].d} satırı`}
      params={<>
        <Range label="Pencere uzunluğu (gün)" value={k} min={3} max={14} onChange={setK} />
        <Range label="Bölge başına ilk N yolculuk" value={n} min={1} max={5} onChange={setN} />
      </>}
      notebookView={<EChart height={340} option={opt(full.map((r) => r.pickup_date), full.map((r) => r.trip_count), full.map((r) => r.trailing_7d_avg))} />}
      liveView={daily.length > 0 && <>
        <div className="mb-3 grid grid-cols-3 gap-3">
          <Stat label="Satır" value={daily[cur].d} />
          <Stat label={`Son ${k} günün ortalaması`} value={daily[cur].avg == null ? 'pencere dolmadı' : trDec(daily[cur].avg, 1)} accent="text-taxi" />
          <Stat label={`row_number() ≤ ${n} sonucu`} value={`${trInt(top.length)} satır`} accent="text-signal" />
        </div>
        <EChart height={320} option={opt(daily.map((x) => x.d), daily.map((x) => x.v), daily.map((x, i) => (i <= cur ? x.avg : null)), win)} />
      </>}
      explain="rowsBetween(-(k−1), 0): pencere tam dolana kadar ortalama boş kalır. Örneklemde günde ~33 biniş düştüğü için eğri tam verideki kadar düzgün değildir; yöntem aynıdır (Python ile doğrulandı)." />
  )
}

// ------------------------------------------------------------------ 4) Bölge haritası
export function ZoneMapSim() {
  const clean = useSample('clean')
  const [live, setLive] = useState(false); const [step, setStep] = useState(23)
  const [pay, setPay] = useState('all')
  const counts = useMemo(() => {
    const m = new Map()
    clean?.forEach((r) => { if (r.pickup_hour <= step && (pay === 'all' || r.payment_type === Number(pay))) m.set(r.PULocationID, (m.get(r.PULocationID) || 0) + 1) })
    return m
  }, [clean, step, pay])
  const bj = useMemo(() => clean && boroughJoin(clean.filter((r) => r.pickup_hour <= step && (pay === 'all' || r.payment_type === Number(pay))), zones), [clean, step, pay])
  return (
    <SimShell title="Bölge yoğunluğu haritası (join: bölge sözlüğü)" sampleN={clean && trInt(clean.length)} steps={24} step={step} setStep={setStep} live={live} setLive={(v) => { setLive(v); if (!v) setStep(23) }}
      stepLabel={`00.00–${step}.59 arası binişler`}
      params={<Select label="Ödeme türü" value={pay} onChange={setPay} options={[['all', 'Tümü'], ['1', 'Kredi kartı (1)'], ['0', 'Flex Fare (0)'], ['2', 'Nakit (2)']]} />}
      notebookView={<EChart height={560} option={zoneMapOption(fullDataZoneCounts())} />}
      liveView={<div className="grid gap-4 xl:grid-cols-[1fr_260px]">
        <EChart height={540} option={zoneMapOption(counts, { title: 'Biniş (örneklem)' })} />
        <div className="space-y-2">{bj?.map((b) => <Stat key={b.borough} label={b.borough} value={`${trInt(b.trips)} · ort. $${trDec(b.avgTotal, 2)}`} />)}</div>
      </div>}
      explain="Animasyon saatleri birer birer ekler. İlçe kutuları, sayımın 265 satırlık TLC sözlüğüyle birleştirilmesinin (broadcast join) sonucudur." />
  )
}

// ------------------------------------------------------------------ 5) Model eğitimi
export function ModelSim() {
  const clean = useSample('clean')
  const [live, setLive] = useState(false); const [step, setStep] = useState(0)
  const [cfg, setCfg] = useState({ size: 5000, nTrees: 50, maxDepth: 3, learningRate: 0.1, includeLeaky: true })
  const [out, setOut] = useState(null); const [busy, setBusy] = useState(false)
  const worker = useRef(null)
  useEffect(() => {
    worker.current = new Worker(new URL('./model.worker.js', import.meta.url), { type: 'module' })
    worker.current.onmessage = (e) => { setOut(e.data); setBusy(false) }
    return () => worker.current.terminate()
  }, [])
  useEffect(() => {
    if (!live || !clean) return
    setBusy(true)
    const t = setTimeout(() => worker.current.postMessage({ rows: clean, ...cfg }), 150)
    return () => clearTimeout(t)
  }, [live, clean, cfg])
  const set = (k) => (v) => setCfg((o) => ({ ...o, [k]: v }))
  const comp = R.s4_comparison.filter((r) => r.size_label === '17M')
  const curve = out?.curve || []
  const shownTrees = Math.max(1, Math.round(((step + 1) / 10) * curve.length))
  return (
    <SimShell title="Model eğitimi: doğrusal regresyon vs gradyan artırma" sampleN={clean && trInt(Math.min(cfg.size, clean.length))} steps={10} step={step} setStep={setStep} live={live} setLive={setLive}
      stepLabel={`${shownTrees} ağaç`}
      params={<>
        <Range label="Örneklem büyüklüğü" value={cfg.size} min={1000} max={5000} step={500} onChange={set('size')} fmt={trInt} />
        <Range label="Ağaç sayısı" value={cfg.nTrees} min={5} max={100} step={5} onChange={set('nTrees')} />
        <Range label="Derinlik" value={cfg.maxDepth} min={1} max={5} onChange={set('maxDepth')} />
        <Range label="Öğrenme oranı" value={cfg.learningRate} min={0.05} max={0.5} step={0.05} onChange={set('learningRate')} fmt={(v) => trDec(v, 2)} />
        <Toggle label="Ücret kalemleri dahil (sızıntı)" value={cfg.includeLeaky} onChange={set('includeLeaky')} />
      </>}
      notebookView={<>
        <div className="mb-2 text-sm text-ink-muted">Tam veri (17M), s4_comparison.csv:</div>
        <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">{comp.map((r) => <Stat key={r.framework + r.model} label={`${r.framework} · ${r.model}`} value={`RMSE ${trDec(r.rmse, 3)}`} accent={r.model === 'LinearRegression' ? 'text-ink' : 'text-taxi'} />)}</div>
        <p className="mt-3 text-ink-muted">Arama ile seçilen ayar (5M test): RMSE {trDec(R.s4_tuning.find((r) => r.stage === 'test_tuned').rmse, 3)} dk.</p>
      </>}
      liveView={<>
        <div className="mb-3 grid grid-cols-4 gap-3">
          <Stat label="Eğitim / test" value={out ? `${trInt(out.nTrain)} / ${trInt(out.nTest)}` : '…'} />
          <Stat label="OLS test RMSE" value={out ? trDec(out.ols.rmse, 3) : '…'} />
          <Stat label={`GBT RMSE (${shownTrees} ağaç)`} value={curve.length ? trDec(curve[shownTrees - 1], 3) : '…'} accent="text-taxi" />
          <Stat label="Hesaplama (Web Worker)" value={busy ? 'hesaplanıyor…' : out ? `${trDec(out.ms, 0)} ms` : '…'} accent="text-signal" />
        </div>
        {curve.length > 0 && <EChart height={320} option={{ backgroundColor: 'transparent', tooltip: tip, legend: { top: 0, textStyle: { color: '#a9b1c7' } }, grid: { left: 70, right: 20, top: 40, bottom: 50 },
          xAxis: { type: 'category', name: 'Ağaç', data: curve.map((_, i) => i + 1), ...ax }, yAxis: { type: 'value', name: 'Test RMSE (dk)', scale: true, ...ax },
          series: [{ name: 'GBT test RMSE', type: 'line', data: curve.map((v, i) => (i < shownTrees ? v : null)), symbol: 'none', lineStyle: { color: C.taxi, width: 3 } },
            { name: 'OLS (doğrusal)', type: 'line', data: curve.map(() => out.ols.rmse), symbol: 'none', lineStyle: { color: C.blue, type: 'dashed' } }] }} />}
      </>}
      explain="Aynı algoritmalar Python'da (scikit-learn LinearRegression ve GradientBoostingRegressor) aynı örneklemde birebir aynı RMSE'yi verdi. Örneklem küçük olduğundan RMSE tam verideki değerlerden yüksektir." />
  )
}

// ------------------------------------------------------------------ 6) Dosya düzeni ve paralellik
export function PartitionSim() {
  const clean = useSample('clean')
  const [live, setLive] = useState(false); const [step, setStep] = useState(0)
  const [rg, setRg] = useState(3); const [cores, setCores] = useState(20); const [cached, setCached] = useState(false); const [op, setOp] = useState('window')
  const tasks = cached ? cores : rg
  const waves = Math.ceil(tasks / cores)
  const used = Math.min(tasks, cores)
  const { check, ms } = useMemo(() => {
    if (!clean) return {}
    const t = performance.now()
    const parts = Array.from({ length: tasks }, (_, i) => clean.filter((_, j) => j % tasks === i))
    let ok
    if (op === 'groupby') { const a = groupCount(clean, (r) => r.pickup_hour); const b = new Map(); parts.forEach((p) => groupCount(p, (r) => r.pickup_hour).forEach((v, k) => b.set(k, (b.get(k) || 0) + v))); ok = [...a].every(([k, v]) => b.get(k) === v) }
    else if (op === 'join') { const a = boroughJoin(clean, zones); const b = new Map(); parts.forEach((p) => boroughJoin(p, zones).forEach((x) => b.set(x.borough, (b.get(x.borough) || 0) + x.trips))); ok = a.every((x) => b.get(x.borough) === x.trips) }
    else { const a = topNPerGroup(clean, 'PULocationID', 'total_amount', 3); const b = topNPerGroup(parts.flatMap((p) => topNPerGroup(p, 'PULocationID', 'total_amount', 3).map((x) => ({ PULocationID: x.group, total_amount: x.value }))), 'PULocationID', 'total_amount', 3); ok = a.length === b.length && Math.abs(a.reduce((s, x) => s + x.value, 0) - b.reduce((s, x) => s + x.value, 0)) < 1e-6 }
    return { check: ok, ms: performance.now() - t }
  }, [clean, tasks, op])
  const lay = (e, o, s) => R.s5_processing.find((r) => r.engine === e && r.op === o && r.size === s && r.format === 'parquet_snappy')?.seconds_mean
  return (
    <SimShell title="Dosya düzeni: row group → görev → çekirdek" sampleN={clean && trInt(clean.length)} steps={waves + 1} step={step} setStep={setStep} live={live} setLive={setLive}
      stepLabel={step < waves ? `${step + 1}. görev dalgası` : 'Sonuçlar birleşti'}
      params={<>
        <Range label="Parquet row group sayısı" value={rg} min={1} max={40} onChange={setRg} />
        <Range label="Çekirdek (iş parçacığı)" value={cores} min={1} max={20} onChange={setCores} />
        <Toggle label="Önbellek + 20 bölüme dağıt (spread_partitions)" value={cached} onChange={setCached} />
        <Select label="İşlem" value={op} onChange={setOp} options={[['groupby', 'groupby (saat)'], ['join', 'join (ilçe)'], ['window', 'window (bölge başına ilk 3)']]} />
      </>}
      notebookView={<>
        <div className="mb-2 text-sm text-ink-muted">Tam veri, 19M Parquet (s5_processing.csv):</div>
        <div className="grid grid-cols-3 gap-3">
          <Stat label="Spark window · 3 row group" value={`${trDec(lay('spark', 'window_top3', 'full'), 2)} sn`} />
          <Stat label="Spark window · 23 row group" value={`${trDec(lay('spark', 'window_top3', 'full_rg16'), 2)} sn`} accent="text-taxi" />
          <Stat label="pandas window" value={`${trDec(lay('pandas', 'window_top3', 'full'), 2)} sn`} accent="text-warn" />
        </div>
      </>}
      liveView={<>
        <div className="mb-3 grid grid-cols-4 gap-3">
          <Stat label="Görev (bölüm)" value={tasks} />
          <Stat label="Kullanılan çekirdek" value={`${used} / ${cores}`} accent={used < cores ? 'text-warn' : 'text-ok'} />
          <Stat label="Görev dalgası" value={waves} />
          <Stat label="Bölümlü sonuç = tek parça?" value={check ? `✔ (${trDec(ms, 0)} ms)` : '✘'} accent={check ? 'text-ok' : 'text-warn'} />
        </div>
        <div className="grid gap-1" style={{ gridTemplateColumns: `repeat(${cores}, minmax(0, 1fr))` }}>
          {Array.from({ length: cores }, (_, c) => (
            <div key={c} className="flex flex-col gap-1">
              <div className="text-center text-[0.65rem] text-ink-faint">Ç{c + 1}</div>
              {Array.from({ length: waves }, (_, w) => {
                const id = w * cores + c
                const has = id < tasks
                const active = has && w <= step
                return <div key={w} className={`h-8 rounded transition-colors duration-500 ${!has ? 'bg-night-800/40' : active ? (w === step && step < waves ? 'bg-taxi' : 'bg-signal/70') : 'bg-night-700'}`} title={has ? `Görev ${id + 1}` : 'boşta'} />
              })}
            </div>
          ))}
        </div>
      </>}
      explain="Spark bir row group'u bölemez: tek dosya + 3 row group = 3 görev, 20 çekirdeğin 17'si boşta. Önbellek + yeniden dağıtım görev sayısını çekirdek sayısına çıkarır. Sonuç her durumda aynı kalır (örneklemde kontrol edilir)." />
  )
}

export const SIMS = { cleaning: CleaningSim, aggregation: AggregationSim, window: WindowSim, zonemap: ZoneMapSim, model: ModelSim, partition: PartitionSim }
