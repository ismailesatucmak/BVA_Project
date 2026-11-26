// JS simülasyon algoritmalarını Python referansıyla karşılaştırır (aynı örneklem, aynı parametreler).
// Çalıştırma: python scripts/sim_reference.py  →  npm run verify:sim
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import {
  cleaningImpact, groupCount, partitionedGroupCount, dailyWindow, topNPerGroup, boroughJoin,
  fitOLS, fitGBT, metrics, modelData,
} from '../src/sim/algorithms.js'

const here = (p) => fileURLToPath(new URL(p, import.meta.url))
const json = (p) => JSON.parse(readFileSync(here(p), 'utf-8'))
const ref = json('./sim_reference.json')
const rows = (doc) => Array.from({ length: doc.rows }, (_, i) => Object.fromEntries(doc.columns.map((c) => [c, doc.data[c][i]])))
const raw = rows(json('../src/data/samples/raw_sample.json'))
const clean = rows(json('../src/data/samples/clean_sample.json'))
const zones = json('../src/data/zones.json')

let failed = 0
const check = (name, ok, detail = '') => {
  console.log(`${ok ? '✔' : '✘'} ${name}${detail ? `  (${detail})` : ''}`)
  if (!ok) failed += 1
}
const near = (a, b, rel = 1e-9) => Math.abs(a - b) <= rel * Math.max(1, Math.abs(b))

for (const c of ref.cleaning) {
  const js = cleaningImpact(raw, c.params, { includeFlex: c.includeFlex })
  const same = js.rows === c.rows && js.removed === c.removed
    && c.rules.every((r, i) => ['isolated', 'sequential', 'unique'].every((k) => js.rules[i][k] === r[k]))
  check(`temizlik kuralları — ${c.name}`, same, `JS silinen ${js.removed} / Python ${c.removed}`)
}

const hourly = groupCount(clean, (r) => r.pickup_hour)
check('saat groupBy', Object.entries(ref.hourly).every(([h, v]) => hourly.get(Number(h)) === v))
for (const p of [1, 4, 8, 20]) {
  const part = partitionedGroupCount(clean, (r) => r.pickup_hour, p)
  check(`bölümlü groupBy = tek parça (P=${p})`, [...hourly].every(([k, v]) => part.final.get(k) === v))
}
const dw = dailyWindow(clean)
check('günlük sayım + 7 günlük pencere', dw.length === ref.daily.length
  && dw.every((d, i) => d.date === ref.daily[i].date && d.trips === ref.daily[i].trips
    && (d.avg7 === null ? ref.daily[i].avg7 === null : near(d.avg7, ref.daily[i].avg7))))
const top = topNPerGroup(clean, 'PULocationID', 'total_amount', 3)
check('window: bölge başına ilk 3', top.length === ref.top3.rows
  && near(top.reduce((s, r) => s + r.value, 0), ref.top3.sum, 1e-9), `${top.length} satır`)
const bj = boroughJoin(clean, zones)
check('join: ilçe başına yolculuk ve ortalama tutar', bj.length === Object.keys(ref.borough).length
  && bj.every((b) => ref.borough[b.borough].trips === b.trips && near(b.avgTotal, ref.borough[b.borough].avgTotal)))

let md = modelData(clean, { includeLeaky: true })
const ols = fitOLS(md.Xtr, md.ytr)
const olsM = metrics(md.yte, md.Xte.map(ols.predict))
check('OLS test RMSE', near(olsM.rmse, ref.ols.rmse, 1e-6), `JS ${olsM.rmse.toFixed(4)} / Python ${ref.ols.rmse.toFixed(4)}`)
check('OLS katsayıları', ols.coef.every((c, i) => near(c, ref.ols.coef[i], 1e-5)))

for (const g of ref.gbt) {
  md = modelData(clean, { includeLeaky: g.includeLeaky })
  const t0 = performance.now()
  const m = fitGBT(md.Xtr, md.ytr, g)
  const ms = performance.now() - t0
  const r = metrics(md.yte, md.Xte.map(m.predict))
  const diff = Math.abs(r.rmse - g.rmse) / g.rmse
  check(`GBT (${g.nTrees} ağaç, derinlik ${g.maxDepth}) test RMSE ±%2`, diff < 0.02,
    `JS ${r.rmse.toFixed(4)} / sklearn ${g.rmse.toFixed(4)} · fark %${(100 * diff).toFixed(2)} · ${ms.toFixed(0)} ms`)
}

console.log(failed ? `\n${failed} kontrol başarısız` : '\nTüm kontroller geçti')
process.exit(failed ? 1 : 0)
