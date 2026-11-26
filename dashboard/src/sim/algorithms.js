// Simülasyon algoritmaları — saf JS, tarayıcıda (Web Worker) ve Node'da (scripts/verify_sim.mjs) aynı kod.
// Python karşılıkları: scripts/sim_reference.py. Eşikler src/config.py ile aynı varsayılanlarla gelir.

export const CLEAN_DEFAULTS = Object.freeze({
  maxPassengers: 6, maxMiles: 100, minDuration: 1, maxDuration: 180, maxSpeed: 80,
  periodStart: '2026-01-01 00:00:00', periodEnd: '2026-06-01 00:00:00', unknownRatecode: 99,
  unknownLocations: [264, 265],
})

/** quality.logic_checks() + clean.RULES karşılığı. Null asla "hatalı" sayılmaz. */
export const RULES = [
  ['R1', 'passenger_count 0 veya > üst sınır', (r, p) => r.passenger_count != null && (r.passenger_count === 0 || r.passenger_count > p.maxPassengers)],
  ['R2', 'trip_distance ≤ 0 veya > üst sınır', (r, p) => r.trip_distance <= 0 || r.trip_distance > p.maxMiles],
  ['R3', 'fare_amount ≤ 0 veya total_amount ≤ 0', (r) => r.fare_amount <= 0 || r.total_amount <= 0],
  ['R4', 'süre ≤ alt sınır, > üst sınır veya negatif', (r, p) => r.trip_duration_min < 0 || (r.trip_duration_min >= 0 && r.trip_duration_min <= p.minDuration) || r.trip_duration_min > p.maxDuration],
  ['R5', 'ortalama hız > üst sınır', (r, p) => r.avg_speed_mph != null && r.avg_speed_mph > p.maxSpeed],
  ['R6', 'pickup dönem dışında', (r, p) => r.tpep_pickup_datetime < p.periodStart || r.tpep_pickup_datetime >= p.periodEnd],
  ['R7', 'RatecodeID = 99', (r, p) => r.RatecodeID === p.unknownRatecode],
  ['R8', 'PU/DO LocationID 264 veya 265', (r, p) => p.unknownLocations.includes(r.PULocationID) || p.unknownLocations.includes(r.DOLocationID)],
  ['R10', 'cbd_congestion_fee < 0', (r) => r.cbd_congestion_fee != null && r.cbd_congestion_fee < 0],
]

/** Her kural için tek başına / sıralı / yalnız bu kural sayıları (clean.rule_impact ile aynı tanım). */
export function cleaningImpact(rows, params = CLEAN_DEFAULTS, { includeFlex = true } = {}) {
  const p = { ...CLEAN_DEFAULTS, ...params }
  const data = includeFlex ? rows : rows.filter((r) => r.payment_type !== 0)
  const flags = data.map((r) => RULES.map(([, , fn]) => Boolean(fn(r, p))))
  const out = RULES.map(([id, desc], k) => {
    let isolated = 0; let sequential = 0; let unique = 0
    for (const f of flags) {
      if (!f[k]) continue
      isolated += 1
      if (!f.slice(0, k).some(Boolean)) sequential += 1
      if (!f.some((v, j) => v && j !== k)) unique += 1
    }
    return { rule: id, description: desc, isolated, sequential, unique }
  })
  const removed = flags.filter((f) => f.some(Boolean)).length
  return { rows: data.length, removed, kept: data.length - removed, rules: out }
}

/** Satırları round-robin P bölüme dağıtır (Spark'ın görevlere bölmesinin basit modeli). */
export function partition(rows, p) {
  const parts = Array.from({ length: p }, () => [])
  rows.forEach((r, i) => parts[i % p].push(r))
  return parts
}

/** groupBy(key).count() — map (bölüm başına kısmi sayım) → shuffle → reduce. Adımlar animasyon içindir. */
export function partitionedGroupCount(rows, keyFn, p) {
  const parts = partition(rows, p)
  const partials = parts.map((part) => {
    const m = new Map()
    for (const r of part) { const k = keyFn(r); m.set(k, (m.get(k) || 0) + 1) }
    return m
  })
  const reducers = Array.from({ length: p }, () => new Map())
  for (const m of partials) {
    for (const [k, v] of m) {
      const target = reducers[hashKey(k) % p]
      target.set(k, (target.get(k) || 0) + v)
    }
  }
  const final = new Map()
  for (const m of reducers) for (const [k, v] of m) final.set(k, v)
  return { parts: parts.map((x) => x.length), partials, reducers, final }
}

export function groupCount(rows, keyFn) {
  const m = new Map()
  for (const r of rows) { const k = keyFn(r); m.set(k, (m.get(k) || 0) + 1) }
  return m
}

function hashKey(k) {
  const s = String(k); let h = 0
  for (let i = 0; i < s.length; i += 1) h = (h * 31 + s.charCodeAt(i)) >>> 0
  return h
}

/** Günlük sayım + 7 günlük hareketli ortalama (tam pencere) + lag(7) — s3 daily_window_metrics karşılığı. */
export function dailyWindow(rows) {
  const counts = groupCount(rows, (r) => r.tpep_pickup_datetime.slice(0, 10))
  const days = [...counts.keys()].sort()
  return days.map((d, i) => {
    const win = days.slice(Math.max(0, i - 6), i + 1)
    return {
      date: d,
      trips: counts.get(d),
      prevWeek: i >= 7 ? counts.get(days[i - 7]) : null,
      avg7: win.length === 7 ? win.reduce((s, x) => s + counts.get(x), 0) / 7 : null,
    }
  })
}

/** row_number() over (partition by group order by value desc) <= n */
export function topNPerGroup(rows, groupKey, valueKey, n) {
  const groups = new Map()
  for (const r of rows) {
    const g = r[groupKey]
    if (!groups.has(g)) groups.set(g, [])
    groups.get(g).push(r)
  }
  const out = []
  for (const [g, list] of groups) {
    list.sort((a, b) => b[valueKey] - a[valueKey])
    list.slice(0, n).forEach((r, i) => out.push({ group: g, rank: i + 1, value: r[valueKey] }))
  }
  return out
}

/** Biniş bölgesi sayımı ⋈ bölge sözlüğü (broadcast) → ilçe başına yolculuk ve ortalama tutar. */
export function boroughJoin(rows, zones) {
  const lookup = new Map(zones.map((z) => [z.LocationID, z.Borough]))
  const agg = new Map()
  for (const r of rows) {
    const b = lookup.get(r.PULocationID)
    if (b === undefined) continue
    const a = agg.get(b) || { borough: b, trips: 0, total: 0 }
    a.trips += 1; a.total += r.total_amount
    agg.set(b, a)
  }
  return [...agg.values()].map((a) => ({ borough: a.borough, trips: a.trips, avgTotal: a.total / a.trips }))
    .sort((x, y) => y.trips - x.trips)
}

// ------------------------------------------------------------------ modeller
export const MODEL_FEATURES = ['trip_distance', 'pickup_hour', 'pickup_dow', 'PULocationID', 'DOLocationID',
  'extra', 'mta_tax', 'tolls_amount', 'improvement_surcharge', 'cbd_congestion_fee']
export const LEAKY = ['tolls_amount', 'cbd_congestion_fee']

export function features(includeLeaky) {
  return MODEL_FEATURES.filter((f) => includeLeaky || !LEAKY.includes(f))
}

export function metrics(y, pred) {
  const n = y.length
  let se = 0; let ae = 0; let mean = 0
  for (let i = 0; i < n; i += 1) mean += y[i]
  mean /= n
  let ss = 0
  for (let i = 0; i < n; i += 1) {
    const e = pred[i] - y[i]
    se += e * e; ae += Math.abs(e); ss += (y[i] - mean) ** 2
  }
  return { rmse: Math.sqrt(se / n), mae: ae / n, r2: 1 - se / ss }
}

/** Sıradan en küçük kareler (kesişim dahil), normal denklemler + Gauss eleme (kısmi pivot). */
export function fitOLS(X, y) {
  const d = X[0].length + 1
  const A = Array.from({ length: d }, () => new Float64Array(d + 1))
  for (let i = 0; i < X.length; i += 1) {
    const row = [1, ...X[i]]
    for (let a = 0; a < d; a += 1) {
      A[a][d] += row[a] * y[i]
      for (let b = 0; b < d; b += 1) A[a][b] += row[a] * row[b]
    }
  }
  for (let c = 0; c < d; c += 1) {
    let piv = c
    for (let r = c + 1; r < d; r += 1) if (Math.abs(A[r][c]) > Math.abs(A[piv][c])) piv = r;
    [A[c], A[piv]] = [A[piv], A[c]]
    const pv = A[c][c]
    if (Math.abs(pv) < 1e-12) continue
    for (let r = 0; r < d; r += 1) {
      if (r === c) continue
      const f = A[r][c] / pv
      for (let k = c; k <= d; k += 1) A[r][k] -= f * A[c][k]
    }
  }
  const coef = Array.from({ length: d }, (_, i) => (Math.abs(A[i][i]) < 1e-12 ? 0 : A[i][d] / A[i][i]))
  return { intercept: coef[0], coef: coef.slice(1), predict: (x) => coef[0] + x.reduce((s, v, j) => s + v * coef[j + 1], 0) }
}

/** Gradyan artırma (kare hata), tam arama ile bölme — sklearn GradientBoostingRegressor(criterion='squared_error') karşılığı. */
export function fitGBT(X, y, { nTrees = 50, maxDepth = 3, learningRate = 0.1, minLeaf = 20, onTree } = {}) {
  const n = X.length; const m = X[0].length
  const sortedIdx = Array.from({ length: m }, (_, j) => Int32Array.from({ length: n }, (_, i) => i).sort((a, b) => X[a][j] - X[b][j]))
  const init = y.reduce((s, v) => s + v, 0) / n
  const pred = new Float64Array(n).fill(init)
  const trees = []
  const resid = new Float64Array(n)
  for (let t = 0; t < nTrees; t += 1) {
    for (let i = 0; i < n; i += 1) resid[i] = y[i] - pred[i]
    const member = new Int32Array(n).fill(0)
    const tree = buildTree(X, resid, sortedIdx, member, 0, maxDepth, minLeaf, { next: 1 })
    trees.push(tree)
    for (let i = 0; i < n; i += 1) pred[i] += learningRate * evalTree(tree, X[i])
    if (onTree) onTree(t, pred)
  }
  return { init, trees, learningRate, predict: (x) => trees.reduce((s, tr) => s + learningRate * evalTree(tr, x), init) }
}

function buildTree(X, r, sortedIdx, member, node, depth, minLeaf, counter) {
  let n = 0; let sum = 0
  for (let i = 0; i < member.length; i += 1) if (member[i] === node) { n += 1; sum += r[i] }
  const leaf = { value: n ? sum / n : 0 }
  if (depth === 0 || n < 2 * minLeaf) return leaf
  let best = null
  for (let j = 0; j < sortedIdx.length; j += 1) {
    let ln = 0; let ls = 0
    const idx = sortedIdx[j]
    let prev = null
    for (let k = 0; k < idx.length; k += 1) {
      const i = idx[k]
      if (member[i] !== node) continue
      const v = X[i][j]
      if (prev !== null && v !== prev && ln >= minLeaf && n - ln >= minLeaf) {
        const rs = sum - ls
        const gain = (ls * ls) / ln + (rs * rs) / (n - ln)
        if (best === null || gain > best.gain + 1e-12) best = { gain, feature: j, threshold: (prev + v) / 2 }
      }
      ln += 1; ls += r[i]; prev = v
    }
  }
  if (!best || best.gain <= (sum * sum) / n + 1e-12) return leaf
  const left = counter.next; const right = counter.next + 1
  counter.next += 2
  for (let i = 0; i < member.length; i += 1) {
    if (member[i] === node) member[i] = X[i][best.feature] <= best.threshold ? left : right
  }
  return {
    feature: best.feature, threshold: best.threshold,
    left: buildTree(X, r, sortedIdx, member, left, depth - 1, minLeaf, counter),
    right: buildTree(X, r, sortedIdx, member, right, depth - 1, minLeaf, counter),
  }
}

function evalTree(node, x) {
  let cur = node
  while (cur.feature !== undefined) cur = x[cur.feature] <= cur.threshold ? cur.left : cur.right
  return cur.value
}

/** Örneklemi model girdisine çevirir: train/test (örneklemdeki is_train bayrağı), boyut kırpma. */
export function modelData(rows, { size = rows.length, includeLeaky = true } = {}) {
  const feats = features(includeLeaky)
  const use = rows.slice(0, size)
  const train = use.filter((r) => r.is_train); const test = use.filter((r) => !r.is_train)
  const mat = (set) => set.map((r) => feats.map((f) => r[f] ?? 0))
  return { feats, Xtr: mat(train), ytr: train.map((r) => r.trip_duration_min), Xte: mat(test), yte: test.map((r) => r.trip_duration_min) }
}
