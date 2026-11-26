// Tüm veriler build'e gömülü JSON'lardan gelir (scripts/extract_*.py üretir). Uydurma sayı yok.
const nbModules = import.meta.glob('../data/notebooks/0*.json', { eager: true, import: 'default' })
const resultModules = import.meta.glob('../data/results/*.json', { eager: true, import: 'default' })

export const notebooks = Object.fromEntries(
  Object.values(nbModules).map((nb) => [nb.slug, nb]),
)

/** results/<name>.json → satır nesneleri dizisi */
export const results = Object.fromEntries(
  Object.entries(resultModules).map(([path, doc]) => {
    const name = path.split('/').pop().replace('.json', '')
    const rows = doc.rows.map((r) => Object.fromEntries(doc.columns.map((c, i) => [c, r[i]])))
    return [name, Object.assign(rows, { source: doc.source, columns: doc.columns })]
  }),
)

export function imageUrl(rel) {
  return `${import.meta.env.BASE_URL}${rel}`
}

/** Örneklem JSON'u (columnar) → satır nesneleri. Ağır olduğundan isteğe bağlı yüklenir. */
export async function loadSample(kind) {
  const mod = kind === 'raw'
    ? await import('../data/samples/raw_sample.json')
    : await import('../data/samples/clean_sample.json')
  return toRows(mod.default)
}

export function toRows(doc) {
  const { columns, data, rows } = doc
  const out = new Array(rows)
  for (let i = 0; i < rows; i += 1) {
    const r = {}
    for (const c of columns) r[c] = data[c][i]
    out[i] = r
  }
  return Object.assign(out, { population: doc.population, source: doc.source })
}
