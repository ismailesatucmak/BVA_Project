// Türkçe sayı biçimi. Değer değiştirilmez; yalnızca gösterim için ondalık basamak seçilir.
const cache = new Map()
function fmt(digits) {
  if (!cache.has(digits)) {
    cache.set(digits, new Intl.NumberFormat('tr-TR', { minimumFractionDigits: digits, maximumFractionDigits: digits }))
  }
  return cache.get(digits)
}
export const trInt = (v) => (v == null || Number.isNaN(v) ? '—' : fmt(0).format(Math.round(v)))
export const trDec = (v, d = 2) => (v == null || Number.isNaN(v) ? '—' : fmt(d).format(v))
export const trAuto = (v) => {
  if (v == null || Number.isNaN(v)) return '—'
  if (Number.isInteger(v)) return trInt(v)
  const a = Math.abs(v)
  return trDec(v, a >= 100 ? 1 : a >= 1 ? 2 : 3)
}
export const sizeLabel = (rows) => (rows >= 1e6 ? `${trDec(rows / 1e6, rows >= 1e7 ? 0 : 1)}M` : `${trInt(rows / 1e3)}K`)
