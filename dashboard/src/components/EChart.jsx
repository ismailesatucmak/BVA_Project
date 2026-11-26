import { useMemo, useState } from 'react'
import * as echarts from 'echarts'
import ReactECharts from 'echarts-for-react/lib/core'
import geo from '../data/geo/taxi_zones.json'
import { results } from '../lib/data.js'
import { trInt } from '../lib/format.js'
import { Card, ImageView } from './cards.jsx'

let mapReady = false
export function ensureMap() {
  if (!mapReady) { echarts.registerMap('nyc-taxi-zones', geo); mapReady = true }
}

export function EChart({ option, height = 400 }) {
  return <ReactECharts echarts={echarts} option={option} style={{ height, width: '100%' }} notMerge lazyUpdate opts={{ renderer: 'canvas' }} />
}

/** Bölge haritası (EPSG:2263 feet koordinatlarıyla düzlemsel çizim). counts: Map(LocationID → değer). */
export function zoneMapOption(counts, { title = 'Biniş', max } = {}) {
  ensureMap()
  const names = new Map(geo.features.map((f) => [f.id, f.properties]))
  const data = geo.features.map((f) => ({ name: f.properties.zone, value: counts.get(f.id) ?? 0, id: f.id }))
  const vmax = max ?? Math.max(1, ...data.map((d) => d.value))
  return {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'item', backgroundColor: '#141b31', borderColor: '#2a3558', textStyle: { color: '#e8ebf4' },
      formatter: (p) => { const z = [...names.values()].find((x) => x.zone === p.name); return `${p.name}${z ? ` · ${z.borough} (${z.LocationID})` : ''}<br><b>${trInt(p.value || 0)}</b> ${title.toLowerCase()}` } },
    visualMap: { min: 0, max: vmax, calculable: true, left: 10, bottom: 10, text: ['çok', 'az'], textStyle: { color: '#a9b1c7' }, inRange: { color: ['#141b31', '#1f4e8c', '#338ced', '#ffc629'] } },
    series: [{ type: 'map', map: 'nyc-taxi-zones', nameProperty: 'zone', roam: true, aspectScale: 1, scaleLimit: { min: 0.8, max: 12 },
      itemStyle: { borderColor: '#0b1020', borderWidth: 0.5, areaColor: '#141b31' }, emphasis: { label: { show: false }, itemStyle: { areaColor: '#ffc629' } }, data }],
  }
}

export function fullDataZoneCounts() {
  return new Map(results.s3_zone.map((r) => [r.PULocationID, r.trip_count]))
}

/** Yeniden çizilmiş grafik + "Orijinal çıktıyı göster" seçeneği. */
export function ChartCard({ chart, image, title, ann, where }) {
  const [original, setOriginal] = useState(false)
  const option = useMemo(() => (chart.map ? zoneMapOption(fullDataZoneCounts()) : chart.option()), [chart])
  return (
    <Card title={title} badge={`Veri: reports/results/${chart.source} · ${where}`} ann={ann}>
      <div className="mb-2 flex items-center justify-between gap-3">
        <span className={`rounded-full px-3 py-1 text-xs font-semibold ${original ? 'bg-night-700 text-ink-muted' : 'bg-signal/15 text-signal'}`}>
          {original ? '🖼 Notebook\'taki orijinal görsel' : '📊 İnteraktif yeniden çizim (tam veri sonuçları)'}
        </span>
        <button type="button" onClick={() => setOriginal((v) => !v)} className="rounded-lg border border-night-600 px-3 py-1 text-sm hover:border-taxi">
          {original ? 'İnteraktif grafiğe dön' : 'Orijinal çıktıyı göster'}
        </button>
      </div>
      {original ? <ImageView src={image} alt={title} /> : <EChart option={option} height={chart.map ? 620 : chart.height || 420} />}
      {option.note && !original && <p className="mt-2 text-sm text-ink-faint">{option.note}</p>}
    </Card>
  )
}
