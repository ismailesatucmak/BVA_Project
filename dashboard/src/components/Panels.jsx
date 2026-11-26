import { useMemo } from 'react'
import { motion } from 'framer-motion'
import { notebooks, results as R } from '../lib/data.js'
import { trDec, trInt } from '../lib/format.js'
import { CHARTS } from '../content/charts.js'
import { MEMBERS, STUDENTS } from '../content/team.js'
import { SIMS } from '../sims/Sims.jsx'
import { Card, CodeCard, DataTable } from './cards.jsx'
import { ChartCard, EChart } from './EChart.jsx'

const IMG = ['image/png', 'image/svg+xml', 'image/jpeg']

/** Notebook'taki orijinal görselin yolu ("Orijinal çıktıyı göster" için). */
function originalImage(key) {
  const [slug, c, o] = key.split(':')
  const data = notebooks[slug].cells[Number(c)].outputs[Number(o)].data || {}
  return IMG.map((m) => data[m]).find(Boolean)
}

function EChartCard({ item }) {
  const option = useMemo(() => item.option(), [item])
  return <Card title={item.title} badge={`Veri: ${item.source}`} ann={item.ann}><EChart option={option} height={item.height || 420} /></Card>
}

/** Kritik kod + aynı kodun gerçek çıktısı yan yana. */
function CodeWithOutput({ item }) {
  const out = item.output
  return (
    <div className="my-6 grid gap-4 xl:grid-cols-[3fr_2fr]">
      <div className="min-w-0 [&>div]:my-0"><CodeCard source={item.code} where={item.where} /></div>
      {out && (
        <div className="min-w-0 rounded-xl border border-ok/40 bg-night-900/70 p-4">
          <div className="mb-2 flex items-center justify-between gap-2 text-sm">
            <span className="font-semibold text-ok">▶ Gerçek çıktı</span>
            <span className="text-xs text-ink-faint">{out.source}</span>
          </div>
          {out.table && <DataTable table={out.table()} />}
          {out.text && <pre className="mt-2 whitespace-pre-wrap font-mono text-sm text-ink">{out.text}</pre>}
        </div>
      )}
    </div>
  )
}

function Item({ item }) {
  if (item.type === 'chart') {
    const [slug, c] = item.key.split(':')
    return <ChartCard chart={item.chart || CHARTS[item.key]} image={originalImage(item.key)} title={item.title} ann={item.ann} where={`${slug} · hücre ${c}`} />
  }
  if (item.type === 'echart') return <EChartCard item={item} />
  if (item.type === 'table') {
    return <Card title={item.title} badge={`Veri: ${item.source}`} ann={item.ann}><DataTable table={item.table()} />{item.note && <p className="mt-2 text-sm text-ink-faint">{item.note}</p>}</Card>
  }
  if (item.type === 'code') return <CodeWithOutput item={item} />
  if (item.type === 'stats') {
    return (
      <div className="my-6">
        <div className="mb-2 flex justify-between gap-3 text-sm"><span className="font-semibold">{item.title}</span><span className="text-xs text-ink-faint">Veri: {item.source}</span></div>
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {item.stats().map(([dim, v, sub]) => (
            <div key={dim} className="rounded-2xl border border-night-700 bg-night-850 p-4">
              <div className="text-sm text-signal">{dim}</div>
              <div className="text-xl font-extrabold text-taxi">{v}</div>
              <div className="text-sm text-ink-muted num">{sub}</div>
            </div>
          ))}
        </div>
      </div>
    )
  }
  if (item.type === 'sim') {
    const S = SIMS[item.id]
    return (
      <div>
        {item.tag && <div className="-mb-5 mt-8 inline-block rounded-t-xl border-2 border-b-0 border-signal/60 bg-signal/10 px-4 py-1.5 text-sm font-semibold text-signal">{item.tag}</div>}
        <S />
      </div>
    )
  }
  return null
}

function TeamList({ large = false }) {
  return (
    <div className={`grid gap-3 ${large ? 'md:grid-cols-2' : 'sm:grid-cols-2 xl:grid-cols-4'}`}>
      {MEMBERS.map((m) => (
        <div key={m.no} className="rounded-2xl border border-night-700 bg-night-850 p-4">
          <div className={`${large ? 'text-2xl' : 'text-lg'} font-bold`}>{m.name}</div>
          <div className="text-ink-muted num">{m.no}</div>
        </div>
      ))}
    </div>
  )
}

const box = 'rounded-2xl border border-night-700 bg-night-850 p-5'
const label = 'text-sm font-semibold text-signal'

function BigDataCard() {
  const full = (f) => R.size_sets_manifest.find((r) => r.size === 'full' && r.format === f)
  const x3 = R.s5_synthetic_manifest.find((r) => r.format === 'csv')
  const types = {}
  R.dictionary_stats.forEach((r) => { types[r.spark_type] = (types[r.spark_type] || 0) + 1 })
  const months = {}
  R.s3_day.forEach((r) => { const m = String(r.pickup_date).slice(0, 7); months[m] = (months[m] || 0) + r.trip_count })
  const MONTH = { '01': 'Oca', '02': 'Şub', '03': 'Mar', '04': 'Nis', '05': 'May' }
  const pandasCsv = R.io_benchmark.find((r) => r.engine === 'pandas' && r.format === 'csv' && r.size === 'full' && r.mode === 'all')
  return (
    <div className={box}>
      <div className="text-xl font-bold">Neden Big Data?</div>
      <div className="mt-3 grid gap-4 lg:grid-cols-3">
        <div>
          <div className={label}>Volume (hacim)</div>
          <div className="mt-1 text-2xl font-extrabold text-taxi num">{trInt(full('csv').rows)} kayıt</div>
          <div className="text-ink-muted num">CSV {trInt(full('csv').mb)} MB · Parquet zstd {trInt(full('parquet_zstd').mb)} MB</div>
          <div className="text-ink-muted num">Sentetik ×3: {trInt(x3.rows)} kayıt, CSV {trInt(x3.mb)} MB</div>
        </div>
        <div>
          <div className={label}>Variety (çeşitlilik)</div>
          <div className="mt-1 text-2xl font-extrabold text-taxi">20 kolon</div>
          <div className="text-ink-muted">{Object.entries(types).map(([t, n]) => `${n} ${t}`).join(' · ')}</div>
          <div className="text-ink-muted">+ 265 satırlık bölge sözlüğü ve bölge geometrisi</div>
        </div>
        <div>
          <div className={label}>Velocity (hız)</div>
          <div className="mt-1 text-2xl font-extrabold text-taxi num">~{trDec(Math.min(...Object.values(months)) / 1e6, 1)}–{trDec(Math.max(...Object.values(months)) / 1e6, 1)}M / ay</div>
          <div className="text-ink-muted num">{Object.entries(months).sort().map(([m, n]) => `${MONTH[m.slice(5)]} ${trDec(n / 1e6, 2)}M`).join(' · ')}</div>
          <div className="text-xs text-ink-faint">temiz kayıt, s3_day.csv günlük sayımlarının aylık toplamı</div>
        </div>
      </div>
      <p className="mt-4 border-t border-night-700 pt-3 text-ink"><strong className="text-taxi">Tek makinede sınır:</strong> 19M kaydın 1,9 GB'lık CSV'si pandas ile <span className="num">{trDec(pandasCsv.seconds_mean, 0)} sn</span>'de okunuyor ve bellekte <span className="num">{trDec(pandasCsv.peak_mb / 1000, 1)} GB</span> tutuyor; veri büyüdükçe süre ve RAM birlikte tükeniyor.</p>
    </div>
  )
}

const LAYERS = [
  { layer: 'Veri kaynağı', boxes: [['NYC TLC Yellow Taxi', '5 aylık Parquet + bölge sözlüğü', 0]], arrow: '19,0M kayıt' },
  { layer: 'Depolama', boxes: [['CSV / Parquet', '5 boyut × 4 format, 100K–19M (+57M sentetik)', 0]], arrow: '19,0M kayıt' },
  { layer: 'İşleme (PySpark)', boxes: [['Temizlik + anomali', '10 kural, Spark window', 1], ['Aggregation', 'groupBy, window, join', 2]], arrow: '17,2M temiz kayıt' },
  { layer: 'Analiz / ML', boxes: [['ML: süre tahmini', 'MLlib vs scikit-learn, 100K–17M', 3], ['Performans deneyi', 'pandas vs Spark × CSV vs Parquet', 4]], arrow: 'sonuç CSV\'leri' },
  { layer: 'Sunum', boxes: [['Dashboard', 'React + ECharts, çevrimdışı', 4]] },
]

function Architecture() {
  return (
    <div className="flex items-stretch gap-1" role="list" aria-label="Katmanlı mimari">
      {LAYERS.map((L, i) => (
        <div key={L.layer} className="flex min-w-0 flex-1 items-stretch gap-1" role="listitem">
          <div className="flex min-w-0 flex-1 flex-col rounded-xl border border-night-600 bg-night-900/60 p-2">
            <div className="mb-2 text-center text-xs font-bold text-signal">{i + 1}. {L.layer}</div>
            <div className="flex flex-1 flex-col justify-center gap-2">
              {L.boxes.map(([t, d, who]) => (
                <div key={t} className="rounded-lg border border-taxi/50 bg-night-850 p-2">
                  <div className="break-words text-[0.9rem] font-bold leading-tight">{t}</div>
                  <div className="mt-0.5 text-xs leading-snug text-ink-muted">{d}</div>
                  <div className="mt-1 text-xs font-semibold text-taxi">{STUDENTS[who].name}</div>
                </div>
              ))}
            </div>
          </div>
          {L.arrow && (
            <div className="flex w-[62px] shrink-0 flex-col items-center justify-center text-center">
              <span className="text-[0.65rem] leading-tight text-ink-muted">{L.arrow}</span>
              <span className="text-xl text-taxi" aria-hidden>→</span>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

const TASKS = [
  [0, 'Veri temini, sözlük, CSV/Parquet, boyut analizi', '20 kolonluk sözlük · 5 boyut × 4 format · 85 okuma ölçümü'],
  [1, 'Temizlik ve anomali analizi', '10 kural · 17.210.961 temiz kayıt · kural vs istatistik'],
  [2, 'PySpark aggregation', '6 Spark özet tablosu · window · broadcast/sort-merge join deneyi'],
  [3, 'ML: yolculuk süresi tahmini', '16 eğitim ölçümü (2 çerçeve × 2 model × 4 boyut) · ayarlı RMSE 5,042'],
  [4, 'Performans karşılaştırması + dashboard', 'pandas/Spark × CSV/Parquet, 100K–57M · bu dashboard'],
]

const ENV = [
  ['Yazılım', 'Python 3.11 · PySpark 3.5.9 (JDK 17) · pandas 2.3.3 · pyarrow 26.0.1 · scikit-learn 1.5.2 · numpy 2.4.6'],
  ['İşleme / ML / performans makinesi', 'Intel i7-13700H, 14 çekirdek / 20 iş parçacığı, 32 GB RAM, Spark local[*]'],
  ['Format ve okuma ölçümleri', '28 çekirdek, 16 GB RAM (Spark yerel mod)'],
  ['Dashboard', 'React + ECharts, internetsiz çalışır'],
]

export function Intro() {
  const checks = ['Gerçek açık veri + sentetik 57M', '5 gerçek boyut + 57M', 'PySpark 3.5', 'CSV ve Parquet (3 sıkıştırma)', 'aggregation · window · join', 'süre · bellek · disk boyutu', 'interaktif grafik · harita · simülasyon', 'her kartta "Ana çıkarım"', 'katmanlı mimari şema', 'öğrenci başına ayrı panel']
  return (
    <div>
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.5 }}>
        <div className="mb-2 text-sm font-semibold tracking-[0.2em] text-taxi">BÜYÜK VERİ ANALİTİĞİ · PROJE</div>
        <h1 className="text-5xl font-extrabold leading-tight tracking-tight">NYC Taxi <span className="text-taxi">Büyük Veri Analitiği</span></h1>
        <div className="mt-5 grid gap-3 lg:grid-cols-2">
          <p className={`${box} text-lg`}><span className="block text-sm font-semibold text-signal">Problem</span>Milyonlarca taksi yolculuğundan şehir içi ulaşım davranışlarını analiz etmek.</p>
          <p className="rounded-2xl border border-taxi/50 bg-night-850 p-5 text-lg"><span className="block text-sm font-semibold text-taxi">Araştırma sorusu</span>Veri büyüdükçe klasik analiz yaklaşımı ne zaman yetersiz kalıyor?</p>
        </div>
      </motion.div>

      <h2 className="mt-8 mb-3 text-xl font-bold">Ekip</h2>
      <TeamList />

      <div className="mt-6"><BigDataCard /></div>

      <h2 className="mt-8 mb-3 text-xl font-bold">Mimari: katmanlar ve veri akışı</h2>
      <Architecture />

      <h2 className="mt-8 mb-3 text-xl font-bold">Görev dağılımı</h2>
      <div className="overflow-hidden rounded-2xl border border-night-700">
        {TASKS.map(([who, task, out], i) => (
          <div key={task} className={`grid grid-cols-[1.3fr_1fr_2fr] gap-3 px-4 py-2 ${i % 2 ? 'bg-night-900/40' : 'bg-night-850'}`}>
            <span className="font-semibold">{task}</span>
            <span className="text-taxi">{STUDENTS[who].name}</span>
            <span className="text-ink-muted">{out}</span>
          </div>
        ))}
      </div>

      <div className="mt-8 grid gap-4 lg:grid-cols-[3fr_2fr]">
        <div className={box}>
          <div className="text-xl font-bold">Çalışma ortamı</div>
          <dl className="mt-2 space-y-1.5">
            {ENV.map(([k, v]) => <div key={k}><dt className="text-sm text-signal">{k}</dt><dd className="text-ink">{v}</dd></div>)}
          </dl>
          <div className="mt-2 text-xs text-ink-faint">Kaynak: requirements.txt, docs/technical_report_s1_s2.md ve s5.md</div>
        </div>
        <div className={box}>
          <div className="text-xl font-bold">Gereksinimler</div>
          <ul className="mt-2 space-y-1">{checks.map((t) => <li key={t} className="flex gap-2"><span className="font-bold text-ok">✓</span><span className="text-ink-muted">{t}</span></li>)}</ul>
        </div>
      </div>
    </div>
  )
}

export function StudentPanel({ panel }) {
  const s = STUDENTS[panel.n - 1]
  return (
    <article>
      <header className="mb-6">
        <div className="flex flex-wrap items-baseline gap-x-3 text-3xl font-extrabold">
          <span className="text-taxi">{s.name}</span><span className="text-xl font-semibold text-ink-muted num">({s.no})</span>
        </div>
        <div className="mt-1 text-lg text-ink-muted">{panel.short}</div>
        <p className="mt-4 rounded-xl border-l-4 border-taxi bg-night-850 px-5 py-3 text-lg"><span className="font-semibold text-taxi">Teknik katkı: </span>{panel.contribution}</p>
      </header>
      {panel.items.map((item, i) => <Item key={i} item={item} />)}
      {panel.answer && (
        <section className="my-8 rounded-2xl border-2 border-taxi bg-night-850 p-6">
          <div className="text-sm font-semibold text-taxi">Araştırma sorusunun cevabı</div>
          <h2 className="mt-1 text-2xl font-bold">Veri büyüdükçe klasik yaklaşım ne zaman yetersiz kalıyor?</h2>
          <p className="mt-3 text-lg leading-relaxed">{panel.answer.join(' ')}</p>
        </section>
      )}
    </article>
  )
}

export function Thanks() {
  return (
    <div className="flex min-h-[70vh] flex-col justify-center">
      <motion.h1 initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} className="text-6xl font-extrabold">Teşekkürler</motion.h1>
      <p className="mt-5 max-w-4xl text-xl text-ink-muted">Veri büyüdükçe önce CSV okuma, sonra bellek darboğaz oluyor: formatı değiştirmek motoru değiştirmekten daha etkili, ama RAM dolduğunda pandas çalışamaz.</p>
      <div className="mt-10"><TeamList large /></div>
      <div className="mt-12 text-5xl font-extrabold text-taxi">Sorularınız?</div>
    </div>
  )
}
