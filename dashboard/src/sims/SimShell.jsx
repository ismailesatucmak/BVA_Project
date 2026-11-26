import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'

/** Ortak simülasyon kabuğu: mod göstergesi, oynatma kontrolleri, parametre paneli. */
export default function SimShell({ title, sampleN, steps, step, setStep, live, setLive, params, notebookView, liveView, stepLabel, explain }) {
  const [playing, setPlaying] = useState(false)
  const [speed, setSpeed] = useState(1)
  useEffect(() => {
    if (!playing) return undefined
    if (step >= steps - 1) { setPlaying(false); return undefined }
    const t = setTimeout(() => setStep(step + 1), 900 / speed)
    return () => clearTimeout(t)
  }, [playing, step, steps, speed, setStep])
  const start = () => { setLive(true); setStep(0); setPlaying(true) }
  const back = () => { setPlaying(false); setLive(false) }
  return (
    <section className="my-8 overflow-hidden rounded-2xl border-2 border-taxi/50 bg-night-850 shadow-[0_0_60px_-30px_rgba(255,198,41,0.6)]">
      <div className="flex flex-wrap items-center gap-3 border-b border-night-700 bg-night-800/70 px-5 py-3">
        <span className="text-xs font-bold uppercase tracking-[0.18em] text-taxi">Simülasyon</span>
        <span className="text-lg font-bold">{title}</span>
        <span className={`ml-auto rounded-full px-3 py-1 text-sm font-semibold ${live ? 'bg-signal/15 text-signal' : 'bg-taxi/15 text-taxi'}`}>
          {live ? `⚡ Canlı hesaplama (örneklem: ${sampleN ?? '…'} kayıt)` : '📊 Notebook sonucu (tam veri)'}
        </span>
      </div>
      <div className="grid gap-0 lg:grid-cols-[300px_1fr]">
        <div className="space-y-4 border-b border-night-700 p-5 lg:border-b-0 lg:border-r">
          {params}
          <div className="space-y-2 border-t border-night-700 pt-4">
            {!live && <button type="button" onClick={start} className="w-full rounded-xl bg-taxi px-4 py-2.5 font-bold text-night-950 hover:bg-taxi-deep">▶ Simülasyonu Çalıştır</button>}
            {live && (
              <>
                <div className="flex gap-2">
                  <button type="button" onClick={() => setPlaying((p) => !p)} className="flex-1 rounded-lg border border-night-600 py-1.5 hover:border-taxi">{playing ? '⏸ Duraklat' : '▶ Oynat'}</button>
                  <button type="button" onClick={() => { setPlaying(false); setStep(Math.max(0, step - 1)) }} className="rounded-lg border border-night-600 px-3 hover:border-taxi" aria-label="Önceki adım">⏮</button>
                  <button type="button" onClick={() => { setPlaying(false); setStep(Math.min(steps - 1, step + 1)) }} className="rounded-lg border border-night-600 px-3 hover:border-taxi" aria-label="Sonraki adım">⏭</button>
                </div>
                <label className="block text-sm text-ink-muted">Hız: {speed}×
                  <input type="range" min="0.5" max="4" step="0.5" value={speed} onChange={(e) => setSpeed(Number(e.target.value))} className="w-full accent-[#ffc629]" />
                </label>
                <div className="text-sm text-ink-muted num">Adım {Math.min(step + 1, steps)} / {steps}{stepLabel ? ` · ${stepLabel}` : ''}</div>
                <button type="button" onClick={back} className="w-full rounded-lg border border-taxi px-3 py-1.5 text-taxi hover:bg-taxi/10">↩ Notebook sonucuna dön</button>
              </>
            )}
          </div>
        </div>
        <motion.div key={live ? 'live' : 'nb'} initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="min-w-0 p-5">
          {live ? liveView : notebookView}
          {explain && <p className="mt-3 text-[0.95rem] text-ink-muted">{explain}</p>}
        </motion.div>
      </div>
    </section>
  )
}

export function Range({ label, value, min, max, step = 1, onChange, fmt = (v) => v }) {
  return (
    <label className="block text-sm text-ink-muted">
      <span className="flex justify-between"><span>{label}</span><span className="font-semibold text-ink num">{fmt(value)}</span></span>
      <input type="range" min={min} max={max} step={step} value={value} onChange={(e) => onChange(Number(e.target.value))} className="w-full accent-[#ffc629]" />
    </label>
  )
}

export function Select({ label, value, options, onChange }) {
  return (
    <label className="block text-sm text-ink-muted">{label}
      <select value={value} onChange={(e) => onChange(e.target.value)} className="mt-1 w-full rounded-lg border border-night-600 bg-night-900 px-2 py-1.5 text-ink">
        {options.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
      </select>
    </label>
  )
}

export function Toggle({ label, value, onChange }) {
  return (
    <label className="flex cursor-pointer items-center justify-between text-sm text-ink-muted">{label}
      <input type="checkbox" checked={value} onChange={(e) => onChange(e.target.checked)} className="h-5 w-5 accent-[#ffc629]" />
    </label>
  )
}
