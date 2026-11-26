import { useCallback, useEffect, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { PANELS } from './content/panels.js'
import { STUDENTS } from './content/team.js'
import { Intro, StudentPanel, Thanks } from './components/Panels.jsx'

const indexOfHash = () => PANELS.findIndex((p) => `#${p.id}` === window.location.hash)
const subtitle = (p) => (p.kind === 'student' ? STUDENTS[p.n - 1].name : null)

export default function App() {
  const [index, setIndex] = useState(() => Math.max(0, indexOfHash()))
  const [scrollPct, setScrollPct] = useState(0)
  const [isFull, setIsFull] = useState(false)
  const mainRef = useRef(null)
  const panel = PANELS[index]

  const go = useCallback((i) => {
    const next = Math.max(0, Math.min(PANELS.length - 1, i))
    setIndex(next)
    window.history.replaceState(null, '', `#${PANELS[next].id}`)
    mainRef.current?.scrollTo({ top: 0 })
  }, [])

  useEffect(() => {
    const onKey = (e) => {
      const tag = e.target.tagName
      if (['INPUT', 'SELECT', 'TEXTAREA'].includes(tag) || e.altKey || e.ctrlKey || e.metaKey) return
      if (e.key === 'ArrowRight') { e.preventDefault(); go(index + 1) }
      if (e.key === 'ArrowLeft') { e.preventDefault(); go(index - 1) }
      if (e.key === 'f' || e.key === 'F') toggleFull()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [index, go])

  useEffect(() => {
    const onHash = () => {
      const i = indexOfHash()
      if (i >= 0) { setIndex(i); mainRef.current?.scrollTo({ top: 0 }) }
    }
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  useEffect(() => {
    const onFs = () => setIsFull(Boolean(document.fullscreenElement))
    document.addEventListener('fullscreenchange', onFs)
    return () => document.removeEventListener('fullscreenchange', onFs)
  }, [])

  const onScroll = (e) => {
    const el = e.currentTarget
    const max = el.scrollHeight - el.clientHeight
    setScrollPct(max > 0 ? el.scrollTop / max : 0)
  }
  const progress = (index + scrollPct) / (PANELS.length - 1 || 1)

  return (
    <div className="flex h-screen overflow-hidden">
      <aside className="flex w-[280px] shrink-0 flex-col border-r border-night-700 bg-night-900/80 backdrop-blur">
        <div className="border-b border-night-700 px-5 py-4">
          <div className="text-xs font-semibold tracking-[0.2em] text-taxi">NYC TAXI</div>
          <div className="text-lg font-bold leading-tight">Büyük Veri Analitiği</div>
        </div>
        <nav className="flex-1 overflow-y-auto px-3 py-3" aria-label="Paneller">
          {PANELS.map((p, i) => {
            const active = i === index
            return (
              <button key={p.id} type="button" onClick={() => go(i)} aria-current={active ? 'page' : undefined}
                className={`mb-1 flex w-full items-center gap-3 rounded-xl px-3 py-2 text-left transition ${active ? 'bg-taxi text-night-950' : 'text-ink-muted hover:bg-night-800 hover:text-ink'}`}>
                <span className={`grid h-7 w-7 shrink-0 place-items-center rounded-lg text-xs font-bold ${active ? 'bg-night-950 text-taxi' : 'bg-night-800 text-ink-faint'}`}>{i + 1}</span>
                <span className="min-w-0">
                  <span className="block truncate text-[0.95rem] font-semibold">{p.short}</span>
                  {p.kind === 'student' && <span className={`block text-xs leading-snug ${active ? 'text-night-800' : 'text-ink-faint'}`}>{STUDENTS[p.n - 1].name}</span>}
                </span>
              </button>
            )
          })}
        </nav>
        <div className="border-t border-night-700 px-5 py-3 text-xs text-ink-faint">← → panel · F tam ekran</div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="relative flex items-center gap-4 border-b border-night-700 bg-night-900/70 px-6 py-3 backdrop-blur">
          <div className="min-w-0 flex-1">
            <div className="truncate text-sm text-ink-faint">{subtitle(panel) ?? 'Sunum'}</div>
            <div className="truncate text-lg font-semibold">{panel.short}</div>
          </div>
          <div className="flex items-center gap-1.5" aria-label="Panel göstergesi">
            {PANELS.map((p, i) => (
              <button key={p.id} type="button" onClick={() => go(i)} aria-label={`${i + 1}. panel: ${p.short}`} title={p.short}
                className={`h-3 rounded-full transition-all ${i === index ? 'w-8 bg-taxi' : 'w-3 bg-night-600 hover:bg-ink-faint'}`} />
            ))}
            <span className="ml-2 text-sm text-ink-faint num">{index + 1} / {PANELS.length}</span>
          </div>
          <button type="button" onClick={() => go(index - 1)} disabled={index === 0} className="rounded-lg border border-night-600 px-3 py-1.5 text-sm hover:border-taxi disabled:opacity-30" aria-label="Önceki panel">←</button>
          <button type="button" onClick={() => go(index + 1)} disabled={index === PANELS.length - 1} className="rounded-lg border border-night-600 px-3 py-1.5 text-sm hover:border-taxi disabled:opacity-30" aria-label="Sonraki panel">→</button>
          <button type="button" onClick={toggleFull} className="rounded-lg bg-taxi px-3 py-1.5 text-sm font-semibold text-night-950 hover:bg-taxi-deep">{isFull ? 'Tam ekrandan çık' : '⛶ Tam ekran'}</button>
          <div className="absolute inset-x-0 bottom-0 h-1 bg-night-800">
            <div className="h-full bg-gradient-to-r from-taxi to-signal transition-[width] duration-300" style={{ width: `${progress * 100}%` }} />
          </div>
        </header>
        <main ref={mainRef} onScroll={onScroll} className="flex-1 overflow-y-auto">
          <AnimatePresence mode="wait">
            <motion.div key={panel.id} initial={{ opacity: 0, y: 18 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -10 }} transition={{ duration: 0.28 }}
              className="mx-auto max-w-[1280px] px-8 py-8">
              {panel.kind === 'intro' && <Intro />}
              {panel.kind === 'student' && <StudentPanel panel={panel} />}
              {panel.kind === 'thanks' && <Thanks />}
            </motion.div>
          </AnimatePresence>
        </main>
      </div>
    </div>
  )
}

function toggleFull() {
  if (document.fullscreenElement) document.exitFullscreen()
  else document.documentElement.requestFullscreen?.()
}
