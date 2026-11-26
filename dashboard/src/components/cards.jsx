import { useMemo, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import rehypeRaw from 'rehype-raw'
import { Highlight, themes } from 'prism-react-renderer'
import { imageUrl } from '../lib/data.js'

export function Markdown({ text, attachments }) {
  const src = attachments
    ? text.replace(/attachment:([^)\s]+)/g, (m, name) => (attachments[name] ? imageUrl(attachments[name]) : m))
    : text
  return (
    <div className="prose-nb">
      <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeRaw]}
        components={{ a: ({ href, children }) => <a href={href} target="_blank" rel="noreferrer">{children}</a> }}>
        {src}
      </ReactMarkdown>
    </div>
  )
}

/** Ne gösteriyor / Nasıl okunur / Ana çıkarım — her kartta her zaman görünür. */
export function Triplet({ ann }) {
  if (!ann) return null
  const rows = [['Ne gösteriyor', ann.what, 'text-signal'], ['Nasıl okunur', ann.how, 'text-ink-muted'], ['Ana çıkarım', ann.takeaway, 'text-taxi']]
  return (
    <div className="grid gap-3 border-t border-night-700 bg-night-900/60 px-5 py-4 md:grid-cols-3">
      {rows.map(([label, body, color]) => (
        <div key={label}>
          <div className={`mb-1 text-xs font-bold uppercase tracking-[0.14em] ${color}`}>{label}</div>
          <div className="text-[0.95rem] leading-snug text-ink">{body}</div>
        </div>
      ))}
    </div>
  )
}

export function Card({ title, badge, children, ann, footer }) {
  return (
    <figure className="my-6 overflow-hidden rounded-2xl border border-night-700 bg-night-850 shadow-[0_10px_40px_-20px_rgba(0,0,0,0.8)]">
      {(title || badge) && (
        <figcaption className="flex items-center justify-between gap-3 border-b border-night-700 px-5 py-3">
          <span className="font-semibold">{title}</span>
          {badge && <span className="shrink-0 text-xs text-ink-faint">{badge}</span>}
        </figcaption>
      )}
      <div className="p-4">{children}</div>
      {footer}
      <Triplet ann={ann} />
    </figure>
  )
}

const SKIP = /^(import |from |\s*$|#)/
/** 5–15 satırlık anlamlı kod özeti: baştaki import/boş/yorum satırları atlanır. */
export function snippetOf(source) {
  const lines = source.split('\n')
  if (lines.length <= 15) return { text: source, partial: false }
  let start = 0
  while (start < lines.length - 5 && SKIP.test(lines[start])) start += 1
  const text = lines.slice(start, start + 12).join('\n')
  return { text, partial: true }
}

export function CodeCard({ source, where }) {
  const [full, setFull] = useState(false)
  const snip = useMemo(() => snippetOf(source), [source])
  if (!source.trim()) return null
  const code = full ? source : snip.text
  return (
    <div className="my-4 overflow-hidden rounded-xl border border-night-700 bg-night-950/80">
      <div className="flex items-center justify-between border-b border-night-700 px-4 py-2 text-xs text-ink-faint">
        <span className="font-mono">{where}</span>
        {snip.partial && (
          <button type="button" onClick={() => setFull((v) => !v)} className="rounded-md border border-night-600 px-2 py-0.5 text-ink-muted hover:border-taxi hover:text-ink">
            {full ? 'Özeti göster' : `Tam kodu göster (${source.split('\n').length} satır)`}
          </button>
        )}
      </div>
      <Highlight theme={themes.nightOwl} code={code.replace(/\n$/, '')} language="python">
        {({ tokens, getLineProps, getTokenProps }) => (
          <pre className="max-h-[520px] overflow-auto px-4 py-3 font-mono text-[0.8rem] leading-relaxed">
            {tokens.map((line, i) => (
              <div key={i} {...getLineProps({ line })}>
                {line.map((token, k) => <span key={k} {...getTokenProps({ token })} />)}
              </div>
            ))}
          </pre>
        )}
      </Highlight>
    </div>
  )
}

const numeric = (s) => {
  const t = s.replace(/[%+−\s]/g, '').replace('−', '-')
  if (/^-?[\d.]+,\d+$/.test(t)) return Number(t.replace(/\./g, '').replace(',', '.'))
  if (/^-?\d{1,3}(\.\d{3})+$/.test(t)) return Number(t.replace(/\./g, ''))
  if (/^-?[\d,]*\.?\d+$/.test(t)) return Number(t.replace(/,/g, ''))
  return null
}

/** Notebook tablosu: hücre metinleri olduğu gibi; başlığa tıklayınca sıralanır; satır kesilmez. */
export function DataTable({ table }) {
  const [sort, setSort] = useState(null)
  const headerRows = table.header
  const width = Math.max(...table.rows.map((r) => r.length), ...headerRows.map((h) => h.reduce((s, c) => s + (c.colspan || 1), 0)), 0)
  const labels = useMemo(() => {
    const out = Array.from({ length: width }, () => [])
    headerRows.forEach((row) => {
      let col = 0
      row.forEach((c) => { for (let k = 0; k < (c.colspan || 1); k += 1) { if (c.text) out[col].push(c.text); col += 1 } })
    })
    return out.map((parts) => [...new Set(parts)].join(' · '))
  }, [headerRows, width])
  const rows = useMemo(() => {
    if (!sort) return table.rows
    const { col, dir } = sort
    return [...table.rows].sort((a, b) => {
      const x = a[col]?.text ?? ''; const y = b[col]?.text ?? ''
      const nx = numeric(x); const ny = numeric(y)
      const c = nx !== null && ny !== null ? nx - ny : x.localeCompare(y, 'tr')
      return dir * c
    })
  }, [sort, table.rows])
  return (
    <div className="max-h-[560px] overflow-auto rounded-xl border border-night-700">
      <table className="w-full border-collapse text-[0.9rem]">
        <thead className="sticky top-0 bg-night-800">
          <tr>
            {labels.map((label, i) => (
              <th key={i} scope="col" className="cursor-pointer select-none whitespace-nowrap border-b border-night-600 px-3 py-2 text-left font-semibold text-ink-muted hover:text-taxi"
                onClick={() => setSort((s) => ({ col: i, dir: s?.col === i ? -s.dir : -1 }))}>
                {label || ' '}{sort?.col === i ? (sort.dir < 0 ? ' ▼' : ' ▲') : ''}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="odd:bg-night-900/40 hover:bg-night-700/40">
              {r.map((c, j) => (
                <td key={j} className={`border-b border-night-800 px-3 py-1.5 align-top num ${c.th ? 'font-semibold text-ink' : 'text-ink'} ${numeric(c.text) !== null ? 'text-right' : ''}`}>{c.text}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function ImageView({ src, alt }) {
  return <img src={imageUrl(src)} alt={alt} loading="lazy" className="mx-auto max-h-[720px] w-auto max-w-full rounded-lg bg-white" />
}
