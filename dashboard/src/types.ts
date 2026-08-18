/* the wire contract as backend/webserver.py sends it, every message is flat and `type` is
   the only discriminant */

export type BarKind = 'backfill' | 'live'

export interface Bar {
  symbol: string
  /** unix seconds as a float, floor it before handing it to lightweight-charts */
  window_start: number
  window_end: number
  open: number
  high: number
  low: number
  close: number
  /** ticks that fed this candle */
  count: number
}

export interface BarMessage extends Bar {
  type: BarKind
}

export type ConnState = 'connecting' | 'connected' | 'disconnected'

const isFiniteNumber = (v: unknown): v is number => Number.isFinite(v)

/* the typeof is what lets `v >= 0` typecheck, unlike above where isFinite alone is enough */
const isCount = (v: unknown): v is number =>
  typeof v === 'number' && Number.isInteger(v) && v >= 0

/* a malformed frame must not tear the connection down, so this returns null and the caller
   skips it. fields are checked one by one rather than in a loop because that is the form
   TypeScript narrows, which is what keeps the returned object cast free */
export function parseBarMessage(raw: unknown): BarMessage | null {
  if (typeof raw !== 'object' || raw === null) return null
  const m = raw as Record<string, unknown>

  if (m.type !== 'backfill' && m.type !== 'live') return null
  if (typeof m.symbol !== 'string') return null

  if (!isFiniteNumber(m.window_start)) return null
  if (!isFiniteNumber(m.window_end)) return null
  if (!isFiniteNumber(m.open)) return null
  if (!isFiniteNumber(m.high)) return null
  if (!isFiniteNumber(m.low)) return null
  if (!isFiniteNumber(m.close)) return null
  /* tighter than the prices, a count is a cardinality so -3.7 is as malformed as NaN */
  if (!isCount(m.count)) return null

  return {
    type: m.type,
    symbol: m.symbol,
    window_start: m.window_start,
    window_end: m.window_end,
    open: m.open,
    high: m.high,
    low: m.low,
    close: m.close,
    count: m.count,
  }
}
