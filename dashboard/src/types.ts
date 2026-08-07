/* The wire contract, exactly as backend/webserver.py sends it.
   Every message is a flat object; `type` is the only discriminant. */

export type BarKind = 'backfill' | 'live'

export interface Bar {
  symbol: string
  /** Unix seconds, float. Floor before handing to Lightweight Charts. */
  window_start: number
  window_end: number
  open: number
  high: number
  low: number
  close: number
  /** number of ticks that fed this candle */
  count: number
}

export interface BarMessage extends Bar {
  type: BarKind
}

export type ConnState = 'connecting' | 'connected' | 'disconnected'

const isFiniteNumber = (v: unknown): v is number =>
  typeof v === 'number' && Number.isFinite(v)

/* Socket frames are `unknown` until proven otherwise. A malformed message must not tear the
   connection down, so this returns null instead of throwing and the caller skips the frame. */
export function parseBarMessage(raw: unknown): BarMessage | null {
  if (typeof raw !== 'object' || raw === null) return null
  const m = raw as Record<string, unknown>

  if (m.type !== 'backfill' && m.type !== 'live') return null
  if (typeof m.symbol !== 'string') return null

  const nums = ['window_start', 'window_end', 'open', 'high', 'low', 'close', 'count'] as const
  for (const k of nums) {
    if (!isFiniteNumber(m[k])) return null
  }

  return {
    type: m.type,
    symbol: m.symbol,
    window_start: m.window_start as number,
    window_end: m.window_end as number,
    open: m.open as number,
    high: m.high as number,
    low: m.low as number,
    close: m.close as number,
    count: m.count as number,
  }
}
