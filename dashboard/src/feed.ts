import { parseBarMessage, type Bar, type ConnState } from './types'

/* one socket per symbol, no React in here, bars go to a sink */

/* what the chart did with a live bar, only the chart knows what is actually drawn */
export type LiveOutcome =
  | 'applied'               // new window, appended
  | 'duplicate-superseded'  // same window re-emitted with a higher count, candle replaced
  | 'duplicate-ignored'     // same window re-emitted with an equal or lower count, declined
  | 'late-dropped'          // window predates the last drawn one, declined
  | 'not-drawn'             // no series yet, nothing drawn and nothing tallied

export interface BarSink {
  /** already sorted ascending by window_start */
  onBackfill(bars: Bar[]): void
  onLive(bar: Bar): LiveOutcome
}

/* the only thing that crosses into React state, rebuilt at most once per bar */
export interface Summary {
  lastClose: number | null
  lastCount: number | null
  lastWindowStart: number | null
  lastWindowSpan: number | null
  firstOpen: number | null
  high: number | null
  low: number | null
  barCount: number
  lateDropped: number
  duplicateSuperseded: number
  duplicateIgnored: number
}

export const EMPTY_SUMMARY: Summary = {
  lastClose: null,
  lastCount: null,
  lastWindowStart: null,
  lastWindowSpan: null,
  firstOpen: null,
  high: null,
  low: null,
  barCount: 0,
  lateDropped: 0,
  duplicateSuperseded: 0,
  duplicateIgnored: 0,
}

export function socketUrl(symbol: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}/ws/${encodeURIComponent(symbol)}`
}

const BACKOFF_MIN_MS = 500
const BACKOFF_MAX_MS = 8000
/* no end of backfill marker on the wire, so the burst ends at the first live frame or a gap */
const BACKFILL_IDLE_MS = 250

export interface FeedCallbacks {
  onStatus(state: ConnState, attempt: number): void
  onSummary(summary: Summary): void
}

export class SymbolFeed {
  private ws: WebSocket | null = null
  private sink: BarSink | null = null
  private closed = false

  private attempt = 0
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private backfillTimer: ReturnType<typeof setTimeout> | null = null

  private backfillBuf: Bar[] = []
  private backfillFlushed = false
  /* held so a chart that mounts after the burst still gets its history */
  private lastBackfill: Bar[] = []

  private agg: Summary = { ...EMPTY_SUMMARY }

  private readonly symbol: string
  private readonly cb: FeedCallbacks

  constructor(symbol: string, cb: FeedCallbacks) {
    this.symbol = symbol
    this.cb = cb
  }

  setSink(sink: BarSink | null): void {
    this.sink = sink
    if (sink && this.backfillFlushed && this.lastBackfill.length) {
      sink.onBackfill(this.lastBackfill)
    }
  }

  start(): void {
    this.closed = false
    this.open()
  }

  stop(): void {
    this.closed = true
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    if (this.backfillTimer) clearTimeout(this.backfillTimer)
    this.reconnectTimer = null
    this.backfillTimer = null
    const ws = this.ws
    this.ws = null
    if (ws) {
      // drop handlers first or close() schedules a reconnect on a dead feed
      ws.onopen = ws.onclose = ws.onerror = ws.onmessage = null
      ws.close()
    }
  }

  private open(): void {
    if (this.closed) return

    this.attempt += 1
    this.cb.onStatus('connecting', this.attempt)

    this.backfillBuf = []
    this.backfillFlushed = false

    let ws: WebSocket
    try {
      ws = new WebSocket(socketUrl(this.symbol))
    } catch {
      this.scheduleReconnect()
      return
    }
    this.ws = ws

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.attempt = 0
      this.cb.onStatus('connected', 0)
    }

    ws.onmessage = (ev: MessageEvent) => {
      if (this.ws !== ws) return
      this.handleFrame(ev.data)
    }

    ws.onerror = () => {
      /* onclose always follows, reconnect happens there so it happens once */
    }

    ws.onclose = () => {
      if (this.ws !== ws) return
      this.ws = null
      this.cb.onStatus('disconnected', this.attempt)
      this.scheduleReconnect()
    }
  }

  private handleFrame(data: unknown): void {
    if (typeof data !== 'string') return

    let raw: unknown
    try {
      raw = JSON.parse(data)
    } catch {
      return // malformed JSON, skip the frame and keep the socket
    }

    const msg = parseBarMessage(raw)
    if (!msg) return // unexpected shape, same treatment

    const { type, ...bar } = msg

    if (type === 'backfill' && !this.backfillFlushed) {
      this.backfillBuf.push(bar)
      if (this.backfillTimer) clearTimeout(this.backfillTimer)
      this.backfillTimer = setTimeout(() => this.flushBackfill(), BACKFILL_IDLE_MS)
      return
    }

    if (!this.backfillFlushed) this.flushBackfill()

    this.foldLive(bar)
  }

  private flushBackfill(): void {
    if (this.backfillFlushed) return
    if (this.backfillTimer) {
      clearTimeout(this.backfillTimer)
      this.backfillTimer = null
    }
    this.backfillFlushed = true

    /* the burst can straddle a gap and the chart throws on unordered data */
    const sorted = [...this.backfillBuf].sort((a, b) => a.window_start - b.window_start)
    this.backfillBuf = []
    this.lastBackfill = sorted

    /* aggregates restart with the new history, the anomaly tallies are session scoped */
    this.agg = {
      ...EMPTY_SUMMARY,
      lateDropped: this.agg.lateDropped,
      duplicateSuperseded: this.agg.duplicateSuperseded,
      duplicateIgnored: this.agg.duplicateIgnored,
    }
    for (const b of sorted) this.accumulate(b, 'applied')

    this.sink?.onBackfill(sorted)
    this.cb.onSummary(this.agg)
  }

  private foldLive(bar: Bar): void {
    const outcome: LiveOutcome = this.sink ? this.sink.onLive(bar) : 'not-drawn'
    this.accumulate(bar, outcome)
    this.cb.onSummary(this.agg)
  }

  /* high and low take every bar, a late one still reports a price that traded, the headline
     figures take only what the chart drew */
  private accumulate(bar: Bar, outcome: LiveOutcome): void {
    const a = this.agg
    const drawn = outcome === 'applied' || outcome === 'duplicate-superseded'
    this.agg = {
      lastClose: drawn ? bar.close : a.lastClose,
      lastCount: drawn ? bar.count : a.lastCount,
      lastWindowStart: drawn ? bar.window_start : a.lastWindowStart,
      lastWindowSpan: drawn ? bar.window_end - bar.window_start : a.lastWindowSpan,
      firstOpen: a.firstOpen === null ? bar.open : a.firstOpen,
      high: a.high === null ? bar.high : Math.max(a.high, bar.high),
      low: a.low === null ? bar.low : Math.min(a.low, bar.low),
      // a superseded duplicate replaces a candle rather than adding one
      barCount: outcome === 'applied' ? a.barCount + 1 : a.barCount,
      lateDropped: a.lateDropped + (outcome === 'late-dropped' ? 1 : 0),
      duplicateSuperseded: a.duplicateSuperseded + (outcome === 'duplicate-superseded' ? 1 : 0),
      duplicateIgnored: a.duplicateIgnored + (outcome === 'duplicate-ignored' ? 1 : 0),
    }
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer) return
    // jittered so the sockets do not retry in lockstep
    const base = Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS * 2 ** Math.max(0, this.attempt - 1))
    const delay = base * (0.7 + Math.random() * 0.6)
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.open()
    }, delay)
  }
}
