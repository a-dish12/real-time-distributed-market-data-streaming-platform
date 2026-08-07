import { parseBarMessage, type Bar, type ConnState } from './types'

/* One socket per symbol. This module is deliberately framework-free: it owns the connection,
   the reconnect backoff, and the running aggregates, and it hands bars to a sink.
   React never sees a bar — see useSymbolFeed.ts for where the (small) state boundary sits. */

/* What the chart did with a live bar. The chart is the only thing that knows what is actually
   drawn, so it decides and reports back; the feed just tallies. These are deliberately four
   distinct outcomes — an ignored duplicate and a dropped late bar indicate different upstream
   conditions, and collapsing them would lose that. */
export type LiveOutcome =
  | 'applied'               // new window, appended
  | 'duplicate-superseded'  // same window re-emitted with a higher count, candle replaced
  | 'duplicate-ignored'     // same window re-emitted with an equal or lower count, declined
  | 'late-dropped'          // window predates the last drawn one, declined
  | 'not-drawn'             // no series yet; nothing was drawn, nothing is tallied

/** What the chart registers with the feed. Both callbacks are imperative. */
export interface BarSink {
  /** Called once per connection, with bars already sorted ascending by window_start. */
  onBackfill(bars: Bar[]): void
  /** Called for each live bar, in arrival order. Returns what the chart did with it. */
  onLive(bar: Bar): LiveOutcome
}

/* The only thing that crosses into React state, rebuilt at most once per bar.

   Deliberately flat primitives rather than a retained Bar: these are display values for the
   panel header, not bar data. No Bar object and no series of bars is ever held in state — the
   bars themselves go straight to the chart's series ref. */
export interface Summary {
  /** newest close, the one number the header exists to show */
  lastClose: number | null
  lastCount: number | null
  lastWindowStart: number | null
  lastWindowSpan: number | null
  /** open of the earliest bar held, for change-since-first */
  firstOpen: number | null
  high: number | null
  low: number | null
  barCount: number
  /** live bars declined because they predate the last bar drawn */
  lateDropped: number
  /** same-window re-emissions that were more complete and replaced the drawn candle */
  duplicateSuperseded: number
  /** same-window re-emissions that were no more complete and were declined */
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

/** Derive the socket URL from the page origin. The backend host is never named here:
    in production the page is served by uvicorn itself, and in dev Vite proxies /ws. */
export function socketUrl(symbol: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}/ws/${encodeURIComponent(symbol)}`
}

const BACKOFF_MIN_MS = 500
const BACKOFF_MAX_MS = 8000
/* The server replays history in a tight loop and gives no end-of-backfill marker, so the
   burst is delimited by either the first live frame or a short idle gap. */
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

  /* Per-connection backfill staging. Not React state: these churn on every frame. */
  private backfillBuf: Bar[] = []
  private backfillFlushed = false
  /* Held so a chart that mounts after the burst still gets its history. */
  private lastBackfill: Bar[] = []

  /* Running aggregates. Plain fields — recomputing these by mapping a retained bar array on
     every render is exactly the pattern that forces bar data into state. */
  private agg: Summary = { ...EMPTY_SUMMARY }

  private readonly symbol: string
  private readonly cb: FeedCallbacks

  constructor(symbol: string, cb: FeedCallbacks) {
    this.symbol = symbol
    this.cb = cb
  }

  /** The chart registers here. If history already arrived, it is replayed immediately. */
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
      // Drop handlers first so the close does not schedule a reconnect on an unmounted feed.
      ws.onopen = ws.onclose = ws.onerror = ws.onmessage = null
      ws.close()
    }
  }

  private open(): void {
    if (this.closed) return

    this.attempt += 1
    this.cb.onStatus('connecting', this.attempt)

    // A fresh connection means a fresh backfill; stage it from scratch.
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
      /* onclose always follows; reconnect is driven from there so it happens exactly once. */
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
      return // malformed JSON: skip the frame, keep the socket
    }

    const msg = parseBarMessage(raw)
    if (!msg) return // unexpected shape: same treatment

    const { type, ...bar } = msg

    if (type === 'backfill' && !this.backfillFlushed) {
      this.backfillBuf.push(bar)
      // Restart the idle timer; the burst ends when frames stop or a live frame arrives.
      if (this.backfillTimer) clearTimeout(this.backfillTimer)
      this.backfillTimer = setTimeout(() => this.flushBackfill(), BACKFILL_IDLE_MS)
      return
    }

    // First live frame closes the backfill window, even if the idle timer has not fired.
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

    /* Sort ascending by window_start before it reaches setData. The backfill is "the last
       sixty candles produced", not "the last sixty seconds": it can straddle an arbitrary
       gap, and Lightweight Charts throws on unordered data. */
    const sorted = [...this.backfillBuf].sort((a, b) => a.window_start - b.window_start)
    this.backfillBuf = []
    this.lastBackfill = sorted

    /* A reconnect replaces the previous history wholesale, so the aggregates restart with it.
       The anomaly tallies are session-scoped rather than connection-scoped and carry over —
       they describe what the backend has been doing, not what is currently on screen. */
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
    // The chart decides; the summary follows it, so the headline price can never show a bar
    // the chart declined to draw.
    const outcome: LiveOutcome = this.sink ? this.sink.onLive(bar) : 'not-drawn'
    this.accumulate(bar, outcome)
    // One setState per bar per symbol — the only React work the stream causes.
    this.cb.onSummary(this.agg)
  }

  /* Fold a bar into the running aggregates, honouring what the chart actually did with it.

     Session high/low take every bar regardless of outcome — a bar that arrived too late to
     plot still reports a price that genuinely traded. The headline figures take only bars the
     chart drew, so the number in the header always describes the candle on screen. */
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
      // A superseded duplicate replaces a candle rather than adding one.
      barCount: outcome === 'applied' ? a.barCount + 1 : a.barCount,
      lateDropped: a.lateDropped + (outcome === 'late-dropped' ? 1 : 0),
      duplicateSuperseded: a.duplicateSuperseded + (outcome === 'duplicate-superseded' ? 1 : 0),
      duplicateIgnored: a.duplicateIgnored + (outcome === 'duplicate-ignored' ? 1 : 0),
    }
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer) return
    // Exponential backoff with jitter, so three sockets do not retry in lockstep.
    const base = Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS * 2 ** Math.max(0, this.attempt - 1))
    const delay = base * (0.7 + Math.random() * 0.6)
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.open()
    }, delay)
  }
}
