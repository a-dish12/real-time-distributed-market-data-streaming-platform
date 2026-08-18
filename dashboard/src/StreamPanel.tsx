import { CandleChart } from './CandleChart'
import { useSymbolFeed } from './useSymbolFeed'
import type { ConnState } from './types'

const CONN_LABEL: Record<ConnState, string> = {
  connecting: 'Connecting',
  connected: 'Live',
  disconnected: 'Disconnected',
}

const fmtPrice = (n: number) => n.toFixed(2)
const fmtClock = (unixSeconds: number) =>
  new Date(unixSeconds * 1000).toLocaleTimeString('en-GB', { hour12: false })

/* everything rendered here comes from the summary snapshot, which the feed rebuilds at most
   once per bar, the bars themselves bypass React entirely */
export function StreamPanel({ symbol }: { symbol: string }) {
  const { status, attempt, summary, feed } = useSymbolFeed(symbol)
  const {
    lastClose, lastCount, lastWindowStart, lastWindowSpan,
    firstOpen, high, low, barCount,
    lateDropped, duplicateSuperseded, duplicateIgnored,
  } = summary

  const stale = status === 'disconnected'
  const hasBar = lastClose !== null
  const change = hasBar && firstOpen !== null ? lastClose - firstOpen : 0
  const pct = hasBar && firstOpen ? (change / firstOpen) * 100 : 0
  const dir = change > 0 ? 'up' : change < 0 ? 'down' : 'flat'
  // history fills at one bar per second, so a short series is a cold start and not a fault
  const cold = barCount > 0 && barCount < 8

  return (
    <section className={`sp${stale ? ' sp--fault' : ''}`}>
      <header className="sp__head">
        <div className="sp__id">
          <span className="sp__symbol">{symbol}</span>
          <span className="sp__endpoint">/ws/{symbol}</span>
        </div>

        <div className="sp__price">
          <span className={`sp__last tnum${stale ? ' sp__last--stale' : ''}`}>
            {lastClose !== null ? fmtPrice(lastClose) : '—'}
          </span>
          {hasBar && firstOpen !== null ? (
            <span className={`sp__delta tnum sp__delta--${dir}`}>
              {change >= 0 ? '+' : '−'}
              {fmtPrice(Math.abs(change))}
              <span className="sp__pct">
                {change >= 0 ? '+' : '−'}
                {Math.abs(pct).toFixed(2)}%
              </span>
            </span>
          ) : null}
        </div>

        <div className="sp__stats">
          <div className="sp__stat">
            <span className="sp__statLabel">High</span>
            <span className="sp__statValue tnum">{high !== null ? fmtPrice(high) : '—'}</span>
          </div>
          <div className="sp__stat">
            <span className="sp__statLabel">Low</span>
            <span className="sp__statValue tnum">{low !== null ? fmtPrice(low) : '—'}</span>
          </div>
        </div>

        <div className="sp__right">
          <span className={`conn conn--${status}`}>
            <span className="conn__dot" />
            {CONN_LABEL[status]}
            {status === 'connecting' && attempt > 1 ? (
              <span className="conn__meta tnum">attempt {attempt}</span>
            ) : null}
            {stale ? <span className="conn__meta">stale</span> : null}
          </span>
        </div>
      </header>

      <div className="sp__plot">
        {/* the host stays mounted so the chart is never rebuilt, the empty state is an
            overlay on top of it rather than a replacement */}
        <CandleChart feed={feed} />
        {barCount === 0 ? (
          <div className="sp__empty">
            <span className="sp__emptyTitle">
              {stale ? 'Socket closed' : 'Awaiting first bar'}
            </span>
            <span className="sp__emptyBody">
              {stale
                ? 'No frames since the socket dropped. Retrying with backoff.'
                : 'Connected. History lives in server memory and fills at one bar per second.'}
            </span>
          </div>
        ) : null}
      </div>

      <footer className="sp__foot tnum">
        <span>
          {lastWindowStart !== null ? `last bar ${fmtClock(lastWindowStart)}` : 'no bars yet'}
        </span>
        <span>{lastCount !== null ? `${lastCount} ticks` : '—'}</span>
        <span>{barCount} bars held</span>
        {cold ? <span className="sp__badge">cold start</span> : null}
        {/* three different upstream conditions, collapsing them would hide which is
            happening */}
        {lateDropped > 0 ? (
          <span className="sp__badge sp__badge--note">{lateDropped} late dropped</span>
        ) : null}
        {duplicateSuperseded > 0 ? (
          <span className="sp__badge sp__badge--note">{duplicateSuperseded} superseded</span>
        ) : null}
        {duplicateIgnored > 0 ? (
          <span className="sp__badge sp__badge--note">{duplicateIgnored} dup ignored</span>
        ) : null}
        {/* width comes off the message rather than being assumed to be 1s */}
        <span className="sp__footRight">
          window {lastWindowSpan !== null ? lastWindowSpan.toFixed(0) : '—'}s
        </span>
      </footer>
    </section>
  )
}
