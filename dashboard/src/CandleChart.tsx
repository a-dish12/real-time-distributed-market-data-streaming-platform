import { useEffect, useRef } from 'react'
import {
  createChart,
  CandlestickSeries,
  type CandlestickData,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from 'lightweight-charts'
import { cssColor } from './color'
import type { LiveOutcome, SymbolFeed } from './feed'
import type { Bar } from './types'

/* ────────────────────────────────────────────────────────────────────────────────────────
   WHY THE CHART LIVES IN A REF AND NOT IN STATE

   Lightweight Charts creates and owns its own canvas. React cannot render into it and must
   not try to manage its contents. So:

     - createChart() and addSeries() run ONCE, in the effect with an empty dependency array
       below, and are disposed in that effect's cleanup.
     - Backfill is applied with series.setData(), once per connection, after sorting.
     - Every later bar is applied with series.update(), called straight on the ref.

   If bar data went into useState instead, this component would re-render on every message —
   once a second per chart, three charts — and the chart would be torn down and rebuilt each
   time. The symptom is a chart that flickers, drops frames, and loses zoom and pan position.

   This is the part a future reader is most likely to "clean up" into useState. Don't.
   ──────────────────────────────────────────────────────────────────────────────────────── */

/** window_start is Unix seconds as a float; the library wants integer seconds. */
function toChartBar(bar: Bar): CandlestickData<UTCTimestamp> {
  return {
    time: Math.floor(bar.window_start) as UTCTimestamp,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  }
}

export function CandleChart({ feed }: { feed: SymbolFeed }) {
  const hostRef = useRef<HTMLDivElement | null>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  /* The newest bar time applied to the series. update() rejects anything older, so this is
     what lets us detect a late bar and decline it deliberately instead of throwing. */
  const lastTimeRef = useRef<number>(0)
  /* The tick count of the candle currently drawn at lastTimeRef. The series itself cannot be
     asked — update() only ever received OHLC and a time — so the count is tracked here
     alongside it. It is the tiebreak when the same window is published twice. */
  const lastCountRef = useRef<number>(0)

  // ── Chart lifecycle: created once, disposed once. Never keyed on data. ──
  useEffect(() => {
    const host = hostRef.current
    if (!host) return

    const c = {
      up: cssColor('--price-up'),
      down: cssColor('--price-down'),
      plot: cssColor('--surface-plot'),
      grid: cssColor('--chart-grid'),
      axis: cssColor('--chart-axis-text'),
      border: cssColor('--border-hairline'),
    }

    const chart = createChart(host, {
      layout: {
        background: { color: c.plot },
        textColor: c.axis,
        fontFamily: '"Geist Mono", ui-monospace, monospace',
        fontSize: 10,
        attributionLogo: false,
      },
      grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
      rightPriceScale: { borderColor: c.border, scaleMargins: { top: 0.12, bottom: 0.12 } },
      timeScale: {
        borderColor: c.border,
        timeVisible: true,
        secondsVisible: true,
        rightOffset: 2,
      },
      crosshair: {
        mode: 0,
        vertLine: { color: c.axis, width: 1, style: 3, labelBackgroundColor: c.grid },
        horzLine: { color: c.axis, width: 1, style: 3, labelBackgroundColor: c.grid },
      },
    })

    // v5 API: series definitions are passed to addSeries (v4's addCandlestickSeries is gone).
    const series = chart.addSeries(CandlestickSeries, {
      upColor: c.up,
      downColor: c.down,
      borderVisible: false,
      wickUpColor: c.up,
      wickDownColor: c.down,
      priceLineColor: c.axis,
      priceLineStyle: 2,
    })

    chartRef.current = chart
    seriesRef.current = series

    /* Lightweight Charts does not follow its container on its own. The observer is created
       in this same effect and disconnected in its cleanup, so it can never outlive the chart
       it is resizing. */
    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect
      if (width > 0 && height > 0) {
        chart.applyOptions({ width: Math.floor(width), height: Math.floor(height) })
      }
    })
    ro.observe(host)

    return () => {
      ro.disconnect()
      chart.remove()
      chartRef.current = null
      seriesRef.current = null
      lastTimeRef.current = 0
      lastCountRef.current = 0
    }
  }, [])

  // ── Sink registration: bars arrive here and go straight to the series ref. ──
  useEffect(() => {
    feed.setSink({
      onBackfill(bars: Bar[]) {
        const series = seriesRef.current
        const chart = chartRef.current
        if (!series || !chart) return

        /* Already sorted ascending by the feed. Flooring to whole seconds can collapse two
           window_starts onto one timestamp, and setData rejects repeated times, so drop all
           but the last row for any given second — the later one is the more recent bar.
           Deduping the Bars rather than the mapped rows keeps `count` reachable below. */
        const deduped = bars.filter(
          (b, i) =>
            i === bars.length - 1 ||
            Math.floor(b.window_start) !== Math.floor(bars[i + 1].window_start),
        )

        series.setData(deduped.map(toChartBar))
        const newest = deduped.length ? deduped[deduped.length - 1] : null
        lastTimeRef.current = newest ? Math.floor(newest.window_start) : 0
        lastCountRef.current = newest ? newest.count : 0
        // The only automatic viewport change there is. Live updates never refit, which is
        // what lets a zoom or pan survive the incoming stream.
        chart.timeScale().fitContent()
      },

      onLive(bar: Bar): LiveOutcome {
        const series = seriesRef.current
        // The chart effect above runs before this sink is registered, so the series exists by
        // the time any bar can arrive. Guarded anyway: nothing drawn, nothing tallied.
        if (!series) return 'not-drawn'

        /* Candles are positioned by window_start, never by arrival order. "live" only means
           "arrived after your backfill": a restarted aggregator replays a burst of old
           windows still tagged live. */
        const time = Math.floor(bar.window_start)

        /* Older than what is drawn. update() throws on this, so it is declined and counted
           rather than allowed to throw. Reinserting it would mean rebuilding the series with
           setData(), which resets zoom and pan — too high a price for a candle that has
           already scrolled into history. The drop is surfaced in the footer, not swallowed. */
        if (time < lastTimeRef.current) return 'late-dropped'

        /* Same window published twice. The aggregator can re-emit a sealed window after a
           restart, and the replay is not always the poorer copy: its wall-clock backstop is
           suppressed while it drains a backlog, so a replayed window can include a tick the
           live pass force-sealed past. `count` is the only completeness signal on the wire,
           so first wins unless the newcomer is strictly more complete. */
        if (time === lastTimeRef.current) {
          if (bar.count <= lastCountRef.current) return 'duplicate-ignored'
          series.update(toChartBar(bar))
          lastCountRef.current = bar.count
          return 'duplicate-superseded'
        }

        series.update(toChartBar(bar))
        lastTimeRef.current = time
        lastCountRef.current = bar.count
        return 'applied'
      },
    })

    return () => feed.setSink(null)
  }, [feed])

  return <div ref={hostRef} className="sp__plotHost" />
}
