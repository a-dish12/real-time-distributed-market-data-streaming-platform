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

/* the chart owns its own canvas, so it lives in a ref and bars go straight to
   series.update(). holding bar data in state would rebuild it once a second */

/** window_start is a float, the library wants whole seconds */
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
  /* update() throws on anything older, so late bars are declined against this */
  const lastTimeRef = useRef<number>(0)
  /* the series cannot be asked for it, and it breaks the tie on a repeated window */
  const lastCountRef = useRef<number>(0)

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

    /* the chart does not follow its container on its own */
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

  useEffect(() => {
    feed.setSink({
      onBackfill(bars: Bar[]) {
        const series = seriesRef.current
        const chart = chartRef.current
        if (!series || !chart) return

        /* flooring can collapse two windows onto one second and setData rejects repeats */
        const deduped = bars.filter(
          (b, i) =>
            i === bars.length - 1 ||
            Math.floor(b.window_start) !== Math.floor(bars[i + 1].window_start),
        )

        series.setData(deduped.map(toChartBar))
        const newest = deduped.length ? deduped[deduped.length - 1] : null
        lastTimeRef.current = newest ? Math.floor(newest.window_start) : 0
        lastCountRef.current = newest ? newest.count : 0
        // the only refit there is, so a zoom or pan survives the stream
        chart.timeScale().fitContent()
      },

      onLive(bar: Bar): LiveOutcome {
        const series = seriesRef.current
        if (!series) return 'not-drawn'

        /* live means arrived after your backfill, not recent, a restarted aggregator replays
           old windows still tagged live */
        const time = Math.floor(bar.window_start)

        /* reinserting it means setData() and a lost zoom, so it is declined and footered */
        if (time < lastTimeRef.current) return 'late-dropped'

        /* a replay is not always the poorer copy, the aggregator suppresses its wall clock
           backstop while draining a backlog, so `count` decides */
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
