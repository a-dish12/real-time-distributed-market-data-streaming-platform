import { useEffect, useRef, useState } from 'react'
import { SymbolFeed, EMPTY_SUMMARY, type Summary } from './feed'
import type { ConnState } from './types'

/* The whole React state boundary for a symbol lives here, and it is deliberately small:
   connection status, retry attempt, and a summary snapshot rebuilt at most once per bar.
   Bars themselves go straight from the socket to the chart's series ref and are never
   retained in state — see CandleChart.tsx for why that matters. */
export function useSymbolFeed(symbol: string) {
  const [status, setStatus] = useState<ConnState>('connecting')
  const [attempt, setAttempt] = useState(0)
  const [summary, setSummary] = useState<Summary>(EMPTY_SUMMARY)

  // Created once per symbol and kept stable, so the chart's sink registration survives
  // re-renders (and StrictMode's deliberate double-mount in development).
  const feedRef = useRef<SymbolFeed | null>(null)
  if (feedRef.current === null) {
    feedRef.current = new SymbolFeed(symbol, {
      onStatus: (state, n) => {
        setStatus(state)
        setAttempt(n)
      },
      onSummary: setSummary,
    })
  }

  useEffect(() => {
    const feed = feedRef.current!
    feed.start()
    return () => feed.stop()
  }, [symbol])

  return { status, attempt, summary, feed: feedRef.current }
}
