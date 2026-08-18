import { useEffect, useRef, useState } from 'react'
import { SymbolFeed, EMPTY_SUMMARY, type Summary } from './feed'
import type { ConnState } from './types'

/* the whole React state boundary for a symbol, kept small on purpose: status, attempt and a
   summary rebuilt at most once per bar. bars never enter state, see CandleChart.tsx */
export function useSymbolFeed(symbol: string) {
  const [status, setStatus] = useState<ConnState>('connecting')
  const [attempt, setAttempt] = useState(0)
  const [summary, setSummary] = useState<Summary>(EMPTY_SUMMARY)

  // created once and kept stable so the chart's sink survives re-renders and StrictMode's
  // double mount
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
