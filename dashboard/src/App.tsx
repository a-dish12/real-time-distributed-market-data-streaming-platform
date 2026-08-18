import { StreamPanel } from './StreamPanel'

/* one socket per symbol, there is no combined endpoint */
const SYMBOLS = ['AAPL', 'MSFT', 'TSLA'] as const

export default function App() {
  return (
    <div className="app">
      <header className="app__bar">
        <span className="app__mark">TAPE</span>
        <span className="app__sub">per-second OHLC · kafka → websocket</span>
      </header>
      <main className="app__main">
        {SYMBOLS.map((s) => (
          <StreamPanel key={s} symbol={s} />
        ))}
      </main>
    </div>
  )
}
