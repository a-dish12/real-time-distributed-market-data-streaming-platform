import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// This file is the ONLY place the backend's dev-time host may be named.
//
// In production uvicorn serves the built bundle from backend/static, so the page and the
// WebSocket share an origin and window.location resolves correctly on its own. In dev, Vite
// serves on 5173 while uvicorn is on 8000, so window.location would point at Vite. The proxy
// below closes that gap: application code always builds its URL from window.location and
// hits /ws, and in dev that request is forwarded here. `ws: true` is what makes the proxy
// perform the WebSocket upgrade rather than treating /ws as a plain HTTP route.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/ws': {
        target: 'ws://localhost:8000',
        ws: true,
        changeOrigin: true,
      },
    },
  },
  build: {
    // uvicorn mounts this directory; see backend/webserver.py
    outDir: '../backend/static',
    emptyOutDir: true,
  },
})
