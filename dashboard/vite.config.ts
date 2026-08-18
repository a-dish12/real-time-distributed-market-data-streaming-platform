import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// the only place the backend's host may be named, and only for dev. in production uvicorn
// serves the bundle itself so window.location already points at the right place, in dev vite
// is on 5173 and uvicorn on 8000, so /ws is proxied across
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
    // uvicorn mounts this, and the Dockerfile's dashboard stage relies on the relative path
    outDir: '../backend/static',
    emptyOutDir: true,
  },
})
