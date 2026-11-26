import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Offline sunum: her şey (font, veri, GeoJSON, görseller) build'e gömülür; CDN yok.
export default defineConfig({
  base: './',
  plugins: [react(), tailwindcss()],
  build: {
    chunkSizeWarningLimit: 4000,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules/echarts') || id.includes('node_modules/zrender')) return 'echarts'
          if (id.includes('/src/data/')) return 'data'
          return undefined
        },
      },
    },
  },
  worker: { format: 'es' },
})
