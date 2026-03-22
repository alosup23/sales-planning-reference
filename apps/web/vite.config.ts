import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
  },
  build: {
    // The planning grid is lazy-loaded, so the AG Grid vendor bundle is intentionally
    // larger than the shell. Keep the warning threshold high enough to avoid noisy
    // false positives while still flagging unexpected growth elsewhere.
    chunkSizeWarningLimit: 1800,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules/ag-grid-community")) {
            return "ag-grid-community";
          }

          if (id.includes("node_modules/ag-grid-react")) {
            return "ag-grid-react";
          }

          if (id.includes("node_modules/@tanstack/react-query")) {
            return "query";
          }

          if (id.includes("node_modules/react") || id.includes("node_modules/scheduler")) {
            return "react-vendor";
          }
        },
      },
    },
  },
});
