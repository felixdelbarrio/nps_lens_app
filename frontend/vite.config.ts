import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { configDefaults } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  build: {
    // Plotly is lazy-loaded into its own async chunk on purpose. Raising the threshold
    // avoids a noisy warning without changing the runtime split strategy.
    chunkSizeWarningLimit: 5000
  },
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8000"
    }
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test-setup.ts",
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
    exclude: [...configDefaults.exclude, "tests/**"]
  }
});
