import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// https://vite.dev/config/
export default defineConfig(({ mode }) => ({
  plugins: [react(), tailwindcss()],
  server:
    mode === "development"
      ? {
          proxy: {
            "/api": {
              target: "http://localhost:8080",
              changeOrigin: true,
              secure: false,
            },
          },
        }
      : undefined, // No proxy in production
}));
