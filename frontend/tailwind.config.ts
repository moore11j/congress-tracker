import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      boxShadow: {
        card: "0 24px 80px rgba(15, 23, 42, 0.35)",
      },
    },
  },
  plugins: [],
};

export default config;
