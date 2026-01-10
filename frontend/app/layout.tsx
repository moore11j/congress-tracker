import type { ReactNode } from "react";

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, fontFamily: "ui-sans-serif, system-ui, -apple-system" }}>
        <div style={{ padding: 16, maxWidth: 1100, margin: "0 auto" }}>
          <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <a href="/" style={{ textDecoration: "none", color: "inherit", fontWeight: 700 }}>
              CapitolLedger
            </a>
            <nav style={{ display: "flex", gap: 12 }}>
              <a href="/feed">Feed</a>
            </nav>
          </header>
          {children}
        </div>
      </body>
    </html>
  );
}
