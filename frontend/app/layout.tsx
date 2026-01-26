import type { ReactNode } from "react";
import Link from "next/link";
import "./globals.css";

export const metadata = {
  title: "Capitol Ledger",
  description: "Track congressional trading activity with premium market intelligence.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="h-full">
      <body className="min-h-full">
        <div className="relative min-h-screen">
          <div className="absolute inset-0 -z-10 bg-[radial-gradient(circle_at_top,_rgba(45,212,191,0.18),_transparent_45%),radial-gradient(circle_at_20%_20%,_rgba(56,189,248,0.16),_transparent_38%),linear-gradient(180deg,_rgba(15,23,42,0.2),_rgba(2,6,23,1))]" />
          <header className="border-b border-white/10 bg-slate-950/80 backdrop-blur">
            <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-5">
              <Link href="/" className="flex items-center gap-3 text-lg font-semibold">
                <span className="flex h-10 w-10 items-center justify-center rounded-2xl bg-emerald-400/15 text-emerald-200 ring-1 ring-emerald-400/30">
                  CL
                </span>
                <span>
                  Capitol Ledger
                  <span className="block text-xs font-normal text-slate-400">Congress trades intelligence</span>
                </span>
              </Link>
              <nav className="flex items-center gap-4 text-sm font-medium text-slate-200">
                <Link href="/" className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Feed
                </Link>
                <Link href="/watchlists" className="rounded-full px-3 py-1 text-slate-200 hover:text-white">
                  Watchlists
                </Link>
              </nav>
            </div>
          </header>
          <main className="mx-auto w-full max-w-6xl px-6 py-10">{children}</main>
        </div>
      </body>
    </html>
  );
}
