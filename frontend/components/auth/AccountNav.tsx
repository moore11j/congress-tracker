"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { getMe, logout, type AccountUser } from "@/lib/api";

function displayName(user: AccountUser): string {
  const name = user.name?.trim();
  if (name) return name;
  return user.email.split("@")[0] || "there";
}

export function AccountNav() {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((response) => {
        if (!cancelled) setUser(response.user);
      })
      .catch(() => {
        if (!cancelled) setUser(null);
      })
      .finally(() => {
        if (!cancelled) setLoaded(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const label = useMemo(() => (user ? `Hello, ${displayName(user)}!` : "Login / Register"), [user]);

  if (!loaded || !user) {
    return (
      <Link
        href="/login"
        prefetch={false}
        className="rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-1.5 text-emerald-100 transition hover:bg-emerald-300/15"
      >
        {label}
      </Link>
    );
  }

  return (
    <div className="group relative rounded-lg border border-white/10 bg-white/[0.04] px-2 py-1">
      <Link href="/account/billing" prefetch={false} className="block px-2 py-1 text-slate-100 transition hover:text-white">
        {label}
      </Link>
      <div className="invisible absolute right-0 top-full z-50 min-w-40 pt-2 opacity-0 transition group-hover:visible group-hover:opacity-100 group-focus-within:visible group-focus-within:opacity-100">
        <div className="rounded-lg border border-white/10 bg-slate-950/95 p-1 shadow-xl shadow-slate-950/40 backdrop-blur">
          <Link
            href="/monitoring"
            prefetch={false}
            className="block rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            Inbox
          </Link>
          <Link
            href="/account/settings"
            prefetch={false}
            className="block rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            Settings
          </Link>
          <button
            type="button"
            onClick={() => {
              logout().finally(() => {
                setUser(null);
                window.location.replace("/login");
              });
            }}
            className="block w-full rounded-md px-3 py-2 text-left text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            Logout
          </button>
        </div>
      </div>
    </div>
  );
}
