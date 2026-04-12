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
    <div className="flex items-center gap-2 rounded-lg border border-white/10 bg-white/[0.04] px-2 py-1">
      <Link href="/account/billing" prefetch={false} className="px-2 py-1 text-slate-100 transition hover:text-white">
        {label}
      </Link>
      <button
        type="button"
        onClick={() => {
          logout().finally(() => setUser(null));
        }}
        className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white"
      >
        Sign out
      </button>
    </div>
  );
}
