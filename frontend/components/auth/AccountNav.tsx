"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { ApiError, getMe, getMonitoringUnreadCount, logout, type AccountUser } from "@/lib/api";

function displayName(user: AccountUser): string {
  const name = user.name?.trim();
  if (name) return name;
  return user.email.split("@")[0] || "there";
}

export function AccountNav() {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [authUnavailable, setAuthUnavailable] = useState(false);
  const [unreadCount, setUnreadCount] = useState(0);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((response) => {
        if (!cancelled) {
          setAuthUnavailable(false);
          setUser(response.user);
        }
      })
      .catch((error) => {
        if (!cancelled && error instanceof ApiError && error.status === 401) {
          setAuthUnavailable(false);
          setUser(null);
        } else if (!cancelled) {
          setAuthUnavailable(true);
        }
      })
      .finally(() => {
        if (!cancelled) setLoaded(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!user) {
      setUnreadCount(0);
      return;
    }

    let cancelled = false;
    const loadUnread = () => {
      getMonitoringUnreadCount()
        .then((response) => {
          if (!cancelled) setUnreadCount(Math.max(Number(response.unread_count) || 0, 0));
        })
        .catch(() => {
          if (!cancelled) setUnreadCount(0);
        });
    };

    loadUnread();
    const interval = window.setInterval(loadUnread, 60_000);
    const onUpdated = () => loadUnread();
    window.addEventListener("ct:monitoring-unread-updated", onUpdated);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
      window.removeEventListener("ct:monitoring-unread-updated", onUpdated);
    };
  }, [user]);

  const label = useMemo(() => (user ? `Hello, ${displayName(user)}!` : authUnavailable ? "Account" : "Login / Register"), [authUnavailable, user]);
  const unreadLabel = unreadCount > 99 ? "99+" : String(unreadCount);

  if (!loaded || (!user && !authUnavailable)) {
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

  if (!user && authUnavailable) {
    return (
      <Link
        href="/account/billing"
        prefetch={false}
        className="rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-1.5 text-emerald-100 transition hover:bg-emerald-300/15"
      >
        {label}
      </Link>
    );
  }

  return (
    <div className="group relative z-[1100] rounded-lg border border-white/10 bg-white/[0.04] px-2 py-1">
      <Link href="/account/billing" prefetch={false} className="relative block px-2 py-1 pr-5 text-slate-100 transition hover:text-white">
        {label}
        {unreadCount > 0 ? (
          <span className="absolute -right-1 -top-1 inline-flex min-h-5 min-w-5 items-center justify-center rounded-full bg-red-500 px-1 text-[10px] font-bold leading-none text-white shadow-lg shadow-red-950/40">
            {unreadLabel}
          </span>
        ) : null}
      </Link>
      <div className="invisible absolute right-0 top-full z-[1100] min-w-40 pt-2 opacity-0 transition group-hover:visible group-hover:opacity-100 group-focus-within:visible group-focus-within:opacity-100">
        <div className="rounded-lg border border-white/10 bg-slate-950/95 p-1 shadow-xl shadow-slate-950/40 backdrop-blur">
          <Link
            href="/monitoring"
            prefetch={false}
            className="flex items-center justify-between gap-4 rounded-md px-3 py-2 text-sm text-slate-200 transition hover:bg-white/[0.06] hover:text-white"
          >
            <span>Inbox</span>
            {unreadCount > 0 ? (
              <span className="inline-flex min-w-5 items-center justify-center rounded-full bg-red-500/15 px-1.5 py-0.5 text-xs font-bold text-red-200">
                {unreadLabel}
              </span>
            ) : null}
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
