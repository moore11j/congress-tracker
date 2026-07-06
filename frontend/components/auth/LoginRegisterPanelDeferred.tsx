"use client";

import dynamic from "next/dynamic";

const LoginRegisterPanel = dynamic(
  () => import("@/components/auth/LoginRegisterPanel").then((module) => module.LoginRegisterPanel),
  {
    ssr: false,
    loading: () => <LoginFallback />,
  },
);

export function LoginRegisterPanelDeferred() {
  return <LoginRegisterPanel />;
}

function LoginFallback() {
  return (
    <div className="mx-auto max-w-5xl rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Account Access</p>
      <h1 className="mt-3 text-3xl font-semibold text-white">Welcome back.</h1>
      <p className="mt-2 text-sm text-slate-300">Loading account access...</p>
    </div>
  );
}
