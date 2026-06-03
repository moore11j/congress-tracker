"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

export type AdminToastTone = "success" | "error" | "info";

export type AdminToastMessage = {
  message: string;
  tone?: AdminToastTone;
  durationMs?: number;
};

type AdminToastState = {
  id: number;
  message: string;
  tone: AdminToastTone;
  durationMs: number;
};

export type AdminToastApi = {
  showToast: (toast: string | AdminToastMessage, tone?: AdminToastTone) => void;
};

const DEFAULT_DURATION_MS = 4000;

const toastToneClasses: Record<AdminToastTone, string> = {
  success: "border-emerald-300/40 bg-slate-950 text-emerald-100 shadow-[0_0_28px_rgba(16,185,129,0.18)]",
  error: "border-rose-300/45 bg-slate-950 text-rose-100 shadow-[0_0_28px_rgba(244,63,94,0.18)]",
  info: "border-slate-300/30 bg-slate-950 text-slate-100 shadow-[0_0_28px_rgba(148,163,184,0.14)]",
};

const toastDotClasses: Record<AdminToastTone, string> = {
  success: "bg-emerald-300",
  error: "bg-rose-300",
  info: "bg-slate-300",
};

export function useAdminToast() {
  const [toast, setToast] = useState<AdminToastState | null>(null);
  const nextId = useRef(0);

  const showToast = useCallback((input: string | AdminToastMessage, tone?: AdminToastTone) => {
    const next =
      typeof input === "string"
        ? { message: input, tone: tone ?? "success", durationMs: DEFAULT_DURATION_MS }
        : {
            message: input.message,
            tone: input.tone ?? "success",
            durationMs: input.durationMs ?? DEFAULT_DURATION_MS,
          };

    setToast({
      id: nextId.current + 1,
      message: next.message,
      tone: next.tone,
      durationMs: next.durationMs,
    });
    nextId.current += 1;
  }, []);

  const clearToast = useCallback(() => {
    setToast(null);
  }, []);

  return useMemo(() => ({ toast, showToast, clearToast }), [clearToast, showToast, toast]);
}

export function AdminToastViewport({
  toast,
  onClose,
}: {
  toast: AdminToastState | null;
  onClose: () => void;
}) {
  useEffect(() => {
    if (!toast) return;
    const timeoutId = window.setTimeout(onClose, toast.durationMs);
    return () => window.clearTimeout(timeoutId);
  }, [onClose, toast]);

  if (!toast) return null;

  const isError = toast.tone === "error";

  return (
    <div className="pointer-events-none fixed inset-x-3 top-4 z-[100] flex justify-center sm:inset-x-auto sm:right-4 sm:justify-end">
      <div
        role={isError ? "alert" : "status"}
        aria-live={isError ? "assertive" : "polite"}
        className={`pointer-events-auto flex w-full max-w-md items-start gap-3 rounded-lg border px-4 py-3 font-mono text-sm leading-5 ${toastToneClasses[toast.tone]}`}
      >
        <span className={`mt-2 h-2 w-2 shrink-0 rounded-full ${toastDotClasses[toast.tone]}`} aria-hidden="true" />
        <span className="min-w-0 flex-1 break-words">{toast.message}</span>
        <button
          type="button"
          onClick={onClose}
          className="shrink-0 rounded-md px-1.5 py-0.5 text-xs font-semibold text-current opacity-70 transition hover:bg-white/10 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-white/30"
          aria-label="Close notification"
        >
          X
        </button>
      </div>
    </div>
  );
}
