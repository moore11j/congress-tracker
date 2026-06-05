"use client";

import { useEffect, useId, type ReactNode } from "react";

type DialogTone = "success" | "danger" | "neutral";

type Props = {
  open: boolean;
  eyebrow?: string;
  title: string;
  description?: ReactNode;
  confirmLabel: string;
  cancelLabel?: string;
  tone?: DialogTone;
  onConfirm: () => void | Promise<void>;
  onClose: () => void;
  isBusy?: boolean;
  children?: ReactNode;
  confirmDisabled?: boolean;
};

const eyebrowClassName: Record<DialogTone, string> = {
  success: "text-emerald-200",
  danger: "text-rose-200",
  neutral: "text-slate-300",
};

const confirmButtonClassName: Record<DialogTone, string> = {
  success:
    "border-emerald-300/40 bg-emerald-500/15 text-emerald-100 hover:bg-emerald-500/25 focus-visible:ring-emerald-300/40",
  danger:
    "border-rose-300/40 bg-rose-500/10 text-rose-100 hover:bg-rose-500/20 focus-visible:ring-rose-300/40",
  neutral:
    "border-white/15 bg-white/[0.06] text-slate-100 hover:border-white/25 hover:bg-white/[0.09] focus-visible:ring-white/20",
};

export function WalnutConfirmDialog({
  open,
  eyebrow,
  title,
  description,
  confirmLabel,
  cancelLabel = "Cancel",
  tone = "neutral",
  onConfirm,
  onClose,
  isBusy = false,
  children,
  confirmDisabled = false,
}: Props) {
  const titleId = useId();
  const descriptionId = useId();

  useEffect(() => {
    if (!open) return undefined;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !isBusy) {
        onClose();
      }
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [isBusy, onClose, open]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/75 px-4 py-6 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-labelledby={titleId}
      aria-describedby={description ? descriptionId : undefined}
      onClick={() => {
        if (!isBusy) onClose();
      }}
    >
      <div
        className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900/95 p-5 text-slate-100 shadow-2xl shadow-black/50"
        onClick={(event) => event.stopPropagation()}
      >
        {eyebrow ? (
          <p className={`text-xs font-semibold uppercase tracking-[0.24em] ${eyebrowClassName[tone]}`}>{eyebrow}</p>
        ) : null}
        <h2 id={titleId} className={eyebrow ? "mt-2 text-lg font-semibold text-white" : "text-lg font-semibold text-white"}>
          {title}
        </h2>
        {description ? (
          <div id={descriptionId} className="mt-2 text-sm leading-6 text-slate-300">
            {description}
          </div>
        ) : null}
        {children ? <div className="mt-4">{children}</div> : null}
        <div className="mt-5 flex flex-wrap justify-end gap-3">
          <button
            type="button"
            onClick={onClose}
            disabled={isBusy}
            className="inline-flex h-10 items-center justify-center rounded-xl border border-white/10 px-4 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/20 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {cancelLabel}
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={isBusy || confirmDisabled}
            className={`inline-flex h-10 items-center justify-center rounded-xl border px-4 text-sm font-semibold transition focus-visible:outline-none focus-visible:ring-2 disabled:cursor-not-allowed disabled:opacity-60 ${confirmButtonClassName[tone]}`}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
