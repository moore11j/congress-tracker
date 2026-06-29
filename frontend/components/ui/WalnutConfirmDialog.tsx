"use client";

import type { ReactNode } from "react";
import { WalnutModal } from "@/components/ui/WalnutModal";

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

export const cancelDialogButtonClass =
  "inline-flex h-10 items-center justify-center rounded-xl border border-white/10 px-4 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/20 disabled:cursor-not-allowed disabled:opacity-60";

export const successDialogButtonClass =
  "border-emerald-300/40 bg-emerald-500/15 text-emerald-100 hover:bg-emerald-500/25 focus-visible:ring-emerald-300/40";

export const dangerDialogButtonClass =
  "border-rose-300/40 bg-rose-500/10 text-rose-100 hover:bg-rose-500/20 focus-visible:ring-rose-300/40";

const neutralDialogButtonClass =
  "border-white/15 bg-white/[0.06] text-slate-100 hover:border-white/25 hover:bg-white/[0.09] focus-visible:ring-white/20";

const confirmButtonClassName: Record<DialogTone, string> = {
  success: successDialogButtonClass,
  danger: dangerDialogButtonClass,
  neutral: neutralDialogButtonClass,
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
  if (!open) return null;

  return (
    <WalnutModal
      open={open}
      title={title}
      eyebrow={eyebrow}
      description={description}
      onClose={onClose}
      closeLabel="Close dialog"
      isBusy={isBusy}
      tone={tone}
      panelClassName="max-w-md"
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            disabled={isBusy}
            className={cancelDialogButtonClass}
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
        </>
      }
    >
      {children}
    </WalnutModal>
  );
}
