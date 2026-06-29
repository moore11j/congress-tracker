"use client";

import { useEffect, useId, useRef, type ReactNode, type RefObject } from "react";
import { createPortal } from "react-dom";

type ModalTone = "neutral" | "success" | "danger" | "warning";

type Props = {
  open: boolean;
  title: string;
  eyebrow?: string;
  description?: ReactNode;
  children?: ReactNode;
  footer?: ReactNode;
  onClose: () => void;
  closeLabel?: string;
  isBusy?: boolean;
  allowEscapeClose?: boolean;
  initialFocusRef?: RefObject<HTMLElement | null>;
  tone?: ModalTone;
  labelledById?: string;
  className?: string;
  panelClassName?: string;
};

const eyebrowClassName: Record<ModalTone, string> = {
  neutral: "text-slate-300",
  success: "text-emerald-200",
  danger: "text-rose-200",
  warning: "text-amber-200",
};

const focusableSelector = [
  "a[href]",
  "button:not([disabled])",
  "textarea:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

function focusableElements(root: HTMLElement | null): HTMLElement[] {
  if (!root) return [];
  return Array.from(root.querySelectorAll<HTMLElement>(focusableSelector)).filter(
    (element) => !element.hasAttribute("disabled") && element.getAttribute("aria-hidden") !== "true",
  );
}

export function WalnutModal({
  open,
  title,
  eyebrow,
  description,
  children,
  footer,
  onClose,
  closeLabel = "Close",
  isBusy = false,
  allowEscapeClose = true,
  initialFocusRef,
  tone = "neutral",
  labelledById,
  className = "",
  panelClassName = "",
}: Props) {
  const generatedTitleId = useId();
  const descriptionId = useId();
  const titleId = labelledById || generatedTitleId;
  const panelRef = useRef<HTMLDivElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);

  useEffect(() => {
    if (!open) return undefined;
    const previousActiveElement = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    window.setTimeout(() => {
      const preferred = initialFocusRef?.current;
      if (preferred) {
        preferred.focus();
        return;
      }
      closeButtonRef.current?.focus();
    }, 0);

    return () => {
      document.body.style.overflow = previousOverflow;
      previousActiveElement?.focus();
    };
  }, [initialFocusRef, open]);

  useEffect(() => {
    if (!open) return undefined;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        if (allowEscapeClose && !isBusy) onClose();
        return;
      }

      if (event.key !== "Tab") return;
      const focusable = focusableElements(panelRef.current);
      if (focusable.length === 0) {
        event.preventDefault();
        panelRef.current?.focus();
        return;
      }

      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [allowEscapeClose, isBusy, onClose, open]);

  if (!open || typeof document === "undefined") return null;

  return createPortal(
    <div
      className={`fixed inset-0 z-[5000] flex items-start justify-center overflow-y-auto overscroll-contain bg-slate-950/75 px-3 pb-4 pt-[calc(var(--app-header-height,64px)_+_env(safe-area-inset-top)_+_12px)] backdrop-blur-sm sm:items-center sm:px-4 sm:py-8 ${className}`}
      role="presentation"
    >
      <div
        ref={panelRef}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={description ? descriptionId : undefined}
        className={`w-[min(calc(100vw_-_24px),520px)] max-w-full overflow-y-auto rounded-2xl border border-white/10 bg-slate-900/95 p-5 text-left text-slate-100 shadow-2xl shadow-black/50 outline-none ring-1 ring-white/[0.03] max-h-[calc(100dvh_-_var(--app-header-height,64px)_-_env(safe-area-inset-top)_-_32px)] sm:max-h-[calc(100dvh_-_4rem)] ${panelClassName}`}
      >
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            {eyebrow ? (
              <p className={`text-xs font-semibold uppercase tracking-[0.24em] ${eyebrowClassName[tone]}`}>{eyebrow}</p>
            ) : null}
            <h2 id={titleId} className={`${eyebrow ? "mt-2" : ""} text-lg font-semibold text-white`}>
              {title}
            </h2>
          </div>
          <button
            ref={closeButtonRef}
            type="button"
            className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-white/10 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/25 disabled:cursor-not-allowed disabled:opacity-60"
            onClick={() => {
              if (!isBusy) onClose();
            }}
            disabled={isBusy}
            aria-label={closeLabel}
          >
            X
          </button>
        </div>
        {description ? (
          <div id={descriptionId} className="mt-2 text-sm leading-6 text-slate-300">
            {description}
          </div>
        ) : null}
        {children ? <div className="mt-4">{children}</div> : null}
        {footer ? <div className="mt-5 flex flex-wrap justify-end gap-3">{footer}</div> : null}
      </div>
    </div>,
    document.body,
  );
}
