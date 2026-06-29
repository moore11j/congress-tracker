"use client";

import { useEffect, useId, useRef, useState, useTransition } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import {
  cancelDialogButtonClass,
  successDialogButtonClass,
} from "@/components/ui/WalnutConfirmDialog";
import { WalnutModal } from "@/components/ui/WalnutModal";
import { createWatchlist } from "@/lib/api";
import { formatInteger } from "@/lib/accountDisplay";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { inputClassName } from "@/lib/styles";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  open: boolean;
  onClose: () => void;
  onCreated?: (created: WatchlistSummary) => Promise<void> | void;
  watchlistCount: number;
  entitlements?: Entitlements;
  defaultName: string;
  pendingTickerSymbol?: string | null;
};

export function WatchlistCreateForm({
  open,
  onClose,
  onCreated,
  watchlistCount,
  entitlements = defaultEntitlements,
  defaultName,
  pendingTickerSymbol = null,
}: Props) {
  const formId = useId();
  const [name, setName] = useState(defaultName);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const lastDefaultNameRef = useRef(defaultName);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    setName((current) => {
      const shouldUseNextDefault = !current.trim() || current === lastDefaultNameRef.current;
      lastDefaultNameRef.current = defaultName;
      return shouldUseNextDefault ? defaultName : current;
    });
  }, [defaultName]);

  useEffect(() => {
    if (!open) {
      setError(null);
    }
  }, [open]);

  const handleClose = () => {
    if (isPending) return;
    setName(defaultName);
    setError(null);
    onClose();
  };

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) {
      setError("Enter a watchlist name.");
      return;
    }

    const limit = limitFor(entitlements, "watchlists");
    if (!hasEntitlement(entitlements, "watchlists")) {
      setError("Watchlist creation is currently a Premium feature.");
      return;
    }
    if (watchlistCount >= limit) {
      setError(`Free accounts can keep ${formatInteger(limit)} watchlists. Upgrade to create more.`);
      return;
    }

    setError(null);
    startTransition(async () => {
      try {
        const created = await createWatchlist(trimmed);
        await onCreated?.(created);
        setName(defaultName);
        setError(null);
      } catch (err) {
        const message = err instanceof Error ? err.message : "";
        setError(
          message.includes("premium_required") || message.includes("Free accounts")
            ? `Free accounts can keep ${formatInteger(limitFor(entitlements, "watchlists"))} watchlists. Upgrade to create more.`
            : message || "Unable to create watchlist.",
        );
      }
    });
  };

  return (
    <WalnutModal
      open={open}
      title="Name your watchlist"
      description="Organize tickers you want to monitor closely."
      onClose={handleClose}
      closeLabel="Cancel create watchlist"
      isBusy={isPending}
      allowEscapeClose={false}
      initialFocusRef={inputRef}
      tone="success"
      panelClassName="max-w-md"
      footer={
        <>
          <button
            type="button"
            className={cancelDialogButtonClass}
            disabled={isPending}
            onClick={handleClose}
          >
            Cancel
          </button>
          <button
            type="submit"
            form={formId}
            className={`inline-flex h-10 items-center justify-center rounded-xl border px-4 text-sm font-semibold transition focus-visible:outline-none focus-visible:ring-2 disabled:cursor-not-allowed disabled:opacity-60 ${successDialogButtonClass}`}
            disabled={isPending}
          >
            {isPending ? "Creating..." : "OK"}
          </button>
        </>
      }
    >
      <form id={formId} onSubmit={handleSubmit} className="space-y-3">
        <input
          ref={inputRef}
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="Watchlist name"
          className={inputClassName}
        />
        {pendingTickerSymbol ? (
          <p className="text-sm text-emerald-100">Creating this watchlist will add {pendingTickerSymbol}.</p>
        ) : null}
        {error ? <p className="text-sm text-rose-300">{error}</p> : null}
        {!hasEntitlement(entitlements, "watchlists") || watchlistCount >= limitFor(entitlements, "watchlists") ? (
          <UpgradePrompt
            title="More watchlists are a Premium workflow"
            body={
              hasEntitlement(entitlements, "watchlists")
                ? `Free includes ${formatInteger(limitFor(entitlements, "watchlists"))} watchlists so the core monitoring flow stays useful.`
                : "Watchlist creation is currently a Premium feature."
            }
            compact={true}
          />
        ) : null}
      </form>
    </WalnutModal>
  );
}
