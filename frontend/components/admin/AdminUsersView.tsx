"use client";

import { useEffect, useMemo, useState } from "react";
import {
  adminBatchUpdateUsers,
  adminClearUserPriceOverride,
  adminDeleteUser,
  adminSetPremium,
  adminSetUserPriceOverride,
  adminSuspendUser,
  downloadAdminUsers,
  getAdminUsers,
  type AccountUser,
  type AdminUserAdminFilter,
  type AdminUserPlanFilter,
  type AdminUserSortBy,
  type AdminUserSortDir,
  type AdminUsersResponse,
} from "@/lib/api";

const STATUS_OPTIONS = [
  { value: "", label: "All" },
  { value: "active", label: "Active" },
  { value: "suspended", label: "Suspended" },
  { value: "trialing", label: "Trialing" },
  { value: "past_due", label: "Past due" },
  { value: "payment_failed", label: "Payment failed" },
  { value: "canceled", label: "Canceled" },
  { value: "incomplete", label: "Incomplete" },
];

const SORT_OPTIONS: Array<{ value: AdminUserSortBy; label: string }> = [
  { value: "created_at", label: "Registered date" },
  { value: "last_seen_at", label: "Last active" },
  { value: "email", label: "Email" },
  { value: "name", label: "Name" },
  { value: "country", label: "Country" },
  { value: "plan", label: "Plan" },
  { value: "status", label: "Status" },
];

function formatDate(value?: string | null) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function compactStatus(value?: string | null) {
  return (value || "active").replaceAll("_", " ");
}

function displayName(user: AccountUser) {
  const full = [user.first_name, user.last_name].filter(Boolean).join(" ").trim();
  return user.name || full || "-";
}

function displayPlan(user: AccountUser) {
  return user.plan || user.manual_tier_override || user.entitlement_tier || user.subscription_plan || "free";
}

function saveBlob(blob: Blob, filename: string) {
  const href = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(href);
}

export function AdminUsersView() {
  const [plan, setPlan] = useState<AdminUserPlanFilter>("all");
  const [statusFilter, setStatusFilter] = useState("");
  const [country, setCountry] = useState("");
  const [adminFilter, setAdminFilter] = useState<AdminUserAdminFilter>("all");
  const [sortBy, setSortBy] = useState<AdminUserSortBy>("created_at");
  const [sortDir, setSortDir] = useState<AdminUserSortDir>("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [users, setUsers] = useState<AdminUsersResponse | null>(null);
  const [busy, setBusy] = useState(false);
  const [exporting, setExporting] = useState<"xlsx" | "pdf" | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [overrideDraft, setOverrideDraft] = useState({ monthly: "", annual: "", currency: "USD", note: "" });

  const query = useMemo(
    () => ({
      plan,
      status: statusFilter || undefined,
      country,
      admin: adminFilter,
      sort_by: sortBy,
      sort_dir: sortDir,
      page,
      page_size: pageSize,
    }),
    [adminFilter, country, page, pageSize, plan, sortBy, sortDir, statusFilter],
  );

  useEffect(() => {
    let ignore = false;
    const load = async () => {
      setBusy(true);
      setStatus(null);
      try {
        const next = await getAdminUsers(query);
        if (!ignore) setUsers(next);
      } catch (error) {
        if (!ignore) setStatus(error instanceof Error ? error.message : "Unable to load users.");
      } finally {
        if (!ignore) setBusy(false);
      }
    };
    load();
    return () => {
      ignore = true;
    };
  }, [query]);

  const resetPage = () => setPage(1);

  const refreshUsers = async () => {
    setBusy(true);
    try {
      setUsers(await getAdminUsers(query));
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to refresh users.");
    } finally {
      setBusy(false);
    }
  };

  const exportUsers = async (format: "xlsx" | "pdf") => {
    setExporting(format);
    setStatus(null);
    try {
      const { blob, filename } = await downloadAdminUsers(format, {
        ...query,
        page: undefined,
        page_size: undefined,
      });
      saveBlob(blob, filename);
      setStatus(`${format.toUpperCase()} export ready.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : `Unable to export ${format.toUpperCase()}.`);
    } finally {
      setExporting(null);
    }
  };

  const setPremium = async (user: AccountUser, tier: "free" | "premium" | "pro" | null) => {
    setBusy(true);
    try {
      await adminSetPremium(user.id, tier);
      await refreshUsers();
      setStatus(tier ? `${user.email} set to ${tier}.` : `${user.email} manual override cleared.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update user.");
    } finally {
      setBusy(false);
    }
  };

  const overridePayload = () => {
    const monthly = overrideDraft.monthly.trim() ? Math.round(Number(overrideDraft.monthly) * 100) : null;
    const annual = overrideDraft.annual.trim() ? Math.round(Number(overrideDraft.annual) * 100) : null;
    if ((monthly !== null && (!Number.isFinite(monthly) || monthly < 0)) || (annual !== null && (!Number.isFinite(annual) || annual < 0))) {
      throw new Error("Enter non-negative override prices.");
    }
    return {
      monthly_price_override: monthly,
      annual_price_override: annual,
      override_currency: overrideDraft.currency || "USD",
      override_note: overrideDraft.note,
    };
  };

  const setPriceOverride = async (user: AccountUser) => {
    setBusy(true);
    try {
      await adminSetUserPriceOverride(user.id, overridePayload());
      await refreshUsers();
      setStatus(`Billing override metadata saved for ${user.email}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to save price override.");
    } finally {
      setBusy(false);
    }
  };

  const clearPriceOverride = async (user: AccountUser) => {
    setBusy(true);
    try {
      await adminClearUserPriceOverride(user.id);
      await refreshUsers();
      setStatus(`Billing override metadata cleared for ${user.email}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to clear price override.");
    } finally {
      setBusy(false);
    }
  };

  const suspend = async (user: AccountUser, suspended: boolean) => {
    if (suspended && !window.confirm(`Suspend ${user.email}?`)) return;
    setBusy(true);
    try {
      await adminSuspendUser(user.id, suspended);
      await refreshUsers();
      setStatus(suspended ? `${user.email} suspended.` : `${user.email} unsuspended.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update suspension.");
    } finally {
      setBusy(false);
    }
  };

  const deleteUser = async (user: AccountUser) => {
    if (!window.confirm(`Delete ${user.email}? This removes the account record.`)) return;
    setBusy(true);
    try {
      await adminDeleteUser(user.id);
      await refreshUsers();
      setStatus(`${user.email} deleted.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to delete user.");
    } finally {
      setBusy(false);
    }
  };

  const batchUpdate = async (action: "premium" | "pro" | "free" | "suspend" | "unsuspend" | "override" | "clear_override") => {
    if (selectedIds.length === 0) return;
    if ((action === "free" || action === "suspend") && !window.confirm(`${action === "free" ? "Downgrade" : "Suspend"} ${selectedIds.length} selected users?`)) return;
    const payload: Parameters<typeof adminBatchUpdateUsers>[0] = { user_ids: selectedIds };
    if (action === "premium" || action === "pro" || action === "free") payload.tier = action;
    if (action === "suspend") payload.suspended = true;
    if (action === "unsuspend") payload.suspended = false;
    if (action === "clear_override") payload.clear_price_override = true;
    if (action === "override") {
      try {
        payload.price_override = overridePayload();
      } catch (error) {
        setStatus(error instanceof Error ? error.message : "Enter non-negative override prices.");
        return;
      }
    }
    setBusy(true);
    try {
      const result = await adminBatchUpdateUsers(payload);
      await refreshUsers();
      setSelectedIds([]);
      setStatus(`Batch update complete for ${result.updated} users.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to run batch update.");
    } finally {
      setBusy(false);
    }
  };

  const totalPages = users?.total_pages ?? 1;
  const rows = users?.items ?? [];
  const selectedCount = selectedIds.length;
  const allVisibleSelected = rows.length > 0 && rows.every((user) => selectedIds.includes(user.id));

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Users</p>
          <h2 className="mt-1 text-xl font-semibold text-white">Registered accounts</h2>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Account access, subscription state, billing location, and admin controls.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => exportUsers("xlsx")}
            disabled={busy || exporting !== null}
            className="rounded-lg border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-60"
          >
            {exporting === "xlsx" ? "Exporting XLSX" : "Export XLSX"}
          </button>
          <button
            type="button"
            onClick={() => exportUsers("pdf")}
            disabled={busy || exporting !== null}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-60"
          >
            {exporting === "pdf" ? "Exporting PDF" : "Export PDF"}
          </button>
          <button
            type="button"
            onClick={refreshUsers}
            disabled={busy}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-60"
          >
            Refresh
          </button>
        </div>
      </div>

      <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-7">
        <label className="text-sm">
          <span className="block font-medium text-slate-200">Plan</span>
          <select
            value={plan}
            onChange={(event) => {
              setPlan(event.target.value as AdminUserPlanFilter);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            <option value="all">All</option>
            <option value="free">Free</option>
            <option value="premium">Premium</option>
            <option value="pro">Pro</option>
            <option value="admin">Admin</option>
          </select>
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Status</span>
          <select
            value={statusFilter}
            onChange={(event) => {
              setStatusFilter(event.target.value);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            {STATUS_OPTIONS.map((option) => (
              <option key={option.value || "all"} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Country</span>
          <input
            value={country}
            maxLength={2}
            onChange={(event) => {
              setCountry(event.target.value.toUpperCase());
              resetPage();
            }}
            placeholder="All"
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white uppercase outline-none placeholder:normal-case placeholder:text-slate-500 focus:border-emerald-300/50"
          />
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Admin flag</span>
          <select
            value={adminFilter}
            onChange={(event) => {
              setAdminFilter(event.target.value as AdminUserAdminFilter);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            <option value="all">All</option>
            <option value="admin">Admin</option>
            <option value="non_admin">Non-admin</option>
          </select>
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Sort by</span>
          <select
            value={sortBy}
            onChange={(event) => {
              setSortBy(event.target.value as AdminUserSortBy);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            {SORT_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Direction</span>
          <select
            value={sortDir}
            onChange={(event) => {
              setSortDir(event.target.value as AdminUserSortDir);
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            <option value="desc">Descending</option>
            <option value="asc">Ascending</option>
          </select>
        </label>

        <label className="text-sm">
          <span className="block font-medium text-slate-200">Rows</span>
          <select
            value={pageSize}
            onChange={(event) => {
              setPageSize(Number(event.target.value));
              resetPage();
            }}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
          >
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </label>
      </div>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-400">
        <div>
          {busy ? "Loading users." : `${users?.total ?? 0} users`}
          {users?.filters.plan && users.filters.plan !== "all" ? ` on ${users.filters.plan}` : ""}
          {users?.filters.status ? ` with ${compactStatus(users.filters.status)} status` : ""}
          {users?.filters.country ? ` in ${users.filters.country}` : ""}
          {users?.filters.admin && users.filters.admin !== "all" ? ` scoped to ${users.filters.admin.replace("_", "-")}` : ""}
        </div>
      </div>

      <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/40 p-3">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-semibold text-slate-200">{selectedCount} selected</span>
          <button type="button" disabled={busy || selectedCount === 0} onClick={() => batchUpdate("premium")} className="rounded-lg border border-white/10 px-2 py-1 text-xs font-semibold text-slate-200 disabled:opacity-50">Batch Premium</button>
          <button type="button" disabled={busy || selectedCount === 0} onClick={() => batchUpdate("pro")} className="rounded-lg border border-cyan-300/30 px-2 py-1 text-xs font-semibold text-cyan-100 disabled:opacity-50">Batch Pro</button>
          <button type="button" disabled={busy || selectedCount === 0} onClick={() => batchUpdate("free")} className="rounded-lg border border-amber-300/30 px-2 py-1 text-xs font-semibold text-amber-100 disabled:opacity-50">Batch Downgrade</button>
          <button type="button" disabled={busy || selectedCount === 0} onClick={() => batchUpdate("suspend")} className="rounded-lg border border-rose-300/30 px-2 py-1 text-xs font-semibold text-rose-100 disabled:opacity-50">Batch Suspend</button>
          <button type="button" disabled={busy || selectedCount === 0} onClick={() => batchUpdate("unsuspend")} className="rounded-lg border border-white/10 px-2 py-1 text-xs font-semibold text-slate-200 disabled:opacity-50">Batch Unsuspend</button>
          <button type="button" disabled={busy || selectedCount === 0} onClick={() => batchUpdate("override")} className="rounded-lg border border-white/10 px-2 py-1 text-xs font-semibold text-slate-200 disabled:opacity-50">Batch Price Override</button>
          <button type="button" disabled={busy || selectedCount === 0} onClick={() => batchUpdate("clear_override")} className="rounded-lg border border-white/10 px-2 py-1 text-xs font-semibold text-slate-200 disabled:opacity-50">Clear Overrides</button>
        </div>
        <div className="mt-3 grid gap-2 md:grid-cols-[8rem_8rem_6rem_1fr]">
          <input type="number" min={0} step="0.01" value={overrideDraft.monthly} onChange={(event) => setOverrideDraft((current) => ({ ...current, monthly: event.target.value }))} placeholder="Monthly $" className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-xs text-white outline-none focus:border-emerald-300/50" />
          <input type="number" min={0} step="0.01" value={overrideDraft.annual} onChange={(event) => setOverrideDraft((current) => ({ ...current, annual: event.target.value }))} placeholder="Annual $" className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-xs text-white outline-none focus:border-emerald-300/50" />
          <input value={overrideDraft.currency} onChange={(event) => setOverrideDraft((current) => ({ ...current, currency: event.target.value.toUpperCase() }))} maxLength={8} placeholder="USD" className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-xs text-white outline-none focus:border-emerald-300/50" />
          <input value={overrideDraft.note} onChange={(event) => setOverrideDraft((current) => ({ ...current, note: event.target.value }))} placeholder="Billing override metadata note" className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-xs text-white outline-none focus:border-emerald-300/50" />
        </div>
      </div>

      {status ? <p className="mt-3 text-sm text-slate-400">{status}</p> : null}

      <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
        <table className="min-w-[1900px] text-left text-xs">
          <thead className="bg-slate-950/70 uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-3 py-3">
                <input
                  type="checkbox"
                  checked={allVisibleSelected}
                  onChange={(event) => {
                    const visibleIds = rows.map((user) => user.id);
                    setSelectedIds((current) =>
                      event.target.checked
                        ? Array.from(new Set([...current, ...visibleIds]))
                        : current.filter((id) => !visibleIds.includes(id)),
                    );
                  }}
                  className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
                />
              </th>
              <th className="px-3 py-3">User name</th>
              <th className="px-3 py-3">Email</th>
              <th className="px-3 py-3">Country</th>
              <th className="px-3 py-3">State/province</th>
              <th className="px-3 py-3">Plan</th>
              <th className="px-3 py-3">Status</th>
              <th className="px-3 py-3">Registered date</th>
              <th className="px-3 py-3">Last active</th>
              <th className="px-3 py-3">Admin flag</th>
              <th className="px-3 py-3">Access expires</th>
              <th className="px-3 py-3">Override metadata</th>
              <th className="px-3 py-3">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {rows.map((user) => (
              <tr key={user.id} className="text-slate-300">
                <td className="whitespace-nowrap px-3 py-3">
                  <input
                    type="checkbox"
                    checked={selectedIds.includes(user.id)}
                    onChange={(event) =>
                      setSelectedIds((current) =>
                        event.target.checked ? Array.from(new Set([...current, user.id])) : current.filter((id) => id !== user.id),
                      )
                    }
                    className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
                  />
                </td>
                <td className="whitespace-nowrap px-3 py-3 text-white">{displayName(user)}</td>
                <td className="whitespace-nowrap px-3 py-3">{user.email}</td>
                <td className="whitespace-nowrap px-3 py-3">{user.country || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3">{user.state_province || "-"}</td>
                <td className="whitespace-nowrap px-3 py-3">{displayPlan(user)}</td>
                <td className="whitespace-nowrap px-3 py-3">{compactStatus(user.status || (user.is_suspended ? "suspended" : user.subscription_status))}</td>
                <td className="whitespace-nowrap px-3 py-3">{formatDate(user.created_at)}</td>
                <td className="whitespace-nowrap px-3 py-3">{formatDate(user.last_seen_at)}</td>
                <td className="whitespace-nowrap px-3 py-3">{user.is_admin ? "Yes" : "No"}</td>
                <td className="whitespace-nowrap px-3 py-3">{formatDate(user.access_expires_at)}</td>
                <td className="whitespace-nowrap px-3 py-3">
                  {user.monthly_price_override || user.annual_price_override
                    ? `${user.override_currency || "USD"} ${user.monthly_price_override ? (user.monthly_price_override / 100).toFixed(2) : "-"} / ${user.annual_price_override ? (user.annual_price_override / 100).toFixed(2) : "-"}`
                    : "-"}
                </td>
                <td className="px-3 py-3">
                  <div className="flex flex-wrap gap-2">
                    <button
                      type="button"
                      className="rounded-lg border border-white/10 px-2 py-1 text-slate-200 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => setPremium(user, "premium")}
                    >
                      Premium
                    </button>
                    <button
                      type="button"
                      className="rounded-lg border border-cyan-300/30 px-2 py-1 text-cyan-100 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => setPremium(user, "pro")}
                    >
                      Pro
                    </button>
                    <button
                      type="button"
                      className="rounded-lg border border-white/10 px-2 py-1 text-slate-200 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => setPremium(user, "free")}
                    >
                      Downgrade
                    </button>
                    <button
                      type="button"
                      className="rounded-lg border border-white/10 px-2 py-1 text-slate-200 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => setPremium(user, null)}
                    >
                      Clear
                    </button>
                    <button
                      type="button"
                      className="rounded-lg border border-white/10 px-2 py-1 text-slate-200 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => suspend(user, !user.is_suspended)}
                    >
                      {user.is_suspended ? "Unsuspend" : "Suspend"}
                    </button>
                    <button
                      type="button"
                      className="rounded-lg border border-white/10 px-2 py-1 text-slate-200 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => setPriceOverride(user)}
                    >
                      Save override
                    </button>
                    <button
                      type="button"
                      className="rounded-lg border border-white/10 px-2 py-1 text-slate-200 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => clearPriceOverride(user)}
                    >
                      Clear override
                    </button>
                    <button
                      type="button"
                      className="rounded-lg border border-rose-300/30 px-2 py-1 text-rose-200 disabled:opacity-50"
                      disabled={busy}
                      onClick={() => deleteUser(user)}
                    >
                      Delete
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {!busy && rows.length === 0 ? (
              <tr>
                <td colSpan={13} className="px-3 py-8 text-center text-sm text-slate-400">
                  No users match these filters.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
        <div className="text-sm text-slate-400">
          Page {users?.page ?? page} of {totalPages}
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => setPage(1)}
            disabled={busy || page <= 1}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            First
          </button>
          <button
            type="button"
            onClick={() => setPage((current) => Math.max(1, current - 1))}
            disabled={busy || page <= 1}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            Previous
          </button>
          <button
            type="button"
            onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
            disabled={busy || page >= totalPages}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            Next
          </button>
          <button
            type="button"
            onClick={() => setPage(totalPages)}
            disabled={busy || page >= totalPages}
            className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
          >
            Last
          </button>
        </div>
      </div>
    </section>
  );
}
