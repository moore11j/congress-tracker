import type { AccountUser } from "@/lib/api";
import type { Entitlements } from "@/lib/entitlements";

type AccountLike = Pick<
  AccountUser,
  "id" | "role" | "is_admin" | "admin_flag" | "plan" | "manual_tier_override" | "entitlement_tier" | "subscription_plan"
> & {
  is_super_admin?: boolean;
  super_admin?: boolean;
  user_display_id?: string | null;
  user_id_display?: string | null;
};

const labelByTier: Record<string, string> = {
  admin: "Admin",
  pro: "Pro",
  premium: "Premium",
  free: "Free",
};

export function formatUserDisplayId(user: Pick<AccountUser, "id"> & { user_display_id?: string | null; user_id_display?: string | null }) {
  const existing = user.user_display_id || user.user_id_display;
  if (existing && !existing.includes("@")) return existing;
  return `U-${String(user.id).padStart(6, "0")}`;
}

function isSuperAdmin(user: AccountLike | null | undefined) {
  const role = String(user?.role ?? "").trim().toLowerCase();
  const adminFlag = String(user?.admin_flag ?? "").trim().toLowerCase();
  return Boolean(user?.is_super_admin || user?.super_admin || role === "super_admin" || role === "owner" || adminFlag === "super_admin");
}

function isAdmin(user: AccountLike | null | undefined, entitlements?: Entitlements | null) {
  return Boolean(user?.is_admin || user?.role === "admin" || entitlements?.tier === "admin" || entitlements?.user?.is_admin);
}

function normalizedTier(user: AccountLike | null | undefined, entitlements?: Entitlements | null) {
  const raw =
    user?.plan ||
    user?.manual_tier_override ||
    user?.entitlement_tier ||
    user?.subscription_plan ||
    entitlements?.tier ||
    "free";
  const tier = String(raw).trim().toLowerCase();
  return tier === "pro" || tier === "premium" || tier === "admin" ? tier : "free";
}

export function formatAccessLabel(user: AccountLike | null | undefined, entitlements?: Entitlements | null) {
  if (isSuperAdmin(user)) return "Super Admin";
  if (isAdmin(user, entitlements)) return "Admin";
  return labelByTier[normalizedTier(user, entitlements)] ?? "Free";
}

export function accountPlanSummary(user: AccountLike | null | undefined, entitlements: Entitlements) {
  const label = formatAccessLabel(user, entitlements);
  if (label === "Super Admin" || label === "Admin") {
    return {
      label,
      description: "Full administrative access across Walnut Market Terminal.",
    };
  }
  if (label === "Pro") {
    return {
      label,
      description: "Pro raises workflow limits and unlocks the highest-capacity research tools.",
    };
  }
  if (label === "Premium") {
    return {
      label,
      description: "Premium raises workflow limits and unlocks alert-first digests.",
    };
  }
  return {
    label: "Free",
    description: "Free stays useful for research. Premium raises workflow limits and unlocks alert-first digests.",
  };
}

export function formatInteger(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}
