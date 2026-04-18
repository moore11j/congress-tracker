"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { countryOptions, normalizeCountryInput, normalizeRegionInput, regionOptionsForCountry } from "@/lib/billingLocation";
import {
  getAccountSettings,
  getMe,
  updateAccountNotifications,
  updateAccountPassword,
  updateAccountProfile,
  type AccountNotificationSettings,
  type AccountUser,
} from "@/lib/api";
import { selectClassName } from "@/lib/styles";

const emptyNotifications: AccountNotificationSettings = {
  alerts_enabled: true,
  email_notifications_enabled: true,
  watchlist_activity_notifications: true,
  signals_notifications: true,
};

type PasswordStrength = {
  label: "Weak" | "Fair" | "Good" | "Strong";
  score: number;
  className: string;
};

function splitName(name?: string | null) {
  const cleaned = (name ?? "").trim();
  if (!cleaned) return { firstName: "", lastName: "" };
  const [firstName, ...rest] = cleaned.split(/\s+/);
  return { firstName, lastName: rest.join(" ") };
}

function passwordChecks(value: string) {
  return {
    length: value.length >= 8,
    alpha: /[A-Za-z]/.test(value),
    number: /\d/.test(value),
    special: /[^A-Za-z0-9]/.test(value),
  };
}

function passwordStrength(value: string): PasswordStrength {
  const checks = passwordChecks(value);
  const score = Object.values(checks).filter(Boolean).length;
  if (!value || score <= 1) return { label: "Weak", score: Math.max(score, 1), className: "bg-rose-300/70" };
  if (score === 2) return { label: "Fair", score, className: "bg-amber-300/70" };
  if (score === 3) return { label: "Good", score, className: "bg-sky-300/70" };
  return { label: "Strong", score, className: "bg-emerald-300/80" };
}

function fieldClassName(disabled = false) {
  return `mt-1 w-full rounded-lg border border-white/10 px-3 py-2 text-sm outline-none focus:border-emerald-300/50 ${
    disabled ? "bg-slate-950/70 text-slate-500" : "bg-slate-950 text-white placeholder:text-slate-500"
  }`;
}

export function AccountSettingsPanel() {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [country, setCountry] = useState("");
  const [stateProvince, setStateProvince] = useState("");
  const [postalCode, setPostalCode] = useState("");
  const [city, setCity] = useState("");
  const [addressLine1, setAddressLine1] = useState("");
  const [addressLine2, setAddressLine2] = useState("");
  const [notifications, setNotifications] = useState<AccountNotificationSettings>(emptyNotifications);
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [profileStatus, setProfileStatus] = useState<string | null>(null);
  const [passwordStatus, setPasswordStatus] = useState<string | null>(null);
  const [notificationStatus, setNotificationStatus] = useState<string | null>(null);
  const [loadStatus, setLoadStatus] = useState<string | null>(null);
  const [settingsApiUnavailable, setSettingsApiUnavailable] = useState(false);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    let cancelled = false;
    async function loadSettings() {
      try {
        const response = await getAccountSettings();
        if (cancelled) return;
        const fallback = splitName(response.user.name);
        setUser(response.user);
        setFirstName(response.user.first_name ?? fallback.firstName);
        setLastName(response.user.last_name ?? fallback.lastName);
        setCountry(response.user.country ?? "");
        setStateProvince(response.user.state_province ?? "");
        setPostalCode(response.user.postal_code ?? "");
        setCity(response.user.city ?? "");
        setAddressLine1(response.user.address_line1 ?? "");
        setAddressLine2(response.user.address_line2 ?? "");
        setNotifications(response.notifications);
      } catch (error) {
        try {
          const response = await getMe();
          if (cancelled) return;
          if (!response.user) {
            setLoadStatus("Sign in to manage your account settings.");
            return;
          }
          const fallback = splitName(response.user.name);
          setUser(response.user);
          setFirstName(response.user.first_name ?? fallback.firstName);
          setLastName(response.user.last_name ?? fallback.lastName);
          setCountry(response.user.country ?? "");
          setStateProvince(response.user.state_province ?? "");
          setPostalCode(response.user.postal_code ?? "");
          setCity(response.user.city ?? "");
          setAddressLine1(response.user.address_line1 ?? "");
          setAddressLine2(response.user.address_line2 ?? "");
          setNotifications(response.user.notifications ?? emptyNotifications);
          setSettingsApiUnavailable(true);
        } catch {
          if (!cancelled) setLoadStatus(error instanceof Error ? error.message : "Unable to load account settings.");
        }
      }
    }
    loadSettings();
    return () => {
      cancelled = true;
    };
  }, []);

  const checks = useMemo(() => passwordChecks(newPassword), [newPassword]);
  const strength = useMemo(() => passwordStrength(newPassword), [newPassword]);
  const normalizedCountry = normalizeCountryInput(country);
  const regionOptions = regionOptionsForCountry(normalizedCountry);
  const stateProvinceLabel =
    normalizedCountry === "US"
      ? "State"
      : normalizedCountry === "CA"
        ? "Province / territory"
        : "State / province / region";
  const passwordValid = Boolean(
    currentPassword &&
      checks.length &&
      checks.alpha &&
      checks.number &&
      checks.special &&
      newPassword &&
      confirmPassword &&
      newPassword === confirmPassword,
  );
  const billingProfileComplete = Boolean(
    firstName.trim() &&
      lastName.trim() &&
      country.trim() &&
      postalCode.trim() &&
      city.trim() &&
      addressLine1.trim() &&
      (!regionOptions.length || stateProvince.trim()),
  );

  const validateProfile = () => {
    const requiredFields = [
      { label: "First name", value: firstName },
      { label: "Last name", value: lastName },
      { label: "Country", value: country },
      { label: "Postal code", value: postalCode },
      { label: "City", value: city },
      { label: "Address line 1", value: addressLine1 },
    ];
    const missing = requiredFields.find((field) => !field.value.trim());
    if (missing) return `${missing.label} is required.`;
    if (normalizedCountry.length !== 2) return "Country must be a two-letter ISO code, like US or CA.";
    if (regionOptions.length && !stateProvince.trim()) return `${stateProvinceLabel} is required.`;
    return null;
  };

  const saveProfile = async (event: FormEvent) => {
    event.preventDefault();
    const validationError = validateProfile();
    if (validationError) {
      setProfileStatus(validationError);
      return;
    }
    setBusy(true);
    setProfileStatus(null);
    try {
      const next = await updateAccountProfile({
        first_name: firstName,
        last_name: lastName,
        country: normalizedCountry,
        state_province: normalizeRegionInput(normalizedCountry, stateProvince),
        postal_code: postalCode,
        city,
        address_line1: addressLine1,
        address_line2: addressLine2,
      });
      setUser(next);
      setProfileStatus("Profile saved.");
    } catch (error) {
      setProfileStatus(error instanceof Error ? error.message : "Unable to save profile.");
    } finally {
      setBusy(false);
    }
  };

  const savePassword = async (event: FormEvent) => {
    event.preventDefault();
    if (!passwordValid) return;
    setBusy(true);
    setPasswordStatus(null);
    try {
      await updateAccountPassword({
        current_password: currentPassword,
        new_password: newPassword,
        confirm_password: confirmPassword,
      });
      setCurrentPassword("");
      setNewPassword("");
      setConfirmPassword("");
      setPasswordStatus("Password updated.");
    } catch (error) {
      setPasswordStatus(error instanceof Error ? error.message : "Unable to update password.");
    } finally {
      setBusy(false);
    }
  };

  const saveNotifications = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setNotificationStatus(null);
    try {
      const next = await updateAccountNotifications(notifications);
      setNotifications(next);
      setNotificationStatus("Notification preferences saved.");
    } catch (error) {
      setNotificationStatus(error instanceof Error ? error.message : "Unable to save notification preferences.");
    } finally {
      setBusy(false);
    }
  };

  const toggleNotification = (key: keyof AccountNotificationSettings) => {
    setNotifications((current) => ({ ...current, [key]: !current[key] }));
  };

  if (loadStatus && !user) {
    return (
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h1 className="text-3xl font-semibold text-white">Account settings</h1>
        <p className="mt-3 text-sm text-slate-400">Sign in to manage your profile, password, and alert preferences.</p>
        <a href="/login?return_to=/account/settings" className="mt-4 inline-flex rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100">
          Login / Register
        </a>
      </section>
    );
  }

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Account</p>
        <h1 className="mt-1 text-3xl font-semibold text-white">General settings</h1>
        <p className="mt-2 text-sm text-slate-400">Manage your profile, password, and alert preferences.</p>
        {settingsApiUnavailable ? (
          <p className="mt-3 text-sm text-amber-200">
            Settings are visible from your current session. Saving requires the updated backend to be deployed.
          </p>
        ) : null}
      </section>

      <form onSubmit={saveProfile} noValidate className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h2 className="text-xl font-semibold text-white">Profile and billing location</h2>
        <p className="mt-2 text-sm text-slate-400">
          These details prepare your account for future Stripe Billing and Stripe Tax checkout.
        </p>
        {!billingProfileComplete ? (
          <p className="mt-3 rounded-lg border border-amber-300/20 bg-amber-300/10 px-3 py-2 text-sm text-amber-100">
            Billing profile incomplete. Add the required location fields before a taxable subscription checkout.
          </p>
        ) : null}
        <div className="mt-4 grid gap-4 md:grid-cols-2">
          <label className="text-sm">
            <span className="font-medium text-slate-200"><RequiredLabel>First Name</RequiredLabel></span>
            <input value={firstName} onChange={(event) => setFirstName(event.target.value)} className={fieldClassName()} />
          </label>
          <label className="text-sm">
            <span className="font-medium text-slate-200"><RequiredLabel>Last Name</RequiredLabel></span>
            <input value={lastName} onChange={(event) => setLastName(event.target.value)} className={fieldClassName()} />
          </label>
        </div>
        <label className="mt-4 block text-sm">
          <span className="font-medium text-slate-200">Email</span>
          <input value={user?.email ?? ""} disabled className={fieldClassName(true)} />
        </label>
        <p className="mt-2 text-sm text-slate-400">Email cannot be changed because it is the unique account identifier. Each user account has one email.</p>

        <div className="mt-4 grid gap-4 md:grid-cols-2">
          <label className="text-sm">
            <span className="font-medium text-slate-200"><RequiredLabel>Country</RequiredLabel></span>
            <select
              value={country}
              onChange={(event) => setCountry(event.target.value)}
              autoComplete="country"
              className={`mt-1 ${selectClassName}`}
            >
              <option value="">Select country</option>
              {countryOptions.map((option) => (
                <option key={option.code} value={option.code}>
                  {option.name}
                </option>
              ))}
            </select>
          </label>
          <label className="text-sm">
            <span className="font-medium text-slate-200">
              {regionOptions.length ? <RequiredLabel>{stateProvinceLabel}</RequiredLabel> : stateProvinceLabel}
            </span>
            {regionOptions.length ? (
              <select
                value={stateProvince}
                onChange={(event) => setStateProvince(event.target.value)}
                autoComplete="address-level1"
                className={`mt-1 ${selectClassName}`}
              >
                <option value="">Select {stateProvinceLabel.toLowerCase()}</option>
                {regionOptions.map((option) => (
                  <option key={option.code} value={option.code}>
                    {option.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                value={stateProvince}
                onChange={(event) => setStateProvince(event.target.value)}
                placeholder="Region"
                autoComplete="address-level1"
                className={fieldClassName()}
              />
            )}
          </label>
          <label className="text-sm">
            <span className="font-medium text-slate-200"><RequiredLabel>Postal code</RequiredLabel></span>
            <input
              value={postalCode}
              onChange={(event) => setPostalCode(event.target.value)}
              autoComplete="postal-code"
              className={fieldClassName()}
            />
          </label>
          <label className="text-sm">
            <span className="font-medium text-slate-200"><RequiredLabel>City</RequiredLabel></span>
            <input
              value={city}
              onChange={(event) => setCity(event.target.value)}
              autoComplete="address-level2"
              className={fieldClassName()}
            />
          </label>
          <label className="text-sm md:col-span-2">
            <span className="font-medium text-slate-200"><RequiredLabel>Address line 1</RequiredLabel></span>
            <input
              value={addressLine1}
              onChange={(event) => setAddressLine1(event.target.value)}
              autoComplete="address-line1"
              className={fieldClassName()}
            />
          </label>
          <label className="text-sm md:col-span-2">
            <span className="font-medium text-slate-200">Address line 2 <span className="text-slate-500">(optional)</span></span>
            <input
              value={addressLine2}
              onChange={(event) => setAddressLine2(event.target.value)}
              autoComplete="address-line2"
              className={fieldClassName()}
            />
          </label>
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-3">
          <button type="submit" disabled={busy} className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-60">
            Save profile
          </button>
          {profileStatus ? <span className="text-sm text-slate-400">{profileStatus}</span> : null}
        </div>
      </form>

      <form onSubmit={savePassword} noValidate className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h2 className="text-xl font-semibold text-white">Password</h2>
        <div className="mt-4 grid gap-4 md:grid-cols-3">
          <label className="text-sm">
            <span className="font-medium text-slate-200">Current password</span>
            <input type="password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} className={fieldClassName()} />
          </label>
          <label className="text-sm">
            <span className="font-medium text-slate-200">New password</span>
            <input type="password" value={newPassword} onChange={(event) => setNewPassword(event.target.value)} className={fieldClassName()} />
          </label>
          <label className="text-sm">
            <span className="font-medium text-slate-200">Confirm new password</span>
            <input type="password" value={confirmPassword} onChange={(event) => setConfirmPassword(event.target.value)} className={fieldClassName()} />
          </label>
        </div>

        <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="flex items-center justify-between gap-3 text-sm">
            <span className="font-semibold text-slate-200">Password strength</span>
            <span className="text-slate-300">{strength.label}</span>
          </div>
          <div className="mt-2 h-2 rounded-full bg-white/10">
            <div className={`h-2 rounded-full ${strength.className}`} style={{ width: `${(strength.score / 4) * 100}%` }} />
          </div>
          <div className="mt-3 grid gap-2 text-xs text-slate-400 sm:grid-cols-4">
            <Rule passed={checks.length} label="8 or more characters" />
            <Rule passed={checks.alpha} label="One letter" />
            <Rule passed={checks.number} label="One number" />
            <Rule passed={checks.special} label="One special character" />
          </div>
          {confirmPassword && newPassword !== confirmPassword ? (
            <p className="mt-3 text-sm text-rose-200">Confirm password must match the new password.</p>
          ) : null}
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <button
            type="submit"
            disabled={busy || !passwordValid}
            className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-50"
          >
            Update password
          </button>
          {passwordStatus ? <span className="text-sm text-slate-400">{passwordStatus}</span> : null}
        </div>
      </form>

      <form onSubmit={saveNotifications} noValidate className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h2 className="text-xl font-semibold text-white">Alert notifications</h2>
        <p className="mt-2 text-sm text-slate-400">Choose which alerts can reach this account.</p>
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          <ToggleRow label="Alerts enabled" checked={notifications.alerts_enabled} onClick={() => toggleNotification("alerts_enabled")} />
          <ToggleRow label="Email notifications" checked={notifications.email_notifications_enabled} onClick={() => toggleNotification("email_notifications_enabled")} />
          <ToggleRow label="Watchlist activity notifications" checked={notifications.watchlist_activity_notifications} onClick={() => toggleNotification("watchlist_activity_notifications")} />
          <ToggleRow label="Signals notifications" checked={notifications.signals_notifications} onClick={() => toggleNotification("signals_notifications")} />
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-3">
          <button type="submit" disabled={busy} className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100 disabled:opacity-60">
            Save notifications
          </button>
          {notificationStatus ? <span className="text-sm text-slate-400">{notificationStatus}</span> : null}
        </div>
      </form>
    </div>
  );
}

function Rule({ passed, label }: { passed: boolean; label: string }) {
  return <span className={passed ? "text-emerald-200" : "text-slate-500"}>{label}</span>;
}

function RequiredLabel({ children }: { children: ReactNode }) {
  return (
    <>
      {children} <span className="text-emerald-300">*</span>
    </>
  );
}

function ToggleRow({ label, checked, onClick }: { label: string; checked: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex items-center justify-between gap-3 rounded-lg border border-white/10 bg-slate-950/40 p-4 text-left transition hover:border-white/20"
    >
      <span className="text-sm font-medium text-slate-200">{label}</span>
      <span
        className={`rounded border px-2 py-1 text-xs font-semibold ${
          checked ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-400"
        }`}
      >
        {checked ? "On" : "Off"}
      </span>
    </button>
  );
}
