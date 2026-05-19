"use client";

import { useState } from "react";
import { inputClassName } from "@/lib/styles";

type FormattedNumberInputProps = {
  name: string;
  label: string;
  value?: string | number;
  placeholder?: string;
  labelClassName: string;
};

export function parseNumberInput(displayValue: string): string {
  return displayValue.replace(/,/g, "").trim();
}

export function formatNumberInput(value?: string | number): string {
  if (value === undefined || value === null) return "";
  const raw = String(value).replace(/,/g, "").trim();
  if (!raw) return "";
  if (!/^-?(?:\d+|\d*\.\d*|\d+\.)$/.test(raw)) return String(value);

  const sign = raw.startsWith("-") ? "-" : "";
  const unsigned = sign ? raw.slice(1) : raw;
  const [wholePart, fractionPart] = unsigned.split(".");
  const formattedWhole = wholePart ? Number(wholePart).toLocaleString("en-US", { maximumFractionDigits: 0 }) : "";
  const withSign = `${sign}${formattedWhole}`;
  if (fractionPart !== undefined) return `${withSign}.${fractionPart}`;
  return withSign;
}

export function FormattedNumberInput({ name, label, value, placeholder, labelClassName }: FormattedNumberInputProps) {
  const initialRaw = parseNumberInput(value === undefined || value === null ? "" : String(value));
  const [displayValue, setDisplayValue] = useState(formatNumberInput(initialRaw));
  const [rawValue, setRawValue] = useState(initialRaw);
  const active = rawValue.trim() !== "";

  return (
    <label className={labelClassName}>
      {label}
      <input type="hidden" name={name} value={rawValue} />
      <input
        value={displayValue}
        onChange={(event) => {
          const nextRaw = parseNumberInput(event.currentTarget.value);
          setRawValue(nextRaw);
          setDisplayValue(formatNumberInput(nextRaw));
        }}
        placeholder={placeholder}
        inputMode="decimal"
        className={active ? `${inputClassName} border-emerald-500/40 bg-slate-950/40` : inputClassName}
      />
    </label>
  );
}
