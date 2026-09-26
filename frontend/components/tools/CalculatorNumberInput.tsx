"use client";

import { useEffect, useId, useRef, useState } from "react";

const format = (value: number) => Number.isFinite(value)
  ? value.toLocaleString("en-US", { maximumFractionDigits: 20 }) : "";

/** Keep partial edits local; publish a valid number when the user leaves the field. */
export function CalculatorNumberInput({ value, onCommit, min = 0, max = 1000000, integer = false, className }: {
  value: number; onCommit: (value: number) => void; min?: number; max?: number;
  integer?: boolean; className?: string;
}) {
  const [draft, setDraft] = useState(() => format(value));
  const [error, setError] = useState("");
  const editing = useRef(false);
  const changed = useRef(false);
  const errorId = useId();
  useEffect(() => {
    if (!editing.current) { setDraft(format(value)); setError(""); }
  }, [value]);

  function commit() {
    editing.current = false;
    if (!changed.current) { setDraft(format(value)); return; }
    const raw = draft.trim();
    // Accept ordinary decimals and properly grouped pasted numbers, never blank as zero.
    const valid = /^-?(?:\d+|\d{1,3}(?:,\d{3})+)?(?:\.\d*)?$/.test(raw) && /\d/.test(raw);
    const next = Number(raw.replace(/,/g, ""));
    if (!valid || !Number.isFinite(next) || next < min || next > max || (integer && !Number.isInteger(next))) {
      setError(`Enter ${integer ? "a whole number" : "a number"} from ${format(min)} to ${format(max)}. Results keep the last valid value.`);
      return;
    }
    changed.current = false;
    setError(""); setDraft(format(next));
    if (next !== value) onCommit(next);
  }

  return <><input type="text" inputMode={integer && min >= 0 ? "numeric" : "decimal"} className={className}
    value={draft} aria-invalid={!!error} aria-describedby={error ? errorId : undefined}
    onFocus={() => { editing.current = true; }}
    onChange={event => { changed.current = true; setDraft(event.target.value); setError(""); }}
    onBlur={commit}
    onKeyDown={event => {
      if (event.key === "Enter") { event.preventDefault(); event.currentTarget.blur(); }
      if (event.key === "Escape") { changed.current = false; setError(""); setDraft(format(value)); event.currentTarget.blur(); }
    }} />{error ? <span id={errorId} role="alert" className="mt-1 block text-xs leading-5 text-amber-200">{error}</span> : null}</>;
}
