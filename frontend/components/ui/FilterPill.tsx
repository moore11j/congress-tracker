import type { ButtonHTMLAttributes, ReactNode } from "react";
import { segmentedFilterControlClassName } from "@/lib/styles";

type FilterPillProps = {
  active?: boolean;
  children: ReactNode;
} & ButtonHTMLAttributes<HTMLButtonElement>;

export function FilterPill({ active = false, children, className = "", ...props }: FilterPillProps) {
  return (
    <button
      type="button"
      className={segmentedFilterControlClassName(active, className)}
      {...props}
    >
      {children}
    </button>
  );
}
