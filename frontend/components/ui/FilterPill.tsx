import type { ButtonHTMLAttributes, ReactNode } from "react";
import { filterControlClassName } from "@/lib/styles";

type FilterPillProps = {
  active?: boolean;
  children: ReactNode;
} & ButtonHTMLAttributes<HTMLButtonElement>;

export function FilterPill({ active = false, children, className = "", ...props }: FilterPillProps) {
  return (
    <button
      type="button"
      className={filterControlClassName(active, className)}
      {...props}
    >
      {children}
    </button>
  );
}
