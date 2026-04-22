"use client";

import { useRouter } from "next/navigation";
import type { KeyboardEvent, MouseEvent, ReactNode } from "react";

type Props = {
  href: string;
  label: string;
  children: ReactNode;
};

function isInteractiveTarget(target: EventTarget | null) {
  if (!(target instanceof HTMLElement)) return false;
  return Boolean(
    target.closest(
      "a,button,input,select,textarea,label,[role='button'],[role='dialog'],[data-row-action='true']",
    ),
  );
}

export function ClickableScreenerRow({ href, label, children }: Props) {
  const router = useRouter();

  const navigate = () => {
    router.push(href);
  };

  const handleClick = (event: MouseEvent<HTMLTableRowElement>) => {
    if (event.defaultPrevented || isInteractiveTarget(event.target)) return;
    navigate();
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLTableRowElement>) => {
    if (event.defaultPrevented || isInteractiveTarget(event.target)) return;
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    navigate();
  };

  return (
    <tr
      role="link"
      tabIndex={0}
      aria-label={label}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      className="group cursor-pointer outline-none transition-colors hover:bg-slate-900/35 focus-visible:bg-slate-900/45 focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-emerald-400/25"
    >
      {children}
    </tr>
  );
}
