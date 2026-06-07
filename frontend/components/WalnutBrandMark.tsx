type WalnutBrandMarkProps = {
  className?: string;
  svgClassName?: string;
};

export function WalnutBrandMark({
  className = "relative flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border border-emerald-300/30 bg-slate-950 shadow-[0_0_24px_rgba(16,185,129,0.16)]",
  svgClassName = "h-8 w-8 overflow-visible",
}: WalnutBrandMarkProps) {
  return (
    <span className={className}>
      <svg viewBox="0 0 48 48" aria-hidden="true" className={svgClassName}>
        <defs>
          <linearGradient id="walnut-mark-stroke" x1="8" x2="40" y1="7" y2="42" gradientUnits="userSpaceOnUse">
            <stop offset="0" stopColor="#34f5a2" />
            <stop offset="0.56" stopColor="#10b981" />
            <stop offset="1" stopColor="#14b8a6" />
          </linearGradient>
        </defs>
        <path
          d="M24 7c-4.5 0-7.8 3.2-8.1 7.5-4.2.5-7.3 3.9-7.3 8.1 0 1.6.4 3 1.2 4.3-2 1.6-3.1 3.9-3.1 6.5 0 4.7 3.8 8.6 8.5 8.6 2.6 0 4.8-1.1 6.4-2.9.7.2 1.5.3 2.4.3s1.7-.1 2.4-.3c1.6 1.8 3.8 2.9 6.4 2.9 4.7 0 8.5-3.9 8.5-8.6 0-2.6-1.1-4.9-3.1-6.5.8-1.3 1.2-2.7 1.2-4.3 0-4.2-3.1-7.6-7.3-8.1C31.8 10.2 28.5 7 24 7Z"
          fill="rgba(2,6,23,0.95)"
          stroke="url(#walnut-mark-stroke)"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3.2"
        />
        <path
          d="M23.9 8.2v30.5M16 15.7c3.2 2.4 5.4 5.5 6.4 9.2M32 15.7c-3.2 2.4-5.4 5.5-6.4 9.2M10.1 26.7c4.1 1.5 7.1 3.9 9.1 7.4M37.9 26.7c-4.1 1.5-7.1 3.9-9.1 7.4"
          fill="none"
          stroke="url(#walnut-mark-stroke)"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="2.7"
        />
        <path
          d="M16.5 34V24.5M23.8 34V19M31.1 34V15.3"
          fill="none"
          stroke="#dfffee"
          strokeLinecap="round"
          strokeWidth="3.2"
        />
      </svg>
    </span>
  );
}
