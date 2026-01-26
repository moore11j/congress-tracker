import type { FeedItem } from "@/lib/types";
import { FeedCard } from "@/components/feed/FeedCard";

export function FeedList({ items }: { items: FeedItem[] }) {
  if (!items.length) {
    return (
      <div className="rounded-3xl border border-dashed border-white/20 bg-white/5 p-8 text-center">
        <div className="text-lg font-semibold">No trades yet</div>
        <p className="mt-2 text-sm text-slate-400">Try broadening your filters or lowering the minimum amount.</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      {items.map((item) => (
        <FeedCard key={item.id} item={item} />
      ))}
    </div>
  );
}
