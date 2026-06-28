import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const source = fs.readFileSync(path.join(process.cwd(), "components", "backtesting", "BacktestingWorkbench.tsx"), "utf8");

test("backtesting position symbols render as ticker links when valid", () => {
  assert.match(source, /import Link from "next\/link";/);
  assert.match(source, /import \{ tickerHref \} from "@\/lib\/ticker";/);
  assert.match(source, /function BacktestPositionSymbol/);
  assert.match(source, /const href = tickerHref\(label\);/);
  assert.match(source, /<Link href=\{href\} prefetch=\{false\}/);
  assert.match(source, /title=\{`Open \$\{label\} ticker page`\}/);
  assert.match(source, /<BacktestPositionSymbol symbol=\{position\.symbol\} \/>/);
});

test("backtesting position symbols avoid clickable malformed or missing values", () => {
  assert.match(source, /if \(!label\) return <span className="text-slate-500">-<\/span>;/);
  assert.match(source, /if \(!href\) return <span className="font-mono text-slate-300">\{label\}<\/span>;/);
  assert.match(source, /max-w-\[7rem\] truncate/);
});

test("backtesting public results do not render fallback implementation labels", () => {
  assert.doesNotMatch(source, />Fallback<\/span>/);
  assert.doesNotMatch(source, /Price fallback used/);
  assert.doesNotMatch(source, /price_fallback_used \?/);
});
