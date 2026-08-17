import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("ticker header falls back to the canonical context identity name", () => {
  const page = readFileSync(join(process.cwd(), "app/ticker/[symbol]/page.tsx"), "utf8");

  assert.match(page, /function tickerCompanyName\(/);
  assert.match(page, /cleanTickerHeaderMetadata\(ticker\.name\) \?\? cleanTickerHeaderMetadata\(identity\?\.company_name\)/);
  assert.match(page, /tickerCompanyName\(profile\.ticker, contextBundle\?\.identity\)/);
});
