import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const screenerPagePath = path.join(process.cwd(), "app", "screener", "page.tsx");
const source = fs.readFileSync(screenerPagePath, "utf8");

test("screener result headers keep the requested contracts column order", () => {
  const theadStart = source.indexOf("<thead");
  const theadEnd = source.indexOf("</thead>", theadStart);
  const tableHeaderSource = source.slice(theadStart, theadEnd);
  const headerMarkers = [
    'label="Symbol"',
    ">Company<",
    ">Sector<",
    'label="Market cap"',
    'label="Price"',
    'label="Volume"',
    'label="Beta"',
    'label="Congress"',
    'label="Insiders"',
    ">Institutional<",
    ">Options Flow<",
    ">Gov Contracts<",
    'label="Confirm"',
  ];
  const positions = headerMarkers.map((marker) => tableHeaderSource.indexOf(marker));
  positions.forEach((position, index) => {
    assert.notEqual(position, -1, `missing header marker ${headerMarkers[index]}`);
  });
  for (let index = 1; index < positions.length; index += 1) {
    assert.ok(
      positions[index] > positions[index - 1],
      `${headerMarkers[index]} should appear after ${headerMarkers[index - 1]}`,
    );
  }
});

test("gov contracts rendering distinguishes unavailable, inactive, and active rows", () => {
  assert.match(
    source,
    /availabilityStatus === "unavailable"[\s\S]*?Unavailable/,
    "gov contracts cell should render Unavailable when backend marks the overlay unavailable",
  );
  assert.match(
    source,
    /if \(!row\.government_contracts_active\) return <span className="text-sm text-slate-500">—<\/span>/,
    "inactive gov contracts rows should render an em dash",
  );
  assert.match(
    source,
    /formatCurrencyCompact\(row\.government_contracts_total_amount\)[\s\S]*?award\{count === 1 \? "" : "s"\}/,
    "active gov contracts rows should render award totals and counts",
  );
});
