import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import ts from 'typescript';

function load(file, imports = {}) {
  const exports = {};
  const js = ts.transpileModule(fs.readFileSync(file, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  vm.runInNewContext(js, { exports, require: name => {
    assert.ok(name in imports, `Unexpected runtime dependency: ${name}`);
    return imports[name];
  }});
  return exports;
}
const format = load('lib/format.ts');
const { marketSnapshotDetailRows } = load('lib/marketSnapshot.ts', {'@/lib/format': format});

test('sector detail retains ETF identity, price-return label and observation date', () => {
  const rows = marketSnapshotDetailRows({sector_performance:[{
    sector:'Technology (XLK ETF proxy)', symbol:'XLK', is_proxy:true,
    change_pct:2, date:'2026-10-14', source:'eod_etf_proxy',
  }], generated_at:'2026-10-15T03:00:00Z'}, 'us-sectors');
  const row = rows.find(item => item.name.includes('XLK'));
  assert.equal(row.name, 'Technology (XLK ETF proxy)');
  assert.equal(row.valueText, 'ETF price return');
  assert.equal(row.changeValue, 2);
  assert.equal(row.unavailable, false);
  assert.equal(row.dateText, format.formatDateShort('2026-10-14'));
  assert.equal(rows.length, 11);
});

test('missing sector prices render unavailable while zero is retained', () => {
  const rows = marketSnapshotDetailRows({sector_performance:[
    {sector:'Technology (XLK ETF proxy)', is_proxy:true, change_pct:null},
    {sector:'Energy (XLE ETF proxy)', is_proxy:true, change_pct:0},
  ]}, 'us-sectors');
  const missing = rows.find(row => row.name.includes('XLK'));
  const zero = rows.find(row => row.name.includes('XLE'));
  assert.equal(missing.unavailable, true);
  assert.equal(missing.changeText, 'Unavailable');
  assert.equal(zero.unavailable, false);
  assert.equal(zero.changeValue, 0);
});

test('legacy FMP sector labels and values keep their existing contract', () => {
  const rows = marketSnapshotDetailRows({sector_performance:[{
    sector:'Basic Materials',change_pct:0.73,date:'2026-10-14',source:'sector_performance_snapshot',
  }]}, 'us-sectors');
  const row = rows.find(item => item.name === 'Basic Materials');
  assert.equal(row.changeValue, 0.73);
  assert.equal(row.valueText, '-');
  assert.equal(row.unavailable, false);
});
