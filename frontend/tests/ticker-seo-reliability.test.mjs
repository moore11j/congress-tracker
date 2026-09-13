import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
const source=fs.readFileSync('app/ticker/[symbol]/page.tsx','utf8');
const start=source.indexOf('const loadPublicTickerSnapshot =');
const end=source.indexOf('export async function generateMetadata',start);
const loader=ts.transpileModule(source.slice(start,end)+'\nglobalThis.loadSnapshot = loadPublicTickerSnapshot;',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText;
class ApiError extends Error { constructor(status){super('API error');this.status=status;} }
function load(fetcher){const context={cache:fn=>fn,ApiError,getSeoSnapshot:fetcher};vm.runInNewContext(loader,context);return context.loadSnapshot;}
test('ticker snapshot errors propagate instead of becoming noindex',async()=>{
 await assert.rejects(load(async()=>{throw new ApiError(503);})('AAPL'),e=>e.status===503);
});
test('an actually missing ticker snapshot remains missing',async()=>{
 assert.equal(await load(async()=>{throw new ApiError(404);})('MISSING'),null);
});
test('ticker metadata uses the persistent public cache without the 2.5 second deadline',async()=>{
 const snapshot={indexable:true};
 assert.equal(await load(async(type,symbol,options)=>{assert.equal(type,'ticker');assert.equal(symbol,'AAPL');assert.equal(options.stalePageCache,true);assert.equal(options.signal,undefined);return {snapshot};})('AAPL'),snapshot);
});
