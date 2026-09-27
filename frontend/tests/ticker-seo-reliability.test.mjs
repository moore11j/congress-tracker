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

function metadataFor(profile, snapshot, searchParams = {}) {
 const exports = {};
 const seoExports = {};
 vm.runInNewContext(ts.transpileModule(fs.readFileSync('lib/tickerSeo.ts','utf8'), {compilerOptions:{module:ts.ModuleKind.CommonJS}}).outputText, {exports:seoExports});
 const section=source.slice(source.indexOf('export async function generateMetadata'),source.indexOf('function isRecoverableTickerProfileError'));
 const context={exports, ...seoExports,
  normalizedTickerSymbolForRoute:s=>s.trim().toUpperCase(), canonicalTickerPathForSymbol:s=>`/ticker/${s}`,
  one:(sp,key)=>sp[key]??'',clampSide:()=> 'all',clampLookback:()=> '30',
  loadPublicTickerSnapshot:async()=>snapshot, loadTickerPageContext:async()=>({profile,bundle:null}),
  tickerCompanyName:t=>t.name,
  appPageMetadata:(path,metadata)=>({...metadata,alternates:{canonical:`https://app.walnutmarkets.com${path}`}})};
 vm.runInNewContext(ts.transpileModule(section,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,context);
 return exports.generateMetadata({params:Promise.resolve({symbol:'CART'}),searchParams:Promise.resolve(searchParams)});
}

test('CART stays indexable without an SEO snapshot or multiple populated modules',async()=>{
 const profile={ticker:{symbol:'CART',name:'Maplebear Inc.',identity_status:'resolved'}};
 for(const snapshot of [null,{indexable:false,payload:{symbol:'CART',company_name:'Maplebear Inc.'}}]) {
  const metadata=await metadataFor(profile,snapshot);
  assert.equal(metadata.robots.index,true);
  assert.equal(metadata.robots.follow,true);
  assert.equal(metadata.alternates.canonical,'https://app.walnutmarkets.com/ticker/CART');
  assert.match(metadata.title,/CART Stock Analysis/);
 }
});

test('cold ticker caches do not turn into a noindex directive',async()=>{
 const metadata=await metadataFor({ticker:{symbol:'CART',identity_status:'loading'}},null);
 assert.equal(metadata.robots.index,true);
});

test('ticker tab and filter URLs index with the clean ticker canonical',async()=>{
 const profile={ticker:{symbol:'CART',name:'Maplebear Inc.'}};
 for(const search of [{tab:'research'},{source:'insider',side:'buy',lookback:'90'},{utm_source:'reddit'}]) {
  const metadata=await metadataFor(profile,null,search);
  assert.equal(metadata.robots.index,true);
  assert.equal(metadata.alternates.canonical,'https://app.walnutmarkets.com/ticker/CART');
 }
});
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

test('a cold public context uses the snapshot without starting a profile rebuild',async()=>{
 const error=new ApiError(503);error.detail='public_context_cache_miss';
 const section=source.slice(source.indexOf('const loadTickerPageContext ='),source.indexOf('const loadPublicTickerSnapshot ='));
 const context={cache:fn=>fn,ApiError,TICKER_CONTEXT_SSR_TIMEOUT_MS:2500,withinTickerLoadBudget:p=>p,
  getTickerContextBundle:async()=>{throw error;},getTickerProfile:()=>assert.fail('must not rebuild'),
  fallbackTickerProfile:symbol=>({ticker:{symbol,identity_status:'loading'}})};
 vm.runInNewContext(ts.transpileModule(section+'\nglobalThis.loadContext=loadTickerPageContext;',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText,context);
 const result=await context.loadContext('NVDA','all',365,null,false,true);
 assert.equal(result.profile.ticker.identity_status,'loading');assert.equal(result.bundle,null);
});
