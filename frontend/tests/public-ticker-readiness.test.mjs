import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
function load(file, modules = {}) {
 const exports = {};
 vm.runInNewContext(ts.transpileModule(fs.readFileSync(file,'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,
 {exports, require:name=>modules[name], fetch, Response, URLSearchParams, AbortController, setTimeout, clearTimeout});
 return exports;
}
const seo=load('lib/tickerSeo.ts');
const {publicTickerReady,unavailableTickerResponse}=load('lib/publicTickerReadiness.ts',{'./tickerSeo':seo});
const snapshot={indexable:true,entity_type:'ticker',payload:{symbol:'QNT',company_name:'Quantinuum',sections:[{heading:'Price',body:'Latest stored close'}]},data_as_of:'2026-09-30T12:00:00Z'};
const json=value=>Response.json(value);
test('saved public snapshot survives a missing or hanging context cache',async()=>{
 for(const hanging of [false,true]) {
  const result=await publicTickerReady('https://api.test','QNT',new URLSearchParams(),async(url,options)=>{
   assert.equal(options.cache,'no-store');
   if(url.includes('seo-snapshots'))return json({snapshot});
   assert.match(url,/cached_only=true/);
   if(!hanging)return new Response('',{status:503});
   return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new Error('cancelled'))));
  });
  assert.equal(result,true);
 }
});
test('public context still works without a snapshot, using the same page filters',async()=>{
 assert.equal(await publicTickerReady('https://api.test','QNT',new URLSearchParams('lookback=90&side=buy'),async url=>{
  if(url.includes('seo-snapshots'))return json({snapshot:null});
  assert.match(url,/lookback_days=90/);assert.match(url,/side=buy/);
  return json({ticker:{symbol:'QNT',identity_status:'resolved'}});
 }),true);
});
test('empty or mismatched public data cannot produce an indexable success shell',async()=>{
 for(const value of [{snapshot:null},{snapshot:{...snapshot,indexable:false}},{ticker:{symbol:'QNT',identity_status:'loading'}},{ticker:{symbol:'NVDA',identity_status:'resolved'}}]) {
  assert.equal(await publicTickerReady('https://api.test','QNT',new URLSearchParams(),async()=>json(value)),false);
 }
});
test('network failures produce a temporary non-cacheable 503 without noindex',async()=>{
 assert.equal(await publicTickerReady('https://api.test','QNT',new URLSearchParams(),async()=>{throw new Error('offline');}),false);
 const response=unavailableTickerResponse();
 assert.equal(response.status,503);assert.equal(response.headers.get('retry-after'),'60');
 assert.equal(response.headers.get('cache-control'),'no-store');assert.equal(response.headers.get('x-robots-tag'),null);
 assert.doesNotMatch(await response.text(),/noindex/);
});
