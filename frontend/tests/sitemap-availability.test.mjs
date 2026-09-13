import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
function route(kind, reject) {
 const exports={};
 const fetchIndex=async()=>{if(reject)throw new Error('API unavailable');return {items:[]};};
 class NextResponse {constructor(body,init={}){this.body=body;this.status=init.status??200;this.headers=new Headers(init.headers);}}
 const source=ts.transpileModule(fs.readFileSync(`app/sitemap-${kind}.xml/route.ts`,'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
 vm.runInNewContext(source,{exports,URL,Date,require:(name)=>{
  if(name==='next/server')return {NextResponse};
  if(name==='@/lib/api')return {getSeoSnapshotIndex:fetchIndex,getPublicInstitutionIndex:fetchIndex,getDepartments:fetchIndex};
  if(name==='@/lib/seoQuality')return {sitemapUrlset:()=>'<urlset />'};
  if(name==='@/lib/insiderSeo')return {insiderSitemapPages:()=>[]};
  if(name==='@/lib/memberSlug')return {nameToSlug:s=>s};
  if(name==='@/lib/departments')return {departmentHref:s=>s};
  throw new Error(name);
 }});return exports.GET;
}
for(const kind of ['tickers','members','insiders','institutions','departments']) {
 test(`${kind} sitemap reports temporary API failure instead of publishing fallback inventory`,async()=>{
  const response=await route(kind,true)();
  assert.equal(response.status,503);assert.equal(response.headers.get('cache-control'),'no-store');assert.equal(response.headers.get('retry-after'),'300');
 });
 test(`${kind} sitemap permits an authoritative empty result`,async()=>{
  const response=await route(kind,false)();assert.equal(response.status,200);assert.equal(response.body,'<urlset />');
 });
}
