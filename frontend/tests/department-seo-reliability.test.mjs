import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
function load(file, dependencies) {
 const exports={};
 const source=ts.transpileModule(fs.readFileSync(file,'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022,jsx:ts.JsxEmit.ReactJSX}}).outputText;
 vm.runInNewContext(source,{exports,URL,process:{env:{}},require(name){return dependencies[name]??{};}});
 return exports;
}
const departments=load('lib/departments.ts',{});
class ApiError extends Error { constructor(status){super('API error');this.status=status;} }
function page(fetchProfile){
 return load('app/departments/[slug]/page.tsx',{
  react:{cache:fn=>{const values=new Map();return key=>{if(!values.has(key))values.set(key,fn(key));return values.get(key);};}},
  'react/jsx-runtime':{jsx:()=>null,jsxs:()=>null},
  'next/navigation':{notFound(){throw Error('NOT_FOUND');},permanentRedirect(path){throw Error('308 '+path);}},
  '@/lib/api':{ApiError,getDepartmentProfile:fetchProfile},
  '@/lib/departments':departments,
  '@/lib/format':{formatDateShort:()=>'-'},
  '@/lib/marketingMetadata':{WALNUT_APP_URL:'https://app.walnutmarkets.com',appCanonicalUrl:p=>'https://app.walnutmarkets.com'+p,appPageMetadata:(p,m)=>({...m,robots:{index:true,follow:true}})},
  '@/lib/seoQuality':{conciseSeoTitle:x=>x,conciseSeoDescription:x=>x,departmentHasIndexableContent:p=>p.summary.contractCount>0||p.summary.linkedTickerCount>0,noindexFollowMetadata:()=>({robots:{index:false,follow:true}})},
 });
}
const profile={name:'National Science Foundation',summary:{contractCount:1,linkedTickerCount:1},tickers:[],trend:[],categoryBreakdown:[],typeBreakdown:[],topPrograms:[],recentContracts:[],largestContracts:[]};
test('department metadata propagates outages instead of issuing noindex',async()=>{
 const mod=page(async()=>{throw new ApiError(503);});
 await assert.rejects(mod.generateMetadata({params:Promise.resolve({slug:'national-science-foundation'})}),e=>e.status===503);
});
test('department metadata remains noindex only for an actual empty profile',async()=>{
 const mod=page(async()=>({...profile,summary:{contractCount:0,linkedTickerCount:0}}));
 assert.equal((await mod.generateMetadata({params:Promise.resolve({slug:'national-science-foundation'})})).robots.index,false);
});
test('department metadata and page share the same profile request',async()=>{
 let calls=0;const mod=page(async()=>{calls++;return profile;});const props={params:Promise.resolve({slug:'national-science-foundation'})};
 assert.equal((await mod.generateMetadata(props)).robots.index,true);
 await mod.default(props);assert.equal(calls,1);
});
test('underscore department alias permanently redirects to hyphen canonical',async()=>{
 const mod=page(async slug=>{assert.equal(slug,'national-science-foundation');return profile;});
 await assert.rejects(mod.generateMetadata({params:Promise.resolve({slug:'national_science_foundation'})}),/308 \/departments\/national-science-foundation/);
});
test('unknown department remains a 404',async()=>{
 const mod=page(async()=>{throw new ApiError(404);});
 await assert.rejects(mod.generateMetadata({params:Promise.resolve({slug:'missing'})}),/NOT_FOUND/);
});
for (const [file, key] of [['app/member/[slug]/page.tsx','slug'],['app/institution/[cik]/page.tsx','cik']]) {
 for (const status of [404,503]) test(`${file}: HTTP ${status} is not converted to noindex metadata`,async()=>{
  const fail=async()=>{throw new ApiError(status);};
  const mod=load(file,{
   react:{cache:fn=>fn},'@/lib/api':{ApiError,getMemberProfileBySlug:fail,getInstitutionProfile:fail},
   'next/navigation':{notFound(){throw Error('NOT_FOUND');}},
   '@/lib/institution':{normalizeInstitutionCik:x=>x},
   '@/lib/marketingMetadata':{WALNUT_APP_URL:'https://app.walnutmarkets.com'},
  });
  await assert.rejects(mod.generateMetadata({params:Promise.resolve({[key]:key==='slug'?'NANCY_PELOSI':'0001081019'})}),
    error=>status===404?error.message==='NOT_FOUND':error.status===503);
 });
}
