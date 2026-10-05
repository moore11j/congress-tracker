import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import test from 'node:test';
import ts from 'typescript';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
const require=createRequire(import.meta.url);
function load(file) {
 const exports={};
 const code=ts.transpileModule(fs.readFileSync(new URL(file,import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022,jsx:ts.JsxEmit.ReactJSX,esModuleInterop:true}}).outputText;
 vm.runInNewContext(code,{exports,URLSearchParams,require(name){
  if(name==='next/link')return {__esModule:true,default:({children,...props})=>React.createElement('a',props,children)};
  if(name==='next/navigation')return {useRouter:()=>({push(){}}),useSearchParams:()=>new URLSearchParams()};
  if(name==='@/lib/strategyPresentation')return load('../lib/strategyPresentation.ts');
  if(name==='@/components/backtesting/BacktestChart')return {BacktestChart:({timeline})=>React.createElement('div',{'data-chart-end':timeline.at(-1)?.date,'data-chart-value':timeline.at(-1)?.strategy_value,'data-chart-cash':timeline.at(-1)?.cash})};
  if(name==='@/components/billing/UpgradePrompt')return {UpgradePrompt:()=>null};
  if(name==='@/components/strategies/StrategyFollowButton')return {StrategyFollowButton:()=>null};
  return require(name);
 }});
 return exports;
}
const {StrategiesDirectory}=load('../components/strategies/StrategiesDirectory.tsx');
const {StrategyDetail}=load('../components/strategies/StrategyDetail.tsx');
const fixture={slug:'cleo',name:'Cleo Fields',category:'congress',status:'published',prospectiveActive:true,performance:{totalReturnPct:267.7},latestRun:{benchmark:'SPY',backtestStartDate:'2023-08-15',backtestEndDate:'2026-08-14'},equityCurve:[{date:'2026-08-14',strategyValue:367700,benchmarkValue:180000}],modelChart:{status:'current',startedOn:'2026-08-12',through:'2026-10-02',expectedThrough:'2026-10-02',points:[{date:'2026-10-02',strategyValue:105200,benchmarkValue:99600,cash:1200}],performance:{totalReturnPct:5.2}},currentHoldings:[]};
function render(surface,strategy,chartMode){
 return renderToStaticMarkup(React.createElement(surface==='preview'?StrategiesDirectory:StrategyDetail,surface==='preview'?{featured:strategy,data:{items:[strategy],metadata:{categoryCounts:{congress:1}}},period:'max',sort:'cagr',category:'all'}:{strategy,period:'max',positionsMode:'holdings',holdingsPage:1,historyPage:1,reportedPage:1,chartMode}));
}
for(const surface of ['preview','detail']){
 test(`${surface} uses daily endpoint, metrics and cash despite a newer historical run`,()=>{
  const html=render(surface,fixture);
  assert.match(html,/data-chart-end="2026-10-02"/);assert.match(html,/data-chart-value="105200"/);assert.match(html,/data-chart-cash="1200"/);assert.match(html,/\+5\.2%/);assert.match(html,/Oct 2, 2026/);assert.doesNotMatch(html,/data-chart-end="2026-08-14"/);
 });
 test(`${surface} never falls back to the stale backtest when daily cache is missing`,()=>{
  const html=render(surface,{...fixture,modelChart:null});assert.doesNotMatch(html,/data-chart-end=/);assert.match(html,/Daily chart refresh is pending/);
 });
 test(`${surface} shows pending first session and partial coverage honestly`,()=>{
  let html=render(surface,{...fixture,modelChart:{...fixture.modelChart,status:'awaiting_first_session',points:[],performance:{}}});assert.doesNotMatch(html,/data-chart-end=/);assert.match(html,/after the next market close/);
  html=render(surface,{...fixture,modelChart:{...fixture.modelChart,status:'partial_coverage',unfilledSymbolCount:3}});assert.match(html,/3 tickers had unfilled entries/);assert.match(html,/data-chart-end="2026-10-02"/);
 });
}
test('detail retains explicitly selected historical research',()=>{
 const html=render('detail',fixture,'historical');assert.match(html,/data-chart-end="2026-08-14"/);assert.match(html,/\+267\.7%/);
});
test('preview uses preserved research for a historical-only strategy',()=>{
 const html=render('preview',{...fixture,prospectiveActive:false});assert.match(html,/data-chart-end="2026-08-14"/);assert.match(html,/Archived research through Aug 14, 2026/);
});
