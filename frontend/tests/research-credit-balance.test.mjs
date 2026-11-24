import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync("lib/researchCreditBalance.ts", "utf8"), {
  compilerOptions: {module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022},
}).outputText,{exports,AbortController});
const {createCreditBalanceStore,validCreditBalance} = exports;
const snapshot = available => ({sandbox:true,monthly:available,purchased:0,available,reserved:0,billing_hold:false});
const flush = async () => {for(let i=0;i<12;i++)await Promise.resolve();};
function harness(fetcher) {
  const timers=new Map();let count=0,visible=true;
  const store=createCreditBalanceStore({fetch:fetcher,visible:()=>visible,
    schedule:(fn,delay)=>{timers.set(++count,{fn,delay});return count;},cancel:id=>timers.delete(id)});
  return {store,timers,hide(){visible=false;},show(){visible=true;}};
}

test("all balance displays share one request and refresh within two seconds",async()=>{
  let calls=0,value=2000;
  const h=harness(async()=>{calls++;return{ok:true,json:async()=>snapshot(value)};});
  const first=h.store.subscribe(()=>{}),second=h.store.subscribe(()=>{});
  await flush();assert.equal(calls,1);assert.equal(h.store.getSnapshot().balance.available,2000);
  value=2500;const timer=[...h.timers.values()][0];assert.equal(timer.delay,2000);timer.fn();await flush();
  assert.equal(h.store.getSnapshot().balance.available,2500);
  first();second();assert.equal(h.timers.size,0);assert.equal(h.store.getSnapshot().balance,null);
});

test("confirmation refresh propagates immediately to every subscriber",async()=>{
  let value=2000,a=0,b=0;
  const h=harness(async()=>({ok:true,json:async()=>snapshot(value)}));
  const one=h.store.subscribe(()=>a++),two=h.store.subscribe(()=>b++);await flush();
  value=2750;await h.store.refresh();assert.equal(h.store.getSnapshot().balance.available,2750);
  assert.equal(a,2);assert.equal(b,2);one();two();
});

test("logout invalidation prevents late responses leaking the previous account balance",async()=>{
  const requests=[];
  const h=harness((_,opts)=>new Promise(resolve=>requests.push({resolve,opts})));
  const stop=h.store.subscribe(()=>{});
  void h.store.refresh(true);
  assert.equal(requests[0].opts.signal.aborted,true);
  requests[1].resolve({ok:false,status:401});await flush();
  requests[0].resolve({ok:true,json:async()=>snapshot(9999)});await flush();
  assert.equal(h.store.getSnapshot().balance,null);assert.match(h.store.getSnapshot().status,/Sign in/);stop();
});

test("hidden tabs stop polling and failures never retain a seemingly current balance",async()=>{
  let good=true,calls=0;
  const h=harness(async()=>{calls++;if(!good)throw Error("offline");return{ok:true,json:async()=>snapshot(2000)};});
  const stop=h.store.subscribe(()=>{});await flush();h.hide();await h.store.refresh();assert.equal(calls,1);assert.equal(h.timers.size,0);
  h.show();good=false;await h.store.refresh();assert.equal(h.store.getSnapshot().balance,null);
  assert.match(h.store.getSnapshot().status,/Reconnecting/);stop();
});

test("malformed, negative, mismatched and non-sandbox balances are rejected",()=>{
  assert.ok(validCreditBalance(snapshot(2000)));
  for(const changes of [{available:-1},{sandbox:false},{billing_hold:"false"},{monthly:10},{reserved:NaN}])
    assert.equal(validCreditBalance({...snapshot(2000),...changes}),false);
});
