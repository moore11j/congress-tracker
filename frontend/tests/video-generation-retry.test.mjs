import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const source = fs.readFileSync(new URL('../components/admin/GrowthVideoView.tsx', import.meta.url), 'utf8');
function retryForm() {
  let acknowledged = false, calls = 0, tree;
  const props = {busy:false, onRetry:async () => {calls++;}};
  const module = {exports:{}};
  vm.runInNewContext(ts.transpileModule(source, {compilerOptions:{module:ts.ModuleKind.CommonJS, jsx:ts.JsxEmit.ReactJSX}}).outputText, {
    module, exports:module.exports,
    require(name) {
      if (name === 'react') return {useState:() => [acknowledged, value => {acknowledged=value;}]};
      if (name.startsWith('@/')) return {};
      return require(name);
    },
  });
  const render = () => {tree=module.exports.RetryGeneration(props);};
  render();
  return {props, render, calls:()=>calls, checkbox:()=>tree.props.children[1].props.children[0], button:()=>tree.props.children[2]};
}
test('retry requires acknowledgment, submits once and resets acknowledgment', () => {
  const f=retryForm();
  assert.equal(f.button().props.children,'Retry generation');
  assert.equal(f.button().props.disabled,true);
  f.button().props.onClick(); assert.equal(f.calls(),0);
  f.checkbox().props.onChange({target:{checked:true}});f.render();
  assert.equal(f.button().props.disabled,false);
  f.button().props.onClick();f.render();
  assert.equal(f.calls(),1);assert.equal(f.button().props.disabled,true);
  f.button().props.onClick();assert.equal(f.calls(),1);
});
test('busy retry cannot queue another request', () => {
  const f=retryForm();
  f.checkbox().props.onChange({target:{checked:true}});f.props.busy=true;f.render();
  assert.equal(f.button().props.disabled,true);assert.equal(f.checkbox().props.disabled,true);
  f.button().props.onClick();assert.equal(f.calls(),0);
});
test('failed generation retry sits next to failure and outside revision disclosure', () => {
  const retry=source.indexOf('<RetryGeneration busy=');
  assert.ok(retry > source.indexOf('Could not finish this version.'));
  assert.ok(retry < source.indexOf('<GrowthDisclosure title="Changes, history and download"'));
  assert.match(source,/item.status === "FAILED" && !publications.length/);
  assert.match(source,/<RetryGeneration busy=\{busy\} onRetry=\{\(\) => act\("retry", \{ acknowledge_provider_retry: true \}\)\}/);
});
