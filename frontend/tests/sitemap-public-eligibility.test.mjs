import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
function sitemap(corrections={}) {
 const exports={};const source=ts.transpileModule(fs.readFileSync('lib/seoQuality.ts','utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
 vm.runInNewContext(source,{exports,URL,require:()=>({sitemapCorrections:corrections})});return exports.sitemapUrlset;
}
const origin='https://app.walnutmarkets.com';
const page=(path,lastmod='2026-09-13')=>({path,lastmod,type:'insider',rationale:'test'});
test('sitemap excludes confirmed noindex pages while preserving eligible URLs',()=>{
 const xml=sitemap({[origin+'/insider/empty']:{exclude:true,reason:'noindex'}})(origin,[page('/insider/empty'),page('/insider/valid')]);
 assert.doesNotMatch(xml,/insider\/empty/);assert.match(xml,/insider\/valid/);
});
test('sitemap emits one verified canonical and keeps the latest date',()=>{
 const xml=sitemap({[origin+'/insider/alias']:{canonical:origin+'/insider/canonical',reason:'canonical mismatch'}})(origin,[page('/insider/alias'),page('/insider/canonical','2026-09-12')]);
 assert.equal((xml.match(/<loc>/g)||[]).length,1);assert.doesNotMatch(xml,/insider\/alias/);assert.match(xml,/<lastmod>2026-09-13<\/lastmod>/);
});
test('sitemap does not emit private query variants or cross-origin replacements',()=>{
 const xml=sitemap({[origin+'/insider/alias']:{canonical:'https://example.com/outside',reason:'test'}})(origin,[page('/insider/alias'),page('/ticker/NVDA?side=buy')]);
 assert.doesNotMatch(xml,/<loc>/);
});
test('a noindex canonical target cannot be restored through an alias',()=>{
 const xml=sitemap({[origin+'/insider/alias']:{canonical:origin+'/insider/empty',reason:'alias'},[origin+'/insider/empty']:{exclude:true,reason:'noindex'}})(origin,[page('/insider/alias')]);
 assert.doesNotMatch(xml,/<loc>/);
});
