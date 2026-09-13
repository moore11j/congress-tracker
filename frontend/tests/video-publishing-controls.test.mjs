import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";
import {createRequire} from "node:module";
import ts from "typescript";

const require = createRequire(import.meta.url);
process.env.TZ = "America/Los_Angeles";
function compile(path) {
  return ts.transpileModule(fs.readFileSync(new URL(path, import.meta.url), "utf8"), {
    compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020, jsx: ts.JsxEmit.ReactJSX},
  }).outputText;
}
const schedule = {exports: {}};
vm.runInNewContext(compile("../lib/videoSchedule.ts"), {...schedule, module: schedule, Intl, Date});

// Execute the real component and its event handlers with isolated hook state.
// No API requests or actual posts are made by these regression tests.
function form(previewLoaded = true) {
  let cursor = 0;
  const values = [], calls = [], effects = [];
  const hooks = {
    useState(initial) {
      const index = cursor++;
      if (!(index in values)) values[index] = initial;
      return [values[index], next => {values[index] = typeof next === "function" ? next(values[index]) : next;}];
    },
    useEffect(effect) {effects.push(effect);},
  };
  const module = {exports: {}};
  vm.runInNewContext(compile("../components/admin/GrowthVideoView.tsx"), {
    module, exports: module.exports, Intl, Date,
    require(name) {
      if (name === "react") return hooks;
      if (name === "@/lib/api") return {growthVideoRequest: async (...args) => {calls.push(args);}};
      if (name === "@/lib/videoSchedule") return schedule.exports;
      if (name === "@/components/admin/GrowthDisclosure") return {};
      return require(name);
    },
  });
  const props = {item: {id: "test-video"}, caption: "Reviewed caption", publications: [], bufferReady: true,
    previewLoaded, busy: false, run: async fn => fn()};
  let nodes;
  function render() {
    cursor = 0;
    const tree = module.exports.PublishVideo(props);
    effects.splice(0).forEach(effect => effect());
    nodes = [];
    function walk(node) {
      if (Array.isArray(node)) return node.forEach(walk);
      if (!node || typeof node !== "object") return;
      nodes.push(node);
      walk(node.props?.children);
    }
    walk(tree);
  }
  render(); render();
  return {
    calls,
    find: predicate => nodes.find(predicate),
    approval: () => nodes.find(n => n.type === "input" && n.props.type === "checkbox" && "disabled" in n.props),
    submit: () => nodes.find(n => n.type === "button"),
    change(node, target) {node.props.onChange({target}); render();},
  };
}

test("scheduled review checkbox works before choosing time; invalid time never enables posting", () => {
  const f = form();
  assert.ok(f.find(n => n.type === "input" && n.props.type === "datetime-local"));
  assert.equal(f.approval().props.disabled, false);
  f.change(f.approval(), {checked: true});
  assert.equal(f.approval().props.checked, true);
  assert.equal(f.submit().props.disabled, true);
  assert.ok(f.find(n => n.props?.children === "Choose a date and time."));
  f.change(f.find(n => n.props?.type === "datetime-local"), {value: "2000-01-01T09:00"});
  assert.equal(f.approval().props.checked, true);
  assert.equal(f.submit().props.disabled, true);
  assert.equal(f.calls.length, 0);
});

test("choosing a valid time preserves review and submits an explicit schedule with caption", async () => {
  const f = form();
  f.change(f.approval(), {checked: true});
  f.change(f.find(n => n.props?.type === "datetime-local"), {value: "2099-07-15T09:30"});
  assert.equal(f.approval().props.checked, true);
  assert.equal(f.submit().props.disabled, false);
  f.submit().props.onClick();
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0][0], "/jobs/test-video/publish");
  const payload = f.calls[0][2];
  assert.equal(payload.scheduled_at, "2099-07-15T16:30:00.000Z");
  assert.equal(payload.schedule_timezone, "America/Los_Angeles");
  assert.equal(payload.caption, "Reviewed caption");
  assert.equal(payload.reviewed_video_and_caption, true);
});

test("switching to Now requires renewed approval; preview and content review gates remain", () => {
  const unloaded = form(false);
  assert.equal(unloaded.approval().props.disabled, true);
  const f = form();
  f.change(f.approval(), {checked: true});
  f.change(f.find(n => n.props?.type === "radio" && !n.props.checked), {});
  assert.equal(f.approval().props.checked, false);
  assert.equal(f.submit().props.disabled, true);
  f.change(f.approval(), {checked: true});
  assert.equal(f.submit().props.disabled, false);
  f.change(f.find(n => n.type === "textarea"), {value: "Changed caption"});
  assert.equal(f.approval().props.checked, false);
  assert.equal(f.submit().props.disabled, true);
  assert.equal(f.calls.length, 0);
});
