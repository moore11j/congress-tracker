import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { createRequire } from 'node:module';
import test from 'node:test';
import ts from 'typescript';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';

const require = createRequire(import.meta.url);
function harness({ activeTab = 'overview', reducedMotion = false } = {}) {
  const hooks = {
    ...React,
    useState: initial => [initial === 'overview' ? activeTab : initial, () => {}],
    useRef: initial => ({ current: initial }),
    useEffect() {},
    useCallback: callback => callback,
  };
  function load(file) {
    const exports = {};
    const code = ts.transpileModule(fs.readFileSync(new URL(file, import.meta.url), 'utf8'), {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true },
    }).outputText;
    vm.runInNewContext(code, {
      exports,
      window: { matchMedia: () => ({ matches: reducedMotion }) },
      require(name) {
        if (name === 'react') return hooks;
        if (name === 'next/link') return { __esModule: true, default: ({ children, ...props }) => React.createElement('a', props, children) };
        if (name === '@/components/ui/HorizontalScrollAffordance') return scroll;
        if (name.startsWith('@/')) return new Proxy({}, { get: (_, key) => key === 'cardClassName' ? '' : ({ children }) => children ?? null });
        return require(name);
      },
    });
    return exports;
  }
  const scroll = load('../components/ui/HorizontalScrollAffordance.tsx');
  return { scroll, card: () => load('../components/ticker/TickerContextCard.tsx').TickerContextCard };
}

test('ticker arrows are separate accessible buttons and only invoke scrolling', () => {
  const { scroll } = harness();
  const moves = [];
  const state = scroll.useHorizontalScrollAffordance();
  state.scrollRef.current = { clientWidth: 400, scrollBy: options => moves.push(options) };
  const controls = scroll.HorizontalScrollIndicators({
    canScrollLeft: true, canScrollRight: true, ariaControls: 'ticker-tab-strip',
    onScrollLeft: () => state.scrollByPage(-1), onScrollRight: () => state.scrollByPage(1),
  });
  const buttons = controls.props.children;
  assert.equal(buttons.length, 2);
  for (const button of buttons) {
    assert.equal(button.type, 'button');
    assert.equal(button.props['aria-controls'], 'ticker-tab-strip');
    button.props.onClick();
  }
  assert.equal(buttons[1].props['aria-label'], 'Scroll tabs right');
  assert.deepEqual(moves.map(move => [move.left, move.behavior]), [[-300, 'smooth'], [300, 'smooth']]);
});

test('scrolling honors reduced motion and disables arrows at the bounds', () => {
  const { scroll } = harness({ reducedMotion: true });
  const state = scroll.useHorizontalScrollAffordance();
  let behavior;
  state.scrollRef.current = { clientWidth: 300, scrollBy: options => { behavior = options.behavior; } };
  state.scrollByPage(1);
  assert.equal(behavior, 'auto');
  const controls = scroll.HorizontalScrollIndicators({ canScrollLeft: false, canScrollRight: false, onScrollLeft() {}, onScrollRight() {} });
  assert.ok(controls.props.children.every(button => button.props.disabled));
});

function renderTab(activeTab) {
  const Card = harness({ activeTab }).card();
  return renderToStaticMarkup(React.createElement(Card, { symbol: 'DEMO', overview: 'Overview content' }));
}

test('rendered tabs follow the requested order and retain Research after Analysts', () => {
  const html = renderTab('overview');
  const labels = [...html.matchAll(/role="tab"[^>]*>([^<]+)<\/button>/g)].map(match => match[1]);
  assert.deepEqual(labels, ['Overview', 'Chart', 'News', 'Events/Filings', 'Financials', 'Ownership', 'Congress', 'Insider', 'Government contracts', 'Signals', 'Macro Positioning', 'Valuations', 'Analysts', 'Research']);
  assert.match(html, /aria-label="Scroll tabs right"/);
  assert.match(html, /aria-label="Scroll tabs left"/);
});

test('Events/Filings contains all three sources and Research still renders', () => {
  const events = renderTab('events');
  assert.match(events, /Press Releases/);
  assert.match(events, /SEC Filings/);
  assert.match(events, /Disclosure Activity/);
  const research = renderTab('research');
  assert.match(research, /Walnut research/);
  assert.match(research, /No published research is related to DEMO yet/);
});
