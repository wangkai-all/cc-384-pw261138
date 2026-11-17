// Optional DOM-level smoke test of the real page against a running gateway.
//
//   npm install jsdom            # in any directory (or set JSDOM_PATH)
//   node scripts/ui_smoke.mjs http://127.0.0.1:18080
//
// It loads the page with scripts enabled, clicks the same controls a user
// would, and asserts the rendered state.  Not part of the pytest suite.
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
let jsdom;
try {
  jsdom = require('jsdom');
} catch {
  jsdom = require(process.env.JSDOM_PATH || '/tmp/uitest/node_modules/jsdom');
}
const { JSDOM, VirtualConsole } = jsdom;

const base = process.argv[2];
if (!base) {
  console.error('usage: node scripts/ui_smoke.mjs http://127.0.0.1:PORT');
  process.exit(2);
}

const errors = [];
const virtualConsole = new VirtualConsole();
virtualConsole.on('jsdomError', e => errors.push('jsdomError: ' + e.message));
virtualConsole.on('error', (...args) => errors.push('console.error: ' + args.join(' ')));

const html = await (await fetch(base + '/')).text();
const dom = new JSDOM(html, {
  url: base + '/',
  runScripts: 'dangerously',
  resources: 'usable',
  pretendToBeVisual: true,
  virtualConsole,
  beforeParse(window) {
    window.fetch = (input, init) => fetch(new URL(input, base), init);
    window.HTMLElement.prototype.scrollIntoView = () => {};
    window.prompt = () => null;
  },
});

const { window } = dom;
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
await wait(2500);
const $ = sel => window.document.querySelector(sel);
const text = sel => ($(sel)?.textContent || '').replace(/\s+/g, ' ').trim();
const check = (label, ok, detail = '') => {
  console.log(`${ok ? 'PASS' : 'FAIL'} ${label}${detail ? ' :: ' + detail : ''}`);
  if (!ok) errors.push(label);
};

check('页面加载并显示可用性承诺', text('#banner').includes('可用性承诺'));
check('订单域运行中', text('#orders-state').includes('运行中'));
check('结算域运行中', text('#settlement-state').includes('运行中'));

$('#run-report').click();
await wait(2500);
check('首次查询已对齐', text('#sum-mode') === 'aligned', text('#sum-batch'));
check('选版依据有两行', window.document.querySelectorAll('#selection-table tbody tr').length === 2);
check('未覆盖范围已渲染', text('#uncovered').includes('无结算的订单'));

// Stop one side, keep pushing the other, then query again.
window.document.querySelector('[data-stop="settlement"]').click();
await wait(1600);
window.document.querySelector('[data-advance="orders"]').click();
await wait(1600);
$('#run-report').click();
await wait(2500);
check('单边停机后返回不完整结果', text('#summary-cards').includes('partial') || text('#sum-mode') === 'partial',
      text('#banner').slice(0, 80));
check('页面列出不可达域', text('#uncovered').includes('不可达'));

// Restore and compare.
window.document.querySelector('[data-start="settlement"]').click();
await wait(1800);
$('#run-report').click();
await wait(2500);
check('恢复后退回共同旧版', text('#sum-mode') === 'degraded_stale', text('#sum-mode'));
check('三次查询都可比较', window.document.querySelectorAll('#cmp-left option').length >= 3);
$('#compare').click();
await wait(1500);
check('比较输出列出差异字段', text('#compare-out').includes('mode'));

// Exercise the lease panel: hold a task, inspect it, stale-release it.
window.document.querySelector('[data-hold-task]').click();
await wait(2000);
check('租约任务已渲染', text('#tasks').includes('task-'), text('#tasks').slice(0, 90));
const staleBtn = window.document.querySelector('[data-release-stale-task]');
check('租约卡片有旧 token 释放按钮', Boolean(staleBtn));
if (staleBtn) {
  staleBtn.click();
  await wait(2000);
  check('旧 token 释放被拒绝', text('#toast').includes('拒绝'), text('#toast'));
}

check('无 JS 运行时错误', errors.length === 0, errors.slice(0, 5).join(' | '));
window.close();
process.exit(errors.length ? 1 : 0);
