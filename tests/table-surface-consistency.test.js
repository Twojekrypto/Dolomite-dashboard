const assert = require('assert');
const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');

const rewards = read('rewards-preview.html');
const revenue = read('revenue-preview.html');
const dolo = read('dolo-preview.html');
const odolo = read('odolo-preview.html');
const assets = read('assets-preview.html');
const portfolio = read('portfolio-preview.html');
const coreCss = read('dashboard-core.css');
const coreJs = read('dashboard-core.js');
const earnDraftCss = read('earn/earn-draft.css');

function between(source, start, end) {
  const from = source.indexOf(start);
  assert(from >= 0, `Missing start marker: ${start}`);
  const to = source.indexOf(end, from + start.length);
  assert(to >= 0, `Missing end marker: ${end}`);
  return source.slice(from, to);
}

{
  const renderHero = between(rewards, 'function renderHero()', 'function renderAll()');
  assert(
    rewards.includes('function formatUpdatedAge(value)'),
    'Rewards should format source freshness with a dedicated relative-age helper',
  );
  assert(
    renderHero.includes('Data updated · ${formatUpdatedAge(generatedAt)}'),
    'Live Programs should show source freshness instead of repeating the daily reward total',
  );
  assert(
    !renderHero.includes('/ day across ${live.length} programs'),
    'Live Programs should not repeat its program count in the metadata',
  );
  assert(
    rewards.includes('<th data-sort="campaignEnd" class="ends-col"'),
    'Live Programs should identify the Ends header for shared centering',
  );
  assert(
    renderHero.indexOf('Data updated ·') >= 0,
    'Live Programs freshness label should use the DOLO Holders wording',
  );
  const renderLive = between(rewards, 'function renderLive()', 'function renderPast()');
  assert(
    renderLive.includes('<td class="ends-col">${ends}</td>'),
    'Live Programs end dates should share the centered Ends column class',
  );
  assert(
    rewards.includes('.tbl thead th.ends-col,.tbl tbody td.ends-col{text-align:center'),
    'Live Programs should center both the Ends header and its values',
  );
}

{
  assert.strictEqual(
    (revenue.match(/class="panel-head revenue-section-head/g) || []).length,
    5,
    'All five Revenue sections should use the shared header hierarchy',
  );
  assert.strictEqual(
    (revenue.match(/class="revenue-section-primary"/g) || []).length,
    5,
    'Each Revenue section should expose a title/freshness row',
  );
  assert.strictEqual(
    (revenue.match(/class="revenue-section-secondary"/g) || []).length,
    5,
    'Each Revenue section should expose a subtitle/controls row',
  );
  assert.strictEqual(
    (revenue.match(/ data-revenue-updated>/g) || []).length,
    5,
    'Each Revenue section should expose one freshness target',
  );
  assert(
    revenue.includes('.revenue-section-primary{') &&
      /\.revenue-section-primary\{[^}]*border-bottom:1px solid var\(--line-2\)/s.test(revenue),
    'The full-width divider should belong to the primary row',
  );
  assert.strictEqual(
    between(revenue, '.hero-live{', '}'),
    between(dolo, '.hero-live{', '}'),
    'Revenue hero freshness typography and spacing should match the DOLO hero',
  );
  assert.strictEqual(
    between(revenue, '.hero-live .dot{', '}'),
    between(dolo, '.hero-live .dot{', '}'),
    'Revenue hero freshness dot should match the DOLO hero',
  );
  const renderHero = between(revenue, 'function renderHero()', 'function veBorrowRebateInfo()');
  assert(
    renderHero.includes('Data updated · ${dataAgeLabel(revenueData.generatedAt)}'),
    'Revenue hero freshness should use the same relative-age wording as the DOLO hero',
  );
  assert(
    !renderHero.includes('fresh.classList.toggle("audit-'),
    'Revenue hero freshness should keep the same neutral color and gold dot as the DOLO hero',
  );
  assert(
    !renderHero.includes('"Updated " + dateLabel(revenueData.generatedAt)'),
    'Revenue hero should not fall back to a locale-formatted absolute date',
  );
}

{
  const wrappers = [
    ['Rewards', rewards],
    ['DOLO', dolo],
    ['oDOLO', odolo],
    ['Assets', assets],
    ['Portfolio', portfolio],
  ];
  for (const [name, source] of wrappers) {
    assert(
      !/\.tbl-wrap\{[^}]*border-radius:0 0 var\(--r-xl\) var\(--r-xl\)/s.test(source),
      `${name} table scroll surfaces should not round and clip the last visible row hover`,
    );
  }
  assert(
    revenue.includes('.veborrow-wallet-table-wrap{overflow-x:auto;border-radius:0;'),
    'Top users saved with current veDOLO should keep the last visible row hover rectangular',
  );
  assert(
    /\.earn-terminal-row:hover td:first-child\s*\{[^}]*border-radius:\s*0 0 0 14px/s.test(earnDraftCss),
    'The true terminal Earn row should round its lower-left hover surface',
  );
  assert(
    /\.earn-terminal-row:hover td:last-child\s*\{[^}]*border-radius:\s*0 0 14px 0/s.test(earnDraftCss),
    'The true terminal Earn row should round its lower-right hover surface',
  );
  assert(
    /\.earn-terminal-row:hover td:first-child::before\s*\{[^}]*border-radius:\s*0 2px 0 14px/s.test(earnDraftCss),
    'The terminal Earn gold rail should follow the lower-left table corner',
  );
  assert(
    /\.earn-lending-table tbody tr\.earn-lend-row:hover td:first-child::before/s.test(earnDraftCss),
    'Borrow Positions should expose the shared gold hover rail',
  );
  assert(
    coreJs.includes('function earn_markTerminalPrimaryRow'),
    'Earn should mark the true terminal primary row instead of relying on structural last-child selectors',
  );
}
