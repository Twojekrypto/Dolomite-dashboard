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
  const panelHead = between(revenue, '.panel-head{', '}');
  assert(
    panelHead.includes('border-bottom:1px solid var(--line-1)'),
    'Revenue section headers should keep the DOLO Holders horizontal divider',
  );
  assert.strictEqual(
    (revenue.match(/<div class="panel-head/g) || []).length,
    5,
    'All five requested Revenue sections should use the divided panel header',
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
    !/last-of-type[^{}]*\{[^{}]*border-radius:\s*0 0 (?:0 )?(?:14|16)px/s.test(coreCss),
    'Shared Earn tables should not round the last data row cells',
  );
}
