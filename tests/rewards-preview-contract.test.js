const assert = require('assert');
const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'rewards-preview.html'), 'utf8');

function heroSection() {
  const match = html.match(/<!-- HERO -->[\s\S]*?<\/section>/);
  assert(match, 'Rewards hero section should exist');
  return match[0];
}

{
  const hero = heroSection();
  assert(hero.includes('class="hero-price-row"'), 'Rewards hero should use the DOLO-style focus metric row');
  assert(hero.includes('class="hero-price"') && hero.includes('id="rwDailyUsd"'), 'Daily rewards should be the large hero focus metric');
  assert(hero.includes('class="hero-stats"'), 'Rewards hero should use the DOLO-style stat strip');
  assert(!hero.includes('class="hero-grid"'), 'Rewards hero should not use standalone metric cards');
  assert(!hero.includes('class="hero-card"'), 'Rewards hero should not use nested metric cards');
}

{
  assert(/function\s+programRewardsUrl\(program\)/.test(html), 'Rewards page should resolve campaign links per program');
  assert(html.includes('https://app.merkl.xyz/opportunities/'), 'Merkl programs should link to Merkl opportunity pages');
  assert(html.includes('https://app.dolomite.io/rewards'), 'Dolomite oDOLO programs should link to Dolomite rewards');
  assert(/providerTag\(program\)/.test(html), 'Program cells should pass the full program to providerTag');
  assert(/class="type-tag \$\{cls\} provider-link"/.test(html), 'Provider tag should render as a clickable link');
}

{
  const nameRenderer = html.slice(
    html.indexOf('function shortProgramName(program)'),
    html.indexOf('function chainBadge(chain)')
  );
  assert(nameRenderer.includes("if (market && action === 'LEND') return `Supply ${market}`;"), 'LEND rewards should use Supply in visible program names');
  assert(nameRenderer.includes("replace(/^Lend\\s+/i, 'Supply ')"), 'Provider names beginning with Lend should be normalized to Supply');
  assert(!nameRenderer.includes('return `Lend ${market}`'), 'Rewards should not show Lend for supply programs');
  assert(html.includes("action === 'LEND'"), 'Source LEND classification should remain intact');
}

{
  assert(html.includes('id="rwLiveSearch"'), 'Live Programs should expose campaign search');
  assert(html.includes('id="rwPastSearch"'), 'Ended Programs should expose campaign search');
  assert(html.includes('rewards-search.js?v=rewards-search-20260805'), 'Rewards page should load the tested search helper');
  assert(html.includes("`${live.length} of ${allLive.length} programs`"), 'Live count should expose filtered and total programs');
  assert(html.includes("`${past.length} of ${allPast.length} programs`"), 'Ended count should expose filtered and total programs');
  assert(html.includes("input.dispatchEvent(new Event('input', { bubbles: true }))"), 'Clear buttons should use the normal input path');
  assert(/@media \(max-width:640px\)[\s\S]*?\.live-programs-toolbar\{[^}]*flex-direction:column/.test(html), 'Program controls should stack on mobile');
}
