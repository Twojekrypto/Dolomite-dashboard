(function exposeSupplyHealth(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) {
    module.exports = api;
    return;
  }

  root.DolomiteSupplyHealth = api;
  const boot = () => api.mount();
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
})(typeof globalThis !== 'undefined' ? globalThis : this, function createSupplyHealth() {
  const SUPPLY_HEALTH_PAGE_SIZE = 10;
  const healthScoreWeights = [
    { key: 'wallet', label: 'Wallet Distribution', weight: 25 },
    { key: 'concentration', label: 'Concentration Risk', weight: 30 },
    { key: 'stability', label: 'Supply Stability', weight: 20 },
    { key: 'growth', label: 'Growth', weight: 15 },
    { key: 'resilience', label: 'Exit Resilience', weight: 10 },
  ];
  const healthExplorerAddresses = {
    ethereum: 'https://etherscan.io/address/',
    berachain: 'https://berascan.com/address/',
    arbitrum: 'https://arbiscan.io/address/',
    mantle: 'https://mantlescan.xyz/address/',
    xlayer: 'https://www.okx.com/web3/explorer/xlayer/address/',
  };
  const healthChains = [
    { key: 'ethereum', label: 'Ethereum', icon: 'https://icons.llamao.fi/icons/chains/rsz_ethereum.jpg' },
    { key: 'arbitrum', label: 'Arbitrum', icon: 'https://icons.llamao.fi/icons/chains/rsz_arbitrum.jpg' },
    { key: 'berachain', label: 'Berachain', icon: 'https://icons.llamao.fi/icons/chains/rsz_berachain.jpg' },
    { key: 'mantle', label: 'Mantle', icon: 'https://icons.llamao.fi/icons/chains/rsz_mantle.jpg' },
    { key: 'xlayer', label: 'X Layer', icon: 'https://icons.llamao.fi/icons/chains/rsz_x%20layer.jpg' },
  ];
  const supplyHealthMonthIndex = {
    JAN: 0,
    FEB: 1,
    MAR: 2,
    APR: 3,
    MAY: 4,
    JUN: 5,
    JUL: 6,
    AUG: 7,
    SEP: 8,
    OCT: 9,
    NOV: 10,
    DEC: 11,
  };
  const supplyHealthMarketPresentations = {
    'arbitrum:0x2c799166c9f0dbf9efc5004cbce4c5a37fa39329': { symbol: 'gmARB-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef': { symbol: 'gmBTC-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x505582242757f16d72f8c4462a616e388ca1b074': { symbol: 'gmETH-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x18cb14564fbb015bd3439220d177799355abc0e0': { symbol: 'gmLINK-USD', name: 'Dolomite GM Market' },
    'arbitrum:0xb15bbbfcff6c411410c66642306d1ffa7ecec4d8': { symbol: 'gmBTC', name: 'Dolomite GM Market' },
    'arbitrum:0x2d165a76dd3e552df3860789331ab73c5a3d7f92': { symbol: 'gmETH', name: 'Dolomite GM Market' },
    'arbitrum:0x20d51cb520c4622dcc3d7e35003dbab07d547e7e': { symbol: 'gmUNI-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x24c9121c75c099b38d40020872b8a0d2c27c614d': { symbol: 'gmAAVE-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x1beed3b7d1237b7773b5c4c249933e3ca5e027c1': { symbol: 'gmDOGE-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x5c99f6cf6069698d234d50bf69ebd2f53e45ed1c': { symbol: 'gmGMX-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x1ebb1c7023addbb2b6e30e6f4c8d4a4440bfd412': { symbol: 'gmSOL-USD', name: 'Dolomite GM Market' },
    'arbitrum:0xc587646f67b38739006ed0200e2e0a26fdb01c9b': { symbol: 'gmWstETH-USD', name: 'Dolomite GM Market' },
    'arbitrum:0xcf248baf933c7b1b876b997246f25021a65383b3': { symbol: 'gmGMX', name: 'Dolomite GM Market' },
    'arbitrum:0xe5d6fe410c69b44c357403a1936b3bfaddbe340b': { symbol: 'gmPENDLE-USD', name: 'Dolomite GM Market' },
    'arbitrum:0x6586f1db71513daf94b0431156d225a46c00f20b': { symbol: 'gmPEPE-USD', name: 'Dolomite GM Market' },
    'arbitrum:0xf5063b40fa66ab2fbda2e6807ac5759a41a1b0c3': { symbol: 'gmWIF-USD', name: 'Dolomite GM Market' },
  };
  const state = {
    payload: null,
    query: '',
    chains: new Set(),
    sortField: 'supplyUsd',
    sortAsc: false,
    expandedKey: '',
    page: 1,
  };
  let controlsBound = false;

  function getSupplyHealthMarketPresentation(market) {
    const key = `${String(market?.chain || '').toLowerCase()}:${String(market?.tokenId || '').toLowerCase()}`;
    const override = supplyHealthMarketPresentations[key];
    return override
      ? { ...override }
      : {
          symbol: String(market?.symbol || ''),
          name: String(market?.name || ''),
        };
  }

  function isExpiredSupplyHealthMarket(market, nowMs = Date.now()) {
    const symbol = String(market?.symbol || '').trim();
    if (!/^dPT-/i.test(symbol)) return false;

    const maturityLabel = `${symbol} ${String(market?.name || '')}`;
    const match = maturityLabel.match(/(?:^|[-\s])(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(20\d{2})(?=$|[-\s)])/i);
    if (!match) return false;

    const day = Number(match[1]);
    const month = supplyHealthMonthIndex[match[2].toUpperCase()];
    const year = Number(match[3]);
    const maturityEndMs = Date.UTC(year, month, day, 23, 59, 59, 999);
    return Number.isFinite(maturityEndMs) && maturityEndMs < Number(nowMs);
  }

  function filterSupplyHealthMarkets(markets, filters = {}) {
    const query = String(filters.query || '').trim().toLowerCase();
    const chains = filters.chains instanceof Set ? filters.chains : new Set();

    return (Array.isArray(markets) ? markets : []).filter(market => {
      const matchesChain = chains.size === 0 || chains.has(market.chain);
      const presentation = getSupplyHealthMarketPresentation(market);
      const haystack = [
        presentation.symbol,
        presentation.name,
        market.symbol,
        market.name,
        market.tokenId,
        market.chain,
      ].join(' ').toLowerCase();
      return matchesChain && (!query || haystack.includes(query));
    });
  }

  function paginateSupplyHealthMarkets(markets, requestedPage, pageSize = 10) {
    const source = Array.isArray(markets) ? markets : [];
    const size = Math.max(1, Number(pageSize) || 10);
    const totalPages = Math.max(1, Math.ceil(source.length / size));
    const page = Math.max(1, Math.min(Number(requestedPage) || 1, totalPages));
    const start = (page - 1) * size;
    const rows = source.slice(start, start + size);

    return {
      page,
      totalPages,
      rows,
      spacerCount: Math.max(0, size - rows.length),
    };
  }

  function formatSupplyHealthInteger(value) {
    return String(Math.max(0, Number(value) || 0))
      .replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
  }

  function formatSupplyHealthPageRange(totalRows, requestedPage, pageSize = 10) {
    const total = Math.max(0, Number(totalRows) || 0);
    const size = Math.max(1, Number(pageSize) || 10);
    const totalPages = Math.max(1, Math.ceil(total / size));
    const page = Math.min(totalPages, Math.max(1, Number(requestedPage) || 1));
    const first = total === 0 ? 0 : ((page - 1) * size) + 1;
    const last = total === 0 ? 0 : Math.min(total, page * size);
    return `${formatSupplyHealthInteger(first)}–${formatSupplyHealthInteger(last)} of ${formatSupplyHealthInteger(total)}`;
  }

  function updateSupplyHealthFilters(targetState, patch = {}) {
    if (Object.prototype.hasOwnProperty.call(patch, 'query')) {
      targetState.query = patch.query;
    }
    if (Object.prototype.hasOwnProperty.call(patch, 'chains')) {
      targetState.chains = patch.chains;
    }
    targetState.page = 1;
    targetState.expandedKey = '';
    return targetState;
  }

  function clearSupplyHealthSearch(input) {
    if (!input) return;
    const EventConstructor = input.ownerDocument?.defaultView?.Event || Event;
    input.value = '';
    input.dispatchEvent(new EventConstructor('input', { bubbles: true }));
    input.focus();
  }

  function escapeHealthHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatHealthUsd(value) {
    if (value == null) return '—';
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return '—';
    const absolute = Math.abs(numeric);
    const sign = numeric < 0 ? '−' : '';
    if (absolute >= 1e9) return `${sign}$${(absolute / 1e9).toFixed(2)}B`;
    if (absolute >= 1e6) return `${sign}$${(absolute / 1e6).toFixed(2)}M`;
    if (absolute >= 1e3) return `${sign}$${(absolute / 1e3).toFixed(1)}K`;
    return `${sign}$${absolute.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
  }

  function formatHealthPct(value, digits = 1) {
    const numeric = Number(value);
    if (value == null || !Number.isFinite(numeric)) return '—';
    return `${numeric.toFixed(digits)}%`;
  }

  function formatHealthSignedPct(value, digits = 1) {
    const numeric = Number(value);
    if (value == null || !Number.isFinite(numeric)) return '—';
    const sign = numeric > 0 ? '+' : '';
    return `${sign}${numeric.toFixed(digits)}%`;
  }

  function healthSignedClass(value) {
    const numeric = Number(value);
    if (value == null || !Number.isFinite(numeric) || numeric === 0) return 'neutral';
    return numeric > 0 ? 'positive' : 'negative';
  }

  function healthConcentrationLevel(metric, value) {
    const numeric = Number(value);
    if (
      value == null
      || String(value).trim() === ''
      || !Number.isFinite(numeric)
      || (metric !== 'top10' && metric !== 'largest')
    ) {
      return '';
    }
    const lowMax = metric === 'largest' ? 20 : 40;
    const moderateMax = metric === 'largest' ? 40 : 60;
    if (numeric <= lowMax) return 'low';
    if (numeric <= moderateMax) return 'moderate';
    return 'high';
  }

  function formatHealthConcentrationTip(metric, value) {
    const level = healthConcentrationLevel(metric, value);
    if (!level) return '';
    const label = metric === 'largest' ? 'Largest supplier' : 'Top 10 suppliers';
    const levelLabel = `${level.charAt(0).toUpperCase()}${level.slice(1)}`;
    return `${label} concentration: ${formatHealthPct(value)} · ${levelLabel}`;
  }

  function healthGradeClass(grade) {
    return `grade-${String(grade || 'x').toLowerCase()}`;
  }

  function healthMarketKey(market) {
    return `${market.chain}:${market.tokenId}`;
  }

  function shortHealthAddress(address) {
    const text = String(address || '');
    if (text.length < 12) return text;
    return `${text.slice(0, 6)}…${text.slice(-4)}`;
  }

  function supplyHealthRelativeAge(timestamp) {
    const time = Date.parse(String(timestamp || ''));
    if (!Number.isFinite(time)) return 'recently';
    const minutes = Math.max(0, Math.floor((Date.now() - time) / 60000));
    if (minutes < 1) return 'now';
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  }

  function getHealthIcon(market) {
    if (typeof tokenIcon === 'function') {
      return tokenIcon(market.symbol, {
        chain: market.chain,
        addr: market.tokenId,
      });
    }
    return 'dolomite-logo.svg';
  }

  function healthSortValue(market, field) {
    switch (field) {
      case 'symbol':
        return getSupplyHealthMarketPresentation(market).symbol.toUpperCase();
      case 'supply30dPct': {
        const value = Number(market.growth?.supply30dPct);
        return Number.isFinite(value) ? value : -Infinity;
      }
      case 'scoreTotal': {
        const value = Number(market.score?.total);
        return Number.isFinite(value) ? value : -Infinity;
      }
      default: {
        const value = Number(market[field]);
        return Number.isFinite(value) ? value : -Infinity;
      }
    }
  }

  function getSortedSupplyHealthMarkets() {
    const markets = filterSupplyHealthMarkets(state.payload?.markets, {
      query: state.query,
      chains: state.chains,
    });
    return markets.sort((a, b) => {
      const aValue = healthSortValue(a, state.sortField);
      const bValue = healthSortValue(b, state.sortField);
      const comparison = typeof aValue === 'string'
        ? aValue.localeCompare(bValue)
        : aValue - bValue;
      return state.sortAsc ? comparison : -comparison;
    });
  }

  function renderSupplyHealthScoreBreakdown(market) {
    const score = market.score || {};
    const rows = healthScoreWeights.map(component => {
      const numeric = Number(score[component.key]);
      const hasValue = score[component.key] != null && Number.isFinite(numeric);
      const width = hasValue ? Math.max(2, Math.min(100, numeric)) : 0;
      return `
        <div class="supply-health-component">
          <div class="supply-health-component-label">${escapeHealthHtml(component.label)} <span>${component.weight}%</span></div>
          <div class="supply-health-component-bar"><span style="width:${width}%"></span></div>
          <div class="supply-health-component-value">${hasValue ? numeric.toFixed(0) : '—'}</div>
        </div>
      `;
    }).join('');
    return `<div class="supply-health-components">${rows}</div>`;
  }

  function renderSupplyHealthDetail(market) {
    const growth = market.growth || {};
    const explorer = healthExplorerAddresses[market.chain] || '';
    const topWallets = (Array.isArray(market.topWallets) ? market.topWallets : [])
      .slice(0, 3)
      .map((wallet, index) => `
        <div class="supply-health-top-wallet">
          <span class="supply-health-top-rank">#${index + 1}</span>
          ${explorer && wallet.address
            ? `<a class="supply-health-top-addr" href="${escapeHealthHtml(explorer + wallet.address)}" target="_blank" rel="noopener" title="Open ${escapeHealthHtml(wallet.address)} in explorer">${escapeHealthHtml(shortHealthAddress(wallet.address))}</a>`
            : `<span class="supply-health-top-addr">${escapeHealthHtml(shortHealthAddress(wallet.address))}</span>`}
          <span class="supply-health-top-share">${formatHealthPct(wallet.sharePct, 2)}</span>
          <span class="supply-health-top-usd">${formatHealthUsd(wallet.usd)}</span>
        </div>
      `).join('');
    const stats = [
      { label: 'Median / Wallet', value: formatHealthUsd(market.medianWalletUsd) },
      { label: 'Gini Coefficient', value: market.gini != null ? Number(market.gini).toFixed(3) : '—' },
      { label: '7D Supply', value: formatHealthSignedPct(growth.supply7dPct), tone: healthSignedClass(growth.supply7dPct) },
      { label: '30D Wallets', value: formatHealthSignedPct(growth.wallets30dPct), tone: healthSignedClass(growth.wallets30dPct) },
      { label: 'Avg Daily Move 30D', value: formatHealthPct(growth.avgDailyChange30dPct) },
      { label: 'Exit Impact', value: market.largestPct != null ? `−${formatHealthPct(market.largestPct)}` : '—' },
    ].map(stat => `
      <div class="supply-health-detail-stat">
        <div class="supply-health-detail-stat-label">${escapeHealthHtml(stat.label)}</div>
        <div class="supply-health-detail-stat-value ${stat.tone || ''}">${stat.value}</div>
      </div>
    `).join('');

    return `
      <div class="supply-health-detail">
        <div class="supply-health-detail-col">
          <div class="supply-health-detail-title">Score Breakdown</div>
          ${renderSupplyHealthScoreBreakdown(market)}
        </div>
        <div class="supply-health-detail-col">
          <div class="supply-health-detail-title">Market Signals</div>
          <div class="supply-health-detail-stats">${stats}</div>
        </div>
        <div class="supply-health-detail-col">
          <div class="supply-health-detail-title">Largest Suppliers</div>
          ${topWallets || '<div class="supply-health-detail-empty">No supplier data</div>'}
          <a class="supply-health-open-market" href="./supply/">Open Supply markets</a>
        </div>
      </div>
    `;
  }

  function renderSupplyHealthPagination(totalRows) {
    const pager = document.getElementById('supply-health-pagination');
    const range = document.getElementById('supply-health-page-range');
    if (!pager && !range) return;
    const pagination = paginateSupplyHealthMarkets(
      Array.from({ length: totalRows }),
      state.page,
      SUPPLY_HEALTH_PAGE_SIZE,
    );
    state.page = pagination.page;
    if (range) {
      range.textContent = formatSupplyHealthPageRange(
        totalRows,
        state.page,
        SUPPLY_HEALTH_PAGE_SIZE,
      );
    }
    if (!pager) return;
    if (pagination.totalPages <= 1) {
      pager.innerHTML = '';
      return;
    }
    pager.innerHTML = `
      <button class="supply-health-pager-btn" aria-label="First page" onclick="supplyHealthGoPage(1)" ${state.page === 1 ? 'disabled' : ''}>«</button>
      <button class="supply-health-pager-btn" aria-label="Previous page" onclick="supplyHealthGoPage(${state.page - 1})" ${state.page === 1 ? 'disabled' : ''}>‹</button>
      <span class="supply-health-pager-info">${state.page} / ${pagination.totalPages}</span>
      <button class="supply-health-pager-btn" aria-label="Next page" onclick="supplyHealthGoPage(${state.page + 1})" ${state.page === pagination.totalPages ? 'disabled' : ''}>›</button>
      <button class="supply-health-pager-btn" aria-label="Last page" onclick="supplyHealthGoPage(${pagination.totalPages})" ${state.page === pagination.totalPages ? 'disabled' : ''}>»</button>
    `;
  }

  function renderSupplyHealthHeaders() {
    document.querySelectorAll('#supply-health-table th[data-health-sort]').forEach(header => {
      const active = header.dataset.healthSort === state.sortField;
      header.classList.toggle('sorted', active);
      header.setAttribute('aria-sort', active ? (state.sortAsc ? 'ascending' : 'descending') : 'none');
      const arrow = header.querySelector('.sort-arrow');
      if (arrow) arrow.textContent = active ? (state.sortAsc ? '▲' : '▼') : '';
    });
  }

  function renderSupplyHealthTable() {
    const table = document.getElementById('supply-health-table');
    const body = document.getElementById('supply-health-table-body');
    const loadingState = document.getElementById('supply-health-state');
    const count = document.getElementById('supply-health-count');
    const updated = document.getElementById('supply-health-updated');
    if (!table || !body || !state.payload) return;

    const markets = getSortedSupplyHealthMarkets();
    const pageData = paginateSupplyHealthMarkets(markets, state.page, SUPPLY_HEALTH_PAGE_SIZE);
    state.page = pageData.page;
    renderSupplyHealthHeaders();
    renderSupplyHealthPagination(markets.length);

    if (count) {
      const total = state.payload.markets.length;
      count.textContent = markets.length === total
        ? `${total} markets`
        : `${markets.length} / ${total} markets`;
    }
    if (updated) {
      updated.textContent = `Data updated · ${supplyHealthRelativeAge(state.payload.generatedAt)}`;
    }
    if (loadingState) loadingState.hidden = true;
    table.hidden = false;

    const dataRows = pageData.rows.map(market => {
      const key = healthMarketKey(market);
      const expanded = state.expandedKey === key;
      const growth30 = market.growth?.supply30dPct;
      const score = market.score || {};
      const icon = getHealthIcon(market);
      const presentation = getSupplyHealthMarketPresentation(market);
      const top10Tip = formatHealthConcentrationTip('top10', market.top10Pct);
      const largestTip = formatHealthConcentrationTip('largest', market.largestPct);
      return `
        <tr class="supply-health-row${expanded ? ' expanded' : ''}" data-health-key="${escapeHealthHtml(key)}" tabindex="0" aria-expanded="${expanded ? 'true' : 'false'}">
          <td class="supply-health-asset-cell">
            <button type="button" class="supply-health-expander" data-health-toggle="${escapeHealthHtml(key)}" aria-label="${expanded ? 'Hide' : 'Show'} ${escapeHealthHtml(presentation.symbol || 'asset')} health details" aria-expanded="${expanded ? 'true' : 'false'}">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>
            </button>
            <img class="supply-health-asset-icon" src="${escapeHealthHtml(icon)}" alt="" onerror="this.src='dolomite-logo.svg'">
            <span class="supply-health-asset-copy">
              <span class="supply-health-asset-symbol">${escapeHealthHtml(presentation.symbol)}</span>
              <span class="supply-health-asset-name">${escapeHealthHtml(presentation.name)}</span>
            </span>
          </td>
          <td class="num">${formatHealthUsd(market.supplyUsd)}</td>
          <td class="num health-participation">${Number(market.wallets || 0).toLocaleString('en-US')}</td>
          <td class="num">${formatHealthUsd(market.avgWalletUsd)}</td>
          <td class="num health-concentration" data-tip="${escapeHealthHtml(top10Tip)}">${formatHealthPct(market.top10Pct)}</td>
          <td class="num health-concentration" data-tip="${escapeHealthHtml(largestTip)}">${formatHealthPct(market.largestPct)}</td>
          <td class="num ${healthSignedClass(growth30)}">${formatHealthSignedPct(growth30)}</td>
          <td class="num supply-health-score-cell">
            <span class="supply-health-score">${score.total != null ? Math.round(score.total) : '—'}</span>
            <span class="supply-health-grade ${healthGradeClass(score.grade)}">${escapeHealthHtml(score.grade || '·')}</span>
          </td>
        </tr>
        ${expanded
          ? `<tr class="supply-health-detail-row"><td colspan="8">${renderSupplyHealthDetail(market)}</td></tr>`
          : ''}
      `;
    }).join('');
    const noMatches = markets.length === 0
      ? '<tr class="supply-health-empty-row"><td colspan="8">No assets match the current filters.</td></tr>'
      : '';
    const visibleSlots = markets.length === 0 ? 1 : pageData.rows.length;
    const spacerRows = Array.from(
      { length: Math.max(0, SUPPLY_HEALTH_PAGE_SIZE - visibleSlots) },
      () => '<tr class="supply-health-spacer-row" aria-hidden="true"><td colspan="8">&nbsp;</td></tr>',
    ).join('');
    body.innerHTML = dataRows + noMatches + spacerRows;

    const toggleRow = key => {
      state.expandedKey = state.expandedKey === key ? '' : key;
      renderSupplyHealthTable();
    };
    body.querySelectorAll('.supply-health-row').forEach(row => {
      row.addEventListener('click', event => {
        if (event.target.closest('a, button')) return;
        toggleRow(row.dataset.healthKey || '');
      });
      row.addEventListener('keydown', event => {
        const isToggleKey = event.key === 'Enter' || event.key === ' ';
        if (event.target !== row || !isToggleKey) return;
        event.preventDefault();
        toggleRow(row.dataset.healthKey || '');
      });
    });
    body.querySelectorAll('[data-health-toggle]').forEach(button => {
      button.addEventListener('click', event => {
        event.stopPropagation();
        toggleRow(button.dataset.healthToggle || '');
      });
    });
  }

  function supplyHealthGoPage(page) {
    state.page = Number(page) || 1;
    state.expandedKey = '';
    renderSupplyHealthTable();
    document.querySelector('.supply-health-scroll')?.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function renderSupplyHealthChainFilter() {
    const button = document.getElementById('supply-health-chain-filter');
    const label = document.getElementById('supply-health-chain-label');
    const count = document.getElementById('supply-health-chain-count');
    const list = document.getElementById('supply-health-chain-list');
    if (!button || !label || !count || !list) return;

    const availableChains = healthChains.filter(chain =>
      state.payload?.markets?.some(market => market.chain === chain.key),
    );
    const selected = state.chains;
    const filtered = selected.size > 0;
    button.classList.toggle('filtered', filtered);
    label.textContent = !filtered
      ? 'All Chains'
      : selected.size === 1
        ? availableChains.find(chain => selected.has(chain.key))?.label || '1 Chain'
        : `${selected.size} Chains`;
    count.textContent = `${filtered ? selected.size : availableChains.length}/${availableChains.length}`;

    const check = '<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m2 6 2.5 2.5L10 3"/></svg>';
    const globe = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15 15 0 0 1 4 10 15 15 0 0 1-4 10 15 15 0 0 1-4-10 15 15 0 0 1 4-10z"/></svg>';
    const allRow = `
      <button type="button" class="tvl-dd-opt select-all${filtered ? '' : ' active'}" data-health-chain="all">
        <span class="dd-opt-check">${check}</span>
        <span class="dd-ico">${globe}</span>
        <span class="tvl-dd-opt-name">All Chains</span>
        <span class="tvl-dd-opt-count">${availableChains.length}</span>
      </button>
    `;
    const chainRows = availableChains.map(chain => {
      const marketCount = state.payload.markets.filter(market => market.chain === chain.key).length;
      return `
        <button type="button" class="tvl-dd-opt${selected.has(chain.key) ? ' active' : ''}" data-health-chain="${chain.key}">
          <span class="dd-opt-check">${check}</span>
          <img src="${chain.icon}" alt="">
          <span class="tvl-dd-opt-name">${chain.label}</span>
          <span class="tvl-dd-opt-count">${marketCount}</span>
        </button>
      `;
    }).join('');
    list.innerHTML = allRow + chainRows;
  }

  function bindSupplyHealthControls() {
    if (controlsBound) return;
    const card = document.getElementById('supply-health-card');
    const searchInput = document.getElementById('supply-health-search');
    const searchClear = document.getElementById('supply-health-search-clear');
    const chainButton = document.getElementById('supply-health-chain-filter');
    const chainPanel = document.getElementById('supply-health-chain-panel');
    const chainList = document.getElementById('supply-health-chain-list');
    const chainClear = document.getElementById('supply-health-chain-clear');
    if (!card || !searchInput || !searchClear || !chainButton || !chainPanel || !chainList) return;
    controlsBound = true;

    searchInput.addEventListener('input', () => {
      updateSupplyHealthFilters(state, { query: searchInput.value });
      searchClear.classList.toggle('visible', !!searchInput.value);
      renderSupplyHealthTable();
    });
    searchClear.addEventListener('click', () => clearSupplyHealthSearch(searchInput));

    const closeChainPanel = () => {
      chainButton.classList.remove('open');
      chainPanel.classList.remove('show');
      chainButton.setAttribute('aria-expanded', 'false');
    };
    chainButton.addEventListener('click', event => {
      if (event.target.closest('#supply-health-chain-clear')) return;
      const open = !chainPanel.classList.contains('show');
      chainButton.classList.toggle('open', open);
      chainPanel.classList.toggle('show', open);
      chainButton.setAttribute('aria-expanded', String(open));
    });
    chainClear?.addEventListener('click', event => {
      event.stopPropagation();
      updateSupplyHealthFilters(state, { chains: new Set() });
      renderSupplyHealthChainFilter();
      renderSupplyHealthTable();
    });
    chainList.addEventListener('click', event => {
      const option = event.target.closest('[data-health-chain]');
      if (!option) return;
      // The option list is rerendered below; stop the document-level outside-click
      // handler from seeing the detached option as an external click.
      event.stopPropagation();
      const key = option.dataset.healthChain;
      let chains = new Set(state.chains);
      if (key === 'all') {
        chains = new Set();
      } else if (chains.size === 0) {
        chains = new Set([key]);
      } else if (chains.has(key)) {
        chains.delete(key);
      } else {
        chains.add(key);
      }
      const availableCount = healthChains.filter(chain =>
        state.payload?.markets?.some(market => market.chain === chain.key),
      ).length;
      if (chains.size === availableCount) chains = new Set();
      updateSupplyHealthFilters(state, { chains });
      renderSupplyHealthChainFilter();
      renderSupplyHealthTable();
    });
    card.querySelectorAll('th[data-health-sort]').forEach(header => {
      header.addEventListener('click', () => {
        const field = header.dataset.healthSort;
        if (state.sortField === field) {
          state.sortAsc = !state.sortAsc;
        } else {
          state.sortField = field;
          state.sortAsc = field === 'symbol';
        }
        state.page = 1;
        state.expandedKey = '';
        renderSupplyHealthTable();
      });
    });
    document.addEventListener('click', event => {
      if (!event.target.closest('#supply-health-chain-dropdown')) closeChainPanel();
    });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape') closeChainPanel();
    });
  }

  async function fetchSupplyHealth() {
    const loadingState = document.getElementById('supply-health-state');
    try {
      const response = await fetch('data/supply-health/latest.json', { cache: 'no-cache' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      if (!payload || !Array.isArray(payload.markets)) throw new Error('Invalid payload');
      const markets = payload.markets.filter(market => !isExpiredSupplyHealthMarket(market));
      state.payload = {
        ...payload,
        markets,
        marketCount: markets.length,
      };
      renderSupplyHealthChainFilter();
      renderSupplyHealthTable();
    } catch (error) {
      if (loadingState) {
        loadingState.hidden = false;
        loadingState.textContent = 'Pool health data is unavailable right now.';
      }
      console.error('Supply Pool Health load failed', error);
    }
  }

  function mount() {
    if (!document.getElementById('supply-health-card')) return;
    bindSupplyHealthControls();
    renderSupplyHealthChainFilter();
    fetchSupplyHealth();
    window.supplyHealthGoPage = supplyHealthGoPage;
  }

  return {
    clearSupplyHealthSearch,
    filterSupplyHealthMarkets,
    formatHealthConcentrationTip,
    formatHealthUsd,
    formatSupplyHealthPageRange,
    getSupplyHealthMarketPresentation,
    healthConcentrationLevel,
    isExpiredSupplyHealthMarket,
    mount,
    paginateSupplyHealthMarkets,
    updateSupplyHealthFilters,
  };
});
