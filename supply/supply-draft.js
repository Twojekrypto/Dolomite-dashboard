(function () {
  const searchIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>';
  const applyIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>';
  const clearIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round"><line x1="6" y1="6" x2="18" y2="18"/><line x1="18" y1="6" x2="6" y2="18"/></svg>';
  const chevronIcon = '<svg class="supply-activity-type-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>';
  let selectionPatched = false;
  let optionsPatched = false;
  let historyPatched = false;
  let tablePatched = false;
  let activityPatched = false;
  let sortPatched = false;
  let originalSelectAsset = null;
  let stagedAssetId = '';
  let appliedAssetId = '';
  let chainDefaultAutoApplyArmed = false;
  const defaultSupplyAssetSymbol = String(window.__DOLO_SUPPLY_DEFAULT_ASSET || '').toUpperCase();
  const activityTypeOptions = [
    { type: 'deposit', label: 'Deposits' },
    { type: 'withdraw', label: 'Withdrawals' },
    { type: 'transfer', label: 'Transfers' },
    { type: 'liquidation', label: 'Liquidations' },
  ];
  const activityPeriodOptions = [
    { key: '1d', short: '24H', label: '24 hours', days: 1 },
    { key: '7d', short: '7D', label: '7 days', days: 7 },
    { key: '30d', short: '30D', label: '30 days', days: 30 },
    { key: '90d', short: '90D', label: '90 days', days: 90 },
    { key: '180d', short: '180D', label: '180 days', days: 180 },
    { key: 'all', short: 'All', label: 'All time', days: null },
  ];
  let activityPeriodKey = '30d';

  function setAssetState(selected) {
    document.body.classList.toggle('supply-has-asset', !!selected);
  }

  function enhanceSupplyFrame() {
    const frame = document.querySelector('#tab-supply .premium-supply-frame');
    if (!frame || frame.querySelector('.supply-draft-copy')) return;

    const copy = document.createElement('div');
    copy.className = 'supply-draft-copy';
    copy.innerHTML = `
      <div class="supply-draft-kicker"><span class="supply-draft-kicker-icon"><img src="dolo-logo.svg" alt="DOLO" onerror="this.src='dolomite-logo.svg'"></span><span>Dolomite Supply</span></div>
      <h1 class="supply-draft-title">Supply <span>Markets</span></h1>
      <div class="supply-draft-subtitle">Dolomite liquidity, supplier concentration, and market flow across supported chains.</div>
    `;
    frame.insertBefore(copy, frame.firstChild);
    organizeSupplyControls();
  }

  function organizeSupplyControls() {
    const frame = document.querySelector('#tab-supply .premium-supply-frame');
    const chain = document.getElementById('supply-chain-container');
    const asset = document.getElementById('supply-asset-container');
    const apply = document.querySelector('.supply-draft-apply-container');
    if (!frame || !chain || !asset) return;

    let deck = frame.querySelector('.supply-draft-control-deck');
    if (!deck) {
      deck = document.createElement('div');
      deck.className = 'supply-draft-control-deck';
      frame.appendChild(deck);
    }

    if (asset.parentElement !== deck) deck.appendChild(asset);
    if (chain.parentElement !== deck) deck.appendChild(chain);
    if (apply && apply.parentElement !== deck) deck.appendChild(apply);
    syncSupplyChainOptions();
  }

  function syncSupplyChainOptions() {
    const selected = document.getElementById('supply-chain-select')?.value || 'ethereum';
    const items = document.querySelectorAll('#chain-options-container .premium-supply-dropdown-item');
    items.forEach(item => {
      const handler = item.getAttribute('onclick') || '';
      const match = handler.match(/selectSupplyChain\('([^']+)'/);
      const chain = match ? match[1] : item.dataset.chain;
      if (chain) item.dataset.chain = chain;
      item.classList.toggle('active', !!chain && chain === selected);
    });
  }

  function markResultCards() {
    const supplyTable = document.getElementById('supply-table');
    const supplierCard = supplyTable ? supplyTable.closest('.table-card-outer') : null;
    if (supplierCard) supplierCard.classList.add('supply-draft-result-card');
    document.getElementById('supply-activity-card')?.classList.add(
      'supply-draft-activity-card',
      'supply-draft-activity-continuous-surface',
    );
  }

  function ensureSupplyActivityStats() {
    const activityCard = document.getElementById('supply-activity-card');
    const inner = activityCard?.querySelector('.table-card-inner');
    if (!inner) return null;
    // Legacy standalone Flow Snapshot card — its data now lives in this strip.
    document.getElementById('supply-flow-snapshot-card')?.remove();
    let stats = document.getElementById('supply-activity-stats');
    if (!stats) {
      stats = document.createElement('div');
      stats.id = 'supply-activity-stats';
      stats.className = 'supply-activity-stats';
      const header = inner.querySelector('.table-card-header');
      if (header) header.insertAdjacentElement('afterend', stats);
      else inner.insertBefore(stats, inner.firstChild);
    }
    return stats;
  }

  function isActivityAllTimePeriod(meta) {
    return !!meta && (meta.key === 'all' || meta.days == null);
  }

  function activityPeriodNeedsFullHistory(meta) {
    return !!meta && (isActivityAllTimePeriod(meta) || Number(meta.days || 0) > 30);
  }

  function supplyDraftEscape(value) {
    try {
      if (typeof supplyEscapeHtml === 'function') return supplyEscapeHtml(value == null ? '' : String(value));
    } catch (error) {}
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function supplyDraftFormatUsd(value) {
    const numeric = Number(value || 0);
    try {
      if (typeof formatUSDCompact === 'function') return formatUSDCompact(numeric);
    } catch (error) {}
    return `$${numeric.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
  }

  function supplyDraftFormatToken(value) {
    const numeric = Number(value || 0);
    try {
      if (typeof supplyFormatTokenCompact === 'function') return supplyFormatTokenCompact(numeric);
    } catch (error) {}
    return numeric.toLocaleString('en-US', { maximumFractionDigits: 2 });
  }

  function renderSupplyActivityStats() {
    const stats = ensureSupplyActivityStats();
    if (!stats || typeof summarizeSupplyActivityRows !== 'function') return;

    let rows = [];
    try {
      rows = Array.isArray(currentSupplyActivity) ? currentSupplyActivity : [];
    } catch (error) {
      return;
    }

    const meta = getActivityPeriodMeta();
    const nowTs = Math.floor(Date.now() / 1000);
    const cutoffTs = isActivityAllTimePeriod(meta) ? null : nowTs - (meta.days * 24 * 60 * 60);
    const summary = summarizeSupplyActivityRows(rows, cutoffTs);
    const isSyncingOlder = activityPeriodNeedsFullHistory(meta)
      && !!currentSupplyOverview?.activityFullLoading
      && currentSupplyOverview?.activityStage !== 'full';
    let tokenSymbol = '';
    try {
      tokenSymbol = currentSupplyOverview?.token?.symbol || '';
    } catch (error) {}

    const statsKey = JSON.stringify([
      'stats',
      activityPeriodKey,
      supplyActivityRowsVersion,
      rows.length,
      tokenSymbol,
      currentSupplyOverview?.activityStage || '',
      isSyncingOlder ? 'loading' : 'ready',
      summary.inflowUsd,
      summary.outflowUsd,
      summary.internalUsd,
      summary.wallets,
      summary.events,
      Math.floor(nowTs / 60),
    ]);
    if (stats.dataset.supplyStatsKey === statsKey && stats.children.length > 0) return;
    stats.dataset.supplyStatsKey = statsKey;

    const tokenSuffix = tokenSymbol ? ` ${tokenSymbol}` : '';
    const activitySub = value => isSyncingOlder ? 'syncing older tx…' : value;

    const statIcons = {
      deposits: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>',
      withdrawals: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>',
      transfers: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m17 2 4 4-4 4"/><path d="M3 11v-1a4 4 0 0 1 4-4h14"/><path d="m7 22-4-4 4-4"/><path d="M21 13v1a4 4 0 0 1-4 4H3"/></svg>',
      wallets: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
    };
    const cells = [
      {
        icon: statIcons.deposits,
        label: 'Deposits',
        value: supplyDraftFormatUsd(summary.inflowUsd),
        sub: activitySub(`${supplyDraftFormatToken(summary.inflowToken)}${tokenSuffix}`),
        cls: 'deposit',
      },
      {
        icon: statIcons.withdrawals,
        label: 'Withdrawals',
        value: supplyDraftFormatUsd(summary.outflowUsd),
        sub: activitySub(`${supplyDraftFormatToken(summary.outflowToken)}${tokenSuffix}`),
        cls: 'withdraw',
      },
      {
        icon: statIcons.transfers,
        label: 'Transfers',
        value: supplyDraftFormatUsd(summary.internalUsd),
        sub: activitySub(`${supplyDraftFormatToken(summary.internalToken)}${tokenSuffix}`),
        cls: 'transfer',
      },
      {
        icon: statIcons.wallets,
        label: 'Active Wallets',
        value: Number(summary.wallets || 0).toLocaleString('en-US'),
        sub: activitySub(`${Number(summary.events || 0).toLocaleString('en-US')} events`),
        cls: '',
      },
    ];

    stats.innerHTML = cells.map(cell => `
      <div class="supply-activity-stat ${cell.cls}">
        <div class="label">${cell.icon}${supplyDraftEscape(cell.label)}</div>
        <div class="value">${supplyDraftEscape(cell.value)}</div>
        <div class="sub">${supplyDraftEscape(cell.sub)}</div>
      </div>
    `).join('');
  }

  function enhanceSupplyHistoryShell() {
    const title = document.querySelector('.supply-history-title');
    if (title) title.textContent = 'Supply Liquidity Over Time';
    if (title && !title.closest('.supply-history-title-line')) {
      const line = document.createElement('div');
      line.className = 'supply-history-title-line';
      title.parentNode.insertBefore(line, title);
      line.appendChild(title);
    }
    const titleLine = document.querySelector('.supply-history-title-line');
    document.getElementById('supply-history-change-badge')?.remove();
    if (titleLine && !document.getElementById('supply-history-mode-switch')) {
      const switcher = document.createElement('div');
      switcher.id = 'supply-history-mode-switch';
      switcher.className = 'supply-history-mode-switch';
      switcher.innerHTML = `
        <button type="button" class="supply-history-mode-btn" data-history-mode="recent" onclick="supplySetHistoryDatasetMode('recent')">90D</button>
        <button type="button" class="supply-history-mode-btn" data-history-mode="all" onclick="supplySetHistoryDatasetMode('all')">All</button>
      `;
      titleLine.appendChild(switcher);
    }
  }

  function syncSupplyHistoryBadge() {
    document.getElementById('supply-history-change-badge')?.remove();
    syncSupplyHistoryModeControls();
  }

  function syncSupplyHistoryModeControls() {
    const mode = currentSupplyOverview?.historyDatasetMode || 'recent';
    const fullStage = currentSupplyOverview?.historyFullStage || 'idle';
    document.querySelectorAll('.supply-history-mode-btn').forEach(btn => {
      const isActive = btn.dataset.historyMode === mode;
      btn.classList.toggle('active', isActive);
      btn.classList.toggle('loading', btn.dataset.historyMode === 'all' && fullStage === 'loading');
      btn.disabled = btn.dataset.historyMode === 'all' && fullStage === 'loading';
      if (btn.dataset.historyMode === 'all') {
        btn.textContent = fullStage === 'loading' ? 'Loading' : 'All';
      }
    });
  }

  function polishSupplyHistoryChart() {
    const chart = document.getElementById('supply-history-chart');
    const brush = document.getElementById('supply-brush-svg');
    if (!chart) return;

    const lineGradient = chart.querySelector('#supplyLineGrad');
    if (lineGradient) {
      const stops = lineGradient.querySelectorAll('stop');
      const colors = ['rgba(201,162,39,0.48)', '#c9a227', '#e4c15a'];
      stops.forEach((stop, index) => stop.setAttribute('stop-color', colors[index] || '#c9a227'));
    }

    const areaGradient = chart.querySelector('#supplyAreaGradient');
    if (areaGradient) {
      const stops = areaGradient.querySelectorAll('stop');
      const colors = ['rgba(201,162,39,0.22)', 'rgba(201,162,39,0.06)', 'rgba(201,162,39,0)'];
      stops.forEach((stop, index) => stop.setAttribute('stop-color', colors[index] || 'rgba(201,162,39,0)'));
    }

    const hoverDot = document.getElementById('supply-history-hover-dot');
    if (hoverDot) hoverDot.setAttribute('fill', '#c9a227');

    if (brush) {
      const brushGradient = brush.querySelector('#brushGrad');
      if (brushGradient) {
        const stops = brushGradient.querySelectorAll('stop');
        const colors = ['rgba(201,162,39,0.30)', 'rgba(201,162,39,0)'];
        stops.forEach((stop, index) => stop.setAttribute('stop-color', colors[index] || 'rgba(201,162,39,0)'));
      }
      const brushLine = brush.querySelector('path[stroke]');
      if (brushLine) brushLine.setAttribute('stroke', 'rgba(201,162,39,0.58)');
    }
  }

  function getSupplyHistoryTooltipValue(point, metric) {
    const value = Number(point?.value || 0);
    try {
      if (metric === 'usd') return formatUSDCompact(point?.usdValue ?? value);
      const symbol = currentSupplyOverview?.token?.symbol || '';
      return `${supplyFormatTokenCompact(value)}${symbol ? ` ${symbol}` : ''}`;
    } catch (error) {
      return Number.isFinite(value) ? value.toLocaleString('en-US') : '—';
    }
  }

  function supplyDraftNonNegative(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
  }

  function getSupplyDraftVisibleHistoryPoints(points) {
    let visible = [];
    try {
      visible = getVisibleSupplyHistoryPoints(points || []);
    } catch (error) {
      visible = Array.isArray(points) ? points : [];
    }
    return visible.map(point => ({
      ...point,
      value: supplyDraftNonNegative(point?.value),
      usdValue: Number.isFinite(Number(point?.usdValue)) ? supplyDraftNonNegative(point.usdValue) : point?.usdValue,
      tokenValue: Number.isFinite(Number(point?.tokenValue)) ? supplyDraftNonNegative(point.tokenValue) : point?.tokenValue,
    }));
  }

  function getSupplyDraftHistoryScale(visiblePoints, historyMetric) {
    const width = 1000;
    const height = 320;
    const padRight = 20;
    const padTop = 20;
    const padBottom = 20;
    const values = visiblePoints.map(point => supplyDraftNonNegative(point?.value));
    const rawMin = Math.max(0, Math.min(...values));
    const rawMax = Math.max(0, Math.max(...values));
    const rawSpread = rawMax - rawMin;
    const vertPad = rawSpread > 0 ? rawSpread * 0.08 : Math.max(rawMax * 0.02, 1);
    const min = Math.max(0, rawMin - vertPad);
    const max = Math.max(rawMax + vertPad, min + Math.max(rawMax * 0.02, 1));
    const labels = Array.from({ length: 5 }).map((_, index) => {
      try {
        return supplyFormatHistoryAxisValue(max - ((max - min) * index / 4), historyMetric);
      } catch (error) {
        return '';
      }
    });
    const yLabelPad = Math.max(...labels.map(label => label.length), 0) * 7;
    const padLeft = Math.max(72, Math.min(120, yLabelPad + 20));
    const innerW = width - padLeft - padRight;
    const innerH = height - padTop - padBottom;
    const plotRight = width - padRight;
    const bottom = height - padBottom;
    const yAt = value => {
      const normalized = max > min ? (supplyDraftNonNegative(value) - min) / (max - min) : 0;
      const y = bottom - normalized * innerH;
      return Math.max(padTop, Math.min(bottom, y));
    };
    return { width, height, padTop, padBottom, padLeft, innerW, innerH, plotRight, bottom, yAt };
  }

  function stabilizeSupplyHistoryGeometry(points) {
    const chart = document.getElementById('supply-history-chart');
    const svg = chart?.querySelector('svg');
    const line = svg?.querySelector('path[stroke="url(#supplyLineGrad)"]');
    const area = svg?.querySelector('path[fill="url(#supplyAreaGradient)"]');
    if (!chart || !svg || !line || !area) return;
    const historyMetric = currentSupplyOverview?.historyMetric || 'token';
    const visiblePoints = getSupplyDraftVisibleHistoryPoints(points);
    if (visiblePoints.length < 2) return;
    const scale = getSupplyDraftHistoryScale(visiblePoints, historyMetric);
    const pathPoints = visiblePoints.map((point, index) => ({
      x: scale.padLeft + (scale.innerW * index) / Math.max(1, visiblePoints.length - 1),
      y: scale.yAt(point.value),
    }));
    const linePath = pathPoints.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(' ');
    const first = pathPoints[0];
    const last = pathPoints[pathPoints.length - 1];
    const areaPath = `${linePath} L ${last.x.toFixed(2)} ${scale.bottom.toFixed(2)} L ${first.x.toFixed(2)} ${scale.bottom.toFixed(2)} Z`;
    line.setAttribute('d', linePath);
    area.setAttribute('d', areaPath);
    line.dataset.supplyDraftStable = 'true';
    area.dataset.supplyDraftStable = 'true';
  }

  function stabilizeSupplyHistoryBrush(points) {
    const brush = document.getElementById('supply-history-brush');
    const svg = document.getElementById('supply-brush-svg');
    if (!brush || !svg || !Array.isArray(points) || points.length < 2) return;
    const safePoints = points.map(point => ({ ...point, value: supplyDraftNonNegative(point?.value) }));
    const bW = brush.offsetWidth || 800;
    const bH = brush.offsetHeight || 48;
    const values = safePoints.map(point => supplyDraftNonNegative(point.value));
    const bMin = Math.max(0, Math.min(...values));
    const bMax = Math.max(0, Math.max(...values));
    const bSpread = bMax - bMin || 1;
    const bPad = 4;
    const bIH = bH - bPad * 2;
    const yAt = value => {
      const y = bH - bPad - ((supplyDraftNonNegative(value) - bMin) / bSpread) * bIH;
      return Math.max(bPad, Math.min(bH - bPad, y));
    };
    const miniLine = safePoints.map((point, index) => {
      const x = (bW * index) / Math.max(1, safePoints.length - 1);
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${yAt(point.value).toFixed(1)}`;
    }).join(' ');
    const lastX = (bW * (safePoints.length - 1)) / Math.max(1, safePoints.length - 1);
    const miniArea = `${miniLine} L ${lastX.toFixed(1)} ${bH} L 0 ${bH} Z`;
    svg.setAttribute('viewBox', `0 0 ${bW} ${bH}`);
    svg.innerHTML = `<defs><linearGradient id="brushGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="rgba(201,162,39,0.30)"/><stop offset="100%" stop-color="rgba(201,162,39,0)"/></linearGradient></defs><path d="${miniArea}" fill="url(#brushGrad)" data-supply-draft-stable="true"/><path d="${miniLine}" fill="none" stroke="rgba(201,162,39,0.58)" stroke-width="1" data-supply-draft-stable="true"/>`;
  }

  function installSupplyHistoryCompactHover(points) {
    const chart = document.getElementById('supply-history-chart');
    const tooltip = document.getElementById('supply-history-tooltip');
    const svg = chart?.querySelector('svg');
    const hoverLine = document.getElementById('supply-history-hover-line');
    const hoverDot = document.getElementById('supply-history-hover-dot');
    if (!chart || !tooltip || !svg || !hoverLine || !hoverDot) return;

    const historyMetric = currentSupplyOverview?.historyMetric || 'token';
    const visiblePoints = getSupplyDraftVisibleHistoryPoints(points);
    if (visiblePoints.length < 2) return;

    tooltip.classList.add('supply-history-tooltip-compact');
    tooltip.innerHTML = `
      <div class="supply-history-tooltip-date"></div>
      <div class="supply-history-tooltip-primary"></div>
    `;
    const tipDate = tooltip.querySelector('.supply-history-tooltip-date');
    const tipPrimary = tooltip.querySelector('.supply-history-tooltip-primary');

    const scale = getSupplyDraftHistoryScale(visiblePoints, historyMetric);

    const pathPoints = visiblePoints.map((point, index) => {
      const x = scale.padLeft + (scale.innerW * index) / Math.max(1, visiblePoints.length - 1);
      return { x, y: scale.yAt(point.value) };
    });

    function hideHover() {
      hoverLine.setAttribute('opacity', '0');
      hoverDot.setAttribute('opacity', '0');
      tooltip.classList.remove('visible');
    }

    function showHover(index) {
      const point = pathPoints[index];
      const data = visiblePoints[index];
      if (!point || !data || !tipDate || !tipPrimary) return;
      hoverLine.setAttribute('x1', point.x.toFixed(2));
      hoverLine.setAttribute('x2', point.x.toFixed(2));
      hoverLine.setAttribute('opacity', '1');
      hoverDot.setAttribute('cx', point.x.toFixed(2));
      hoverDot.setAttribute('cy', point.y.toFixed(2));
      hoverDot.setAttribute('opacity', '1');
      try {
        tipDate.textContent = supplyFormatFullDate(data.timestamp);
      } catch (error) {
        tipDate.textContent = new Date(Number(data.timestamp || 0) * 1000).toLocaleDateString('en-US');
      }
      tipPrimary.textContent = getSupplyHistoryTooltipValue(data, historyMetric);

      const chartRect = chart.getBoundingClientRect();
      const svgRect = svg.getBoundingClientRect();
      const screenX = (svgRect.left - chartRect.left) + (point.x / scale.width) * svgRect.width;
      const screenY = (svgRect.top - chartRect.top) + (point.y / scale.height) * svgRect.height;
      tooltip.classList.add('visible');
      tooltip.style.left = '0px';
      tooltip.style.top = '0px';
      const tipW = tooltip.offsetWidth;
      const tipH = tooltip.offsetHeight;
      const left = Math.min(Math.max(screenX - tipW / 2, 8), chart.clientWidth - tipW - 8);
      const top = Math.max(screenY - tipH - 12, 4);
      tooltip.style.left = `${left}px`;
      tooltip.style.top = `${top}px`;
    }

    chart.onpointermove = event => {
      const svgRect = svg.getBoundingClientRect();
      const rx = ((event.clientX - svgRect.left) / svgRect.width) * scale.width;
      if (!Number.isFinite(rx) || rx < scale.padLeft || rx > scale.plotRight) {
        hideHover();
        return;
      }
      const index = Math.max(0, Math.min(visiblePoints.length - 1, Math.round(((rx - scale.padLeft) / scale.innerW) * (visiblePoints.length - 1))));
      showHover(index);
    };
    chart.onpointerleave = hideHover;
  }

  function syncSupplyHistoryHeaderCopy() {
    const meta = document.getElementById('supply-history-meta');
    const delta = document.getElementById('supply-history-delta');
    if (meta) meta.textContent = '';
    if (delta) {
      const mode = currentSupplyOverview?.historyDatasetMode || 'recent';
      const fullStage = currentSupplyOverview?.historyFullStage || 'idle';
      let label = mode === 'all' ? 'all history loaded' : '90D instant history';
      if (fullStage === 'loading') label = 'loading all history';
      if (fullStage === 'error' && mode === 'all') label = 'all history unavailable';
      delta.innerHTML = `<span class="supply-history-pulse"></span><span>${label} · drag below to zoom</span>`;
    }
  }

  function patchSupplyHistoryRenderer() {
    if (historyPatched || typeof window.renderSupplyHistoryChart !== 'function') return;
    historyPatched = true;
    const originalRenderSupplyHistoryChart = window.renderSupplyHistoryChart;
    window.renderSupplyHistoryChart = function supplyDraftRenderSupplyHistoryChart() {
      const result = originalRenderSupplyHistoryChart.apply(this, arguments);
      enhanceSupplyHistoryShell();
      syncSupplyHistoryBadge();
      syncSupplyHistoryHeaderCopy();
      polishSupplyHistoryChart();
      stabilizeSupplyHistoryGeometry(arguments[0] || []);
      stabilizeSupplyHistoryBrush(arguments[0] || []);
      installSupplyHistoryCompactHover(arguments[0] || []);
      return result;
    };
  }

  function enhanceSupplyLeaderboardShell() {
    const header = document.querySelector('.supply-table-header');
    const heading = document.querySelector('.supply-table-heading');
    const search = document.getElementById('supply-search-wrap');
    const inner = document.getElementById('supply-table')?.closest('.table-card-inner');
    if (!header || !heading || !search || !inner) return;

    if (!inner.querySelector('.supply-leaderboard-toolbar')) {
      const toolbar = document.createElement('div');
      toolbar.className = 'supply-leaderboard-toolbar';
      toolbar.innerHTML = '<div class="tb-left"></div><div class="tb-right"></div>';
      header.insertAdjacentElement('afterend', toolbar);
    }

    const left = inner.querySelector('.supply-leaderboard-toolbar .tb-left');
    if (left && search.parentElement !== left) {
      left.appendChild(search);
    }

    polishSearchClear('supply-search-clear');
  }

  function polishSearchClear(id) {
    const button = document.getElementById(id);
    if (!button || button.dataset.supplyDraftPolished === 'true') return;
    button.dataset.supplyDraftPolished = 'true';
    button.innerHTML = clearIcon;
    button.removeAttribute('style');
  }

  function polishSupplyHeaders() {
    const table = document.getElementById('supply-table');
    if (!table) return;
    table.classList.add('supply-leaderboard-table');

    const colgroup = table.querySelector('colgroup');
    if (colgroup && colgroup.dataset.supplyDraftCols !== 'true') {
      colgroup.dataset.supplyDraftCols = 'true';
      colgroup.innerHTML = `
        <col class="supply-rank-col">
        <col class="supply-wallet-col">
        <col class="supply-amount-col">
        <col class="supply-usd-col">
      `;
    }

    const labels = {
      rank: '#',
      address: 'Address',
      amount: 'Supply Amount',
      usd: 'USD',
    };
    table.querySelectorAll('thead th[data-sort]').forEach(th => {
      const key = th.dataset.sort;
      const label = labels[key] || th.textContent.trim();
      let isActive = false;
      let isAsc = false;
      try {
        isActive = supplySortField === key;
        isAsc = !!supplySortAsc;
      } catch (error) {}
      th.removeAttribute('style');
      th.removeAttribute('onclick');
      th.onclick = () => window.supplySort(key);
      th.classList.add('sortable');
      th.classList.toggle('num', key === 'amount' || key === 'usd');
      th.classList.toggle('sorted', isActive);
      th.setAttribute('aria-sort', isActive ? (isAsc ? 'ascending' : 'descending') : 'none');
      th.innerHTML = `<span class="th-content">${label} <span class="sort-arrow">${isActive ? (isAsc ? '▲' : '▼') : ''}</span></span>`;
    });
  }

  function polishSupplyRows() {
    const table = document.getElementById('supply-table');
    if (!table) return;
    table.querySelectorAll('tbody tr').forEach(row => {
      if (row.cells.length < 4 || row.querySelector('[colspan]')) return;
      row.classList.add('supply-leaderboard-row');
      row.onmouseenter = null;
      row.onmouseleave = null;

      const rankCell = row.cells[0];
      const walletCell = row.cells[1];
      const amountCell = row.cells[2];
      const usdCell = row.cells[3];
      rankCell.classList.add('supply-rank-cell');
      walletCell.classList.add('supply-wallet-cell');
      amountCell.classList.add('num', 'supply-amount-cell');
      usdCell.classList.add('num', 'supply-usd-cell');

      const addressWrap = walletCell.querySelector('.address-cell');
      if (addressWrap) {
        addressWrap.classList.add('supply-wallet-address');
        addressWrap.removeAttribute('style');
      }

      walletCell.querySelectorAll('.addr-tooltip-wrap').forEach(link => {
        if (link.classList.contains('known-address-label')) {
          link.classList.remove('addr-mono');
        } else {
          link.classList.add('addr-mono');
        }
        link.removeAttribute('style');
      });
      walletCell.querySelectorAll('.copy-addr-icon').forEach(icon => {
        icon.removeAttribute('title');
        icon.setAttribute('aria-label', 'Copy address');
        icon.setAttribute('role', 'button');
      });
      walletCell.querySelectorAll('.debank-icon').forEach(link => {
        link.removeAttribute('title');
        link.setAttribute('aria-label', 'Open DeBank profile');
      });

      const valueWrap = amountCell.querySelector('div');
      const amountText = valueWrap?.querySelector('span');
      if (valueWrap) {
        const amountTip = valueWrap.dataset.tooltip || amountCell.dataset.tooltip || amountCell.getAttribute('title');
        valueWrap.classList.add('supply-token-value');
        valueWrap.dataset.tooltipCursor = 'true';
        if (amountTip) valueWrap.dataset.tooltip = amountTip;
        amountCell.removeAttribute('title');
        amountCell.removeAttribute('data-tooltip');
      }
      if (amountText) amountText.classList.add('supply-token-amount');
    });
  }

  function polishSupplyActivityRows() {
    const table = document.getElementById('supply-activity-table');
    if (!table) return;
    table.classList.add('supply-activity-polished-table');
    table.querySelectorAll('tbody tr').forEach(row => {
      if (row.cells.length < 5 || row.querySelector('[colspan]')) return;
      row.classList.add('supply-activity-polished-row');
      row.querySelectorAll('.copy-addr-icon').forEach(icon => {
        icon.removeAttribute('title');
        icon.removeAttribute('data-tooltip');
        icon.setAttribute('aria-label', 'Copy address');
        icon.setAttribute('role', 'button');
      });
      row.querySelectorAll('.supply-activity-link').forEach(link => {
        link.removeAttribute('title');
        link.removeAttribute('data-tooltip');
        link.setAttribute('aria-label', 'Open transaction');
      });
      row.querySelectorAll('.debank-icon').forEach(link => {
        link.removeAttribute('title');
        link.removeAttribute('data-tooltip');
        link.setAttribute('aria-label', 'Open DeBank profile');
      });
      row.querySelectorAll('.addr-tooltip-wrap').forEach(link => {
        if (link.classList.contains('known-address-label')) {
          link.classList.remove('addr-mono');
        } else {
          link.classList.add('addr-mono');
        }
        link.removeAttribute('style');
      });
      row.querySelectorAll('.supply-activity-wallet-meta').forEach(meta => {
        meta.remove();
      });
      row.querySelectorAll('.supply-activity-amount-sub').forEach(sub => {
        sub.remove();
      });
      row.querySelectorAll('.supply-activity-token-value').forEach(value => {
        value.dataset.tooltipCursor = 'true';
      });
    });
  }

  function polishSupplyActivityHeaders() {
    const table = document.getElementById('supply-activity-table');
    if (!table) return;
    table.classList.add('supply-activity-polished-table');
    const colgroup = table.querySelector('colgroup');
    if (colgroup && colgroup.dataset.supplyDraftCols !== 'true') {
      colgroup.dataset.supplyDraftCols = 'true';
      colgroup.innerHTML = `
        <col class="activity-time-col">
        <col class="activity-wallet-col">
        <col class="activity-type-col">
        <col class="activity-amount-col">
        <col class="activity-usd-col">
      `;
    }
    table.querySelectorAll('thead th').forEach(th => {
      th.removeAttribute('style');
      th.classList.toggle('num', th.dataset.sort === 'amount' || th.dataset.sort === 'usd');
      if (th.dataset.sort) th.classList.add('sortable');
    });
    const dateHeader = table.querySelector('thead th[data-sort="time"] .col-header-group');
    const dateHeaderText = dateHeader
      ? Array.from(dateHeader.childNodes).find(node => node.nodeType === Node.TEXT_NODE && node.textContent.trim())
      : null;
    if (dateHeaderText) dateHeaderText.textContent = 'DATE';
    table.querySelectorAll('.col-filter-btn, .col-filter-clear').forEach(button => {
      button.removeAttribute('title');
      button.removeAttribute('data-tooltip');
      button.setAttribute('aria-label', button.classList.contains('col-filter-clear') ? 'Clear filter' : 'Open filter');
    });
  }

  function getActivityFilterSet() {
    try {
      if (typeof supplyActivityFilters !== 'undefined' && supplyActivityFilters instanceof Set) {
        return supplyActivityFilters;
      }
    } catch (error) {}
    return new Set(activityTypeOptions.map(option => option.type));
  }

  function syncActivityTypeDropdown() {
    const dropdown = document.getElementById('supply-activity-type-filter');
    if (!dropdown) return;
    const active = getActivityFilterSet();
    const activeCount = activityTypeOptions.filter(option => active.has(option.type)).length;
    const label = dropdown.querySelector('.supply-activity-type-label');
    const count = dropdown.querySelector('.supply-activity-type-count');
    if (label) {
      label.textContent = activeCount === activityTypeOptions.length
        ? 'All activity'
        : activeCount === 1
          ? (activityTypeOptions.find(option => active.has(option.type))?.label || 'Activity')
          : 'Activity types';
    }
    if (count) count.textContent = `${activeCount}/${activityTypeOptions.length}`;
    dropdown.classList.toggle('active-filter', activeCount !== activityTypeOptions.length);
    dropdown.querySelectorAll('.supply-activity-type-option').forEach(option => {
      const type = option.dataset.type;
      const isActive = active.has(type);
      option.classList.toggle('active', isActive);
      option.setAttribute('aria-checked', isActive ? 'true' : 'false');
    });
  }

  function resetActivityTypeDropdown() {
    const allTypes = activityTypeOptions.map(option => option.type);
    try {
      if (typeof supplyActivityFilters !== 'undefined' && supplyActivityFilters instanceof Set) {
        supplyActivityFilters = new Set(allTypes);
      }
    } catch (error) {}
    document.querySelectorAll('.supply-filter-pill').forEach(btn => {
      btn.classList.toggle('active', allTypes.includes(btn.dataset.type));
    });
    try {
      if (typeof supplyGoActivityPage === 'function') supplyGoActivityPage(1);
      else if (typeof renderSupplyActivityTable === 'function') renderSupplyActivityTable();
    } catch (error) {}
    syncActivityTypeDropdown();
  }

  function getActivityPeriodMeta(key = activityPeriodKey) {
    return activityPeriodOptions.find(option => option.key === key) || activityPeriodOptions[2];
  }

  function getActivityPeriodCutoffTs() {
    const meta = getActivityPeriodMeta();
    if (isActivityAllTimePeriod(meta)) return null;
    const nowRoundedToMinute = Math.floor(Date.now() / 60000) * 60;
    return nowRoundedToMinute - (meta.days * 24 * 60 * 60);
  }

  function applyActivityPeriodFilter() {
    try {
      const cutoffTs = getActivityPeriodCutoffTs();
      supplyActivityTimeMin = cutoffTs;
      supplyActivityTimeMax = null;
      const timeTrigger = document.getElementById('supply-activity-time-filter-trigger');
      timeTrigger?.classList.toggle('has-active', cutoffTs != null);
    } catch (error) {}
  }

  function syncActivityPeriodDropdown() {
    const dropdown = document.getElementById('supply-activity-period-filter');
    if (!dropdown) return;
    const meta = getActivityPeriodMeta();
    dropdown.querySelector('.supply-activity-type-label').textContent = meta.short;
    dropdown.querySelectorAll('.supply-activity-type-option').forEach(option => {
      const isActive = option.dataset.period === activityPeriodKey;
      option.classList.toggle('active', isActive);
      option.setAttribute('aria-checked', isActive ? 'true' : 'false');
    });
  }

  function maybeLoadFullActivityForPeriod() {
    const meta = getActivityPeriodMeta();
    if (!activityPeriodNeedsFullHistory(meta)) return;
    try {
      if (currentSupplyOverview?.activityStage !== 'full' && !currentSupplyOverview?.activityFullLoading && typeof supplyLoadFullActivityHistory === 'function') {
        supplyLoadFullActivityHistory();
      }
    } catch (error) {}
  }

  function selectActivityPeriod(key) {
    if (!activityPeriodOptions.some(option => option.key === key)) return;
    activityPeriodKey = key;
    applyActivityPeriodFilter();
    syncActivityPeriodDropdown();
    maybeLoadFullActivityForPeriod();
    try {
      supplyActivityPage = 1;
      if (typeof renderSupplyActivityTable === 'function') renderSupplyActivityTable();
    } catch (error) {}
  }

  function installActivityTypeDropdown() {
    const toolbar = document.querySelector('.supply-activity-toolbar');
    const main = document.querySelector('.supply-activity-toolbar-main');
    const row = document.querySelector('.supply-activity-filter-row');
    if (!toolbar || !main || !row) return;
    row.classList.add('is-dropdown');

    const searchWrap = document.getElementById('supply-activity-search-wrap');
    const history = document.querySelector('.supply-activity-history-inline');
    toolbar.classList.add('supply-activity-toolbar-polished');
    if (history) {
      history.classList.add('supply-activity-toolbar-actions');
      if (history.parentElement !== toolbar) {
        toolbar.appendChild(history);
      }
    }

    let dropdown = document.getElementById('supply-activity-type-filter');
    if (!dropdown) {
      dropdown = document.createElement('div');
      dropdown.id = 'supply-activity-type-filter';
      dropdown.className = 'supply-activity-type-filter';
      dropdown.innerHTML = `
        <button type="button" class="supply-activity-type-trigger" aria-haspopup="menu" aria-expanded="false">
          <span class="supply-activity-type-label">All activity</span>
          <span class="supply-activity-type-count">4/4</span>
          <span class="supply-activity-type-clear" role="button" aria-label="Clear activity filter">${clearIcon}</span>
          ${chevronIcon}
        </button>
        <div class="supply-activity-type-menu" role="menu">
          ${activityTypeOptions.map(option => `
            <button type="button" class="supply-activity-type-option" data-type="${option.type}" role="menuitemcheckbox" aria-checked="true">
              <span class="supply-activity-type-check" aria-hidden="true">
                <svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="2 6 5 9 10 3"/></svg>
              </span>
              <span class="supply-activity-type-option-label">${option.label}</span>
            </button>
          `).join('')}
        </div>
      `;

      const trigger = dropdown.querySelector('.supply-activity-type-trigger');
      trigger?.addEventListener('click', event => {
        if (event.target.closest('.supply-activity-type-clear')) return;
        event.stopPropagation();
        const isOpen = dropdown.classList.toggle('open');
        trigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      });
      dropdown.querySelector('.supply-activity-type-clear')?.addEventListener('click', event => {
        event.preventDefault();
        event.stopPropagation();
        resetActivityTypeDropdown();
      });

      dropdown.querySelectorAll('.supply-activity-type-option').forEach(option => {
        option.addEventListener('click', event => {
          event.preventDefault();
          event.stopPropagation();
          const type = option.dataset.type;
          let toggleFn = null;
          try {
            if (typeof toggleSupplyActivityType === 'function') toggleFn = toggleSupplyActivityType;
          } catch (error) {}
          if (!toggleFn && typeof window.toggleSupplyActivityType === 'function') {
            toggleFn = window.toggleSupplyActivityType;
          }
          if (type && toggleFn) {
            toggleFn(type);
          }
          syncActivityTypeDropdown();
        });
      });
    }

    if (searchWrap) {
      searchWrap.insertAdjacentElement('afterend', dropdown);
    } else if (dropdown.parentElement !== main) {
      main.appendChild(dropdown);
    }

    let periodDropdown = document.getElementById('supply-activity-period-filter');
    if (!periodDropdown) {
      periodDropdown = document.createElement('div');
      periodDropdown.id = 'supply-activity-period-filter';
      periodDropdown.className = 'supply-activity-type-filter supply-activity-period-filter';
      periodDropdown.innerHTML = `
        <button type="button" class="supply-activity-type-trigger" aria-haspopup="menu" aria-expanded="false">
          <svg class="supply-activity-period-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>
          <span class="supply-activity-type-label">30D</span>
          ${chevronIcon}
        </button>
        <div class="supply-activity-type-menu" role="menu">
          ${activityPeriodOptions.map(option => `
            <button type="button" class="supply-activity-type-option" data-period="${option.key}" role="menuitemradio" aria-checked="${option.key === activityPeriodKey ? 'true' : 'false'}">
              <span class="supply-activity-type-check" aria-hidden="true">
                <svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="2 6 5 9 10 3"/></svg>
              </span>
              <span class="supply-activity-type-option-label">${option.label}</span>
            </button>
          `).join('')}
        </div>
      `;

      const periodTrigger = periodDropdown.querySelector('.supply-activity-type-trigger');
      periodTrigger?.addEventListener('click', event => {
        event.stopPropagation();
        const isOpen = periodDropdown.classList.toggle('open');
        periodTrigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      });
      periodDropdown.querySelectorAll('.supply-activity-type-option').forEach(option => {
        option.addEventListener('click', event => {
          event.preventDefault();
          event.stopPropagation();
          selectActivityPeriod(option.dataset.period);
          periodDropdown.classList.remove('open');
          periodTrigger?.setAttribute('aria-expanded', 'false');
        });
      });
    }

    moveActivityPeriodToToolbarActions(periodDropdown);

    if (toolbar.dataset.supplyDraftClickClose !== 'true') {
      toolbar.dataset.supplyDraftClickClose = 'true';
      document.addEventListener('click', event => {
        document.querySelectorAll('.supply-activity-type-filter.open').forEach(current => {
          if (current.contains(event.target)) return;
          current.classList.remove('open');
          current.querySelector('.supply-activity-type-trigger')?.setAttribute('aria-expanded', 'false');
        });
      });
    }

    applyActivityPeriodFilter();
    syncActivityTypeDropdown();
    syncActivityPeriodDropdown();
  }

  function moveActivityPeriodToToolbarActions(periodDropdown) {
    const actions = document.querySelector('.supply-activity-toolbar-actions');
    if (!periodDropdown || !actions || periodDropdown.parentElement === actions) return;
    actions.appendChild(periodDropdown);
  }

  function stripSupplyActivityHoverExplanations() {
    document.querySelectorAll([
      '#supply-activity-table .col-filter-btn',
      '#supply-activity-table .col-filter-clear',
      '#supply-activity-table .supply-activity-link',
      '#supply-activity-table .copy-addr-icon',
    ].join(',')).forEach(el => {
      el.removeAttribute('title');
      el.removeAttribute('data-tooltip');
    });
  }

  function buildSupplyTableFooter(page, totalPages, totalRows, perPage, pageHandler, noun) {
    const total = Math.max(0, Number(totalRows) || 0);
    const currentPage = Math.max(1, Math.min(Number(page) || 1, Math.max(1, totalPages)));
    const start = total ? ((currentPage - 1) * perPage) + 1 : 0;
    const end = total ? Math.min(currentPage * perPage, total) : 0;
    const disabledPrev = currentPage === 1 ? 'disabled' : '';
    const disabledNext = currentPage === totalPages ? 'disabled' : '';
    return `
      <span class="supply-page-range">${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}</span>
      <span class="supply-pager-controls">
        <button class="flow-pager-btn" aria-label="First page" onclick="${pageHandler}(1)" ${disabledPrev}>«</button>
        <button class="flow-pager-btn" aria-label="Previous page" onclick="${pageHandler}(${currentPage - 1})" ${disabledPrev}>‹</button>
        <span class="flow-pager-info">${currentPage} / ${totalPages}</span>
        <button class="flow-pager-btn" aria-label="Next page" onclick="${pageHandler}(${currentPage + 1})" ${disabledNext}>›</button>
        <button class="flow-pager-btn" aria-label="Last page" onclick="${pageHandler}(${totalPages})" ${disabledNext}>»</button>
      </span>
      <span class="flow-pager-total">${total.toLocaleString()} ${noun}</span>
    `;
  }

  function renderSupplyDraftActivityPagination(totalLen) {
    const el = document.getElementById('supply-activity-pagination');
    if (!el) return;
    let page = 1;
    let perPage = 10;
    try {
      page = supplyActivityPage || 1;
      perPage = SUPPLY_ACTIVITY_PAGE_SIZE || 10;
    } catch (error) {}
    const totalPages = Math.max(1, Math.ceil((Number(totalLen) || 0) / perPage));
    page = Math.max(1, Math.min(page, totalPages));
    el.innerHTML = buildSupplyTableFooter(
      page,
      totalPages,
      totalLen,
      perPage,
      'supplyGoActivityPage',
      'events',
    );
  }

  function polishSupplyActivityPagination() {
    try {
      if (typeof getFilteredSupplyActivityRows === 'function') {
        renderSupplyDraftActivityPagination(getFilteredSupplyActivityRows().length);
      }
    } catch (error) {}
  }

  function polishSupplyActivityUi() {
    polishSearchClear('supply-activity-search-clear');
    renderSupplyActivityStats();
    polishSupplyActivityHeaders();
    polishSupplyActivityRows();
    stripSupplyActivityHoverExplanations();
    installActivityTypeDropdown();
    polishSupplyActivityPagination();
  }

  function renderSupplyDraftPagination(totalLen) {
    const el = document.getElementById('supply-pagination');
    if (!el) return;
    let page = 1;
    let perPage = 10;
    try {
      page = supplyPage || 1;
      perPage = SUPPLY_PER_PAGE || 10;
    } catch (error) {}
    const totalPages = Math.max(1, Math.ceil((Number(totalLen) || 0) / perPage));
    page = Math.max(1, Math.min(page, totalPages));
    el.innerHTML = buildSupplyTableFooter(
      page,
      totalPages,
      totalLen,
      perPage,
      'supply_goPage',
      'wallets',
    );
  }

  function patchSupplyTableRenderer() {
    if (tablePatched || typeof window.renderSupplyTable !== 'function') return;
    tablePatched = true;
    const originalRenderSupplyTable = window.renderSupplyTable;
    window.renderSupplyPagination = renderSupplyDraftPagination;
    window.renderSupplyTable = function supplyDraftRenderSupplyTable() {
      const result = originalRenderSupplyTable.apply(this, arguments);
      enhanceSupplyLeaderboardShell();
      polishSupplyHeaders();
      polishSupplyRows();
      return result;
    };
  }

  function patchSupplyActivityRenderer() {
    if (activityPatched || typeof window.renderSupplyActivityTable !== 'function') return;
    activityPatched = true;
    const originalRenderSupplyActivityTable = window.renderSupplyActivityTable;
    window.renderSupplyActivityTable = function supplyDraftRenderSupplyActivityTable() {
      applyActivityPeriodFilter();
      const result = originalRenderSupplyActivityTable.apply(this, arguments);
      maybeLoadFullActivityForPeriod();
      polishSupplyActivityUi();
      return result;
    };
  }

  function patchSupplySort() {
    if (sortPatched || typeof window.supplySort !== 'function') return;
    sortPatched = true;
    const originalSupplySort = window.supplySort;
    window.supplySort = function supplyDraftSort(field) {
      try {
        if (supplySortField === field) {
          supplySortAsc = !supplySortAsc;
        } else {
          supplySortField = field;
          supplySortAsc = field === 'rank';
        }
        supplyPage = 1;
        renderSupplyTable();
      } catch (error) {
        return originalSupplySort.apply(this, arguments);
      }
    };
  }

  function polishExistingSupplyUi() {
    enhanceSupplyHistoryShell();
    syncSupplyHistoryBadge();
    enhanceSupplyLeaderboardShell();
    polishSupplyHeaders();
    polishSupplyRows();
    polishSearchClear('supply-activity-search-clear');
    renderSupplyActivityStats();
    polishSupplyActivityUi();
  }

  function installEmptyState() {
    const frame = document.querySelector('#tab-supply .premium-supply-frame');
    if (!frame || document.querySelector('.supply-draft-empty')) return;

    const empty = document.createElement('section');
    empty.className = 'supply-draft-empty';
    empty.innerHTML = `
      <div class="supply-draft-empty-inner">
        <div class="supply-draft-empty-icon">${searchIcon}</div>
        <div>
          <div class="supply-draft-empty-title">Select an asset to open market overview</div>
          <div class="supply-draft-empty-copy">The leaderboard, liquidity chart, and activity feed will load after the market is chosen.</div>
        </div>
      </div>
    `;
    frame.insertAdjacentElement('afterend', empty);
  }

  function getSupplyToken(id) {
    try {
      if (typeof currentSupplyTokensMap !== 'undefined' && currentSupplyTokensMap && currentSupplyTokensMap[id]) {
        return currentSupplyTokensMap[id];
      }
    } catch (error) {}
    return null;
  }

  function getTokenSupplyLiquidityUsd(token) {
    return Math.max(0, Number.parseFloat(token?.supplyLiquidityUSD || '0') || 0);
  }

  function getLargestSupplyToken(tokens) {
    const safeTokens = Array.isArray(tokens) ? tokens : [];
    return safeTokens
      .slice()
      .sort((a, b) => {
        const supplyDiff = getTokenSupplyLiquidityUsd(b) - getTokenSupplyLiquidityUsd(a);
        if (Math.abs(supplyDiff) > 1e-9) return supplyDiff;
        return (Number.parseInt(a?.marketId || '0', 10) || 0) - (Number.parseInt(b?.marketId || '0', 10) || 0);
      })[0] || null;
  }

  function getDefaultSupplyToken() {
    const matchesDefault = token => String(token?.symbol || '').toUpperCase() === defaultSupplyAssetSymbol;
    try {
      if (typeof currentSupplyTokensList !== 'undefined' && Array.isArray(currentSupplyTokensList)) {
        if (defaultSupplyAssetSymbol) {
          const token = currentSupplyTokensList.find(matchesDefault);
          if (token) return token;
        }
        const token = getLargestSupplyToken(currentSupplyTokensList);
        if (token) return token;
      }
    } catch (error) {}
    try {
      if (typeof currentSupplyTokensMap !== 'undefined' && currentSupplyTokensMap) {
        const tokens = Object.values(currentSupplyTokensMap);
        if (defaultSupplyAssetSymbol) {
          const token = tokens.find(matchesDefault);
          if (token) return token;
        }
        return getLargestSupplyToken(tokens);
      }
    } catch (error) {}
    return null;
  }

  function syncAppliedAssetFromHidden() {
    if (document.body.classList.contains('supply-has-pending-asset')) return false;
    const hidden = document.getElementById('supply-asset-select');
    const selectedId = hidden?.value || '';
    const token = selectedId ? getSupplyToken(selectedId) : null;
    if (!selectedId || !token) return false;
    stagedAssetId = selectedId;
    appliedAssetId = selectedId;
    document.body.classList.remove('supply-has-pending-asset');
    setAssetState(true);
    setSelectorUi(token, false);
    syncApplyButton();
    return true;
  }

  function autoApplyDefaultSupplyAsset() {
    if (autoApplyArmedDefaultSupplyAsset()) return;
    if (appliedAssetId || syncAppliedAssetFromHidden()) return;
    const token = getDefaultSupplyToken();
    if (!token?.id) return;
    if (!originalSelectAsset) captureOriginalSelectAsset();
    if (!originalSelectAsset) return;
    stagedAssetId = token.id;
    appliedAssetId = token.id;
    document.body.classList.remove('supply-has-pending-asset');
    setAssetState(true);
    setSelectorUi(token, false);
    originalSelectAsset.call(window, token.id, { auto: true });
    syncApplyButton();
  }

  function autoApplyArmedDefaultSupplyAsset() {
    if (!chainDefaultAutoApplyArmed || !stagedAssetId || stagedAssetId === appliedAssetId) return false;
    chainDefaultAutoApplyArmed = false;
    applyStagedAsset({ auto: true });
    return true;
  }

  // Official Dolomite icons used by isolation-market asset selectors.
  const SUPPLY_ASSET_ICON_CDN = 'https://app.dolomite.io/static/media/';
  const supplyAssetIconOverrides = {
    'arbitrum:0x2c799166c9f0dbf9efc5004cbce4c5a37fa39329': SUPPLY_ASSET_ICON_CDN + 'ARB-GM.50df3ed4a1a52b938992cb5e08efbc36.svg',
    'arbitrum:0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef': SUPPLY_ASSET_ICON_CDN + 'WBTC-GM.6e7f69538bb02b42b881b86aea5c6d6e.svg',
    'arbitrum:0x505582242757f16d72f8c4462a616e388ca1b074': SUPPLY_ASSET_ICON_CDN + 'ETH-GM.0b7d447f3c11298af07411c926352c71.svg',
    'arbitrum:0x18cb14564fbb015bd3439220d177799355abc0e0': SUPPLY_ASSET_ICON_CDN + 'LINK-GM.7d4b33346ec9822f9dc7c22a393f7698.svg',
    'arbitrum:0xb15bbbfcff6c411410c66642306d1ffa7ecec4d8': SUPPLY_ASSET_ICON_CDN + 'WBTC-GM.6e7f69538bb02b42b881b86aea5c6d6e.svg',
    'arbitrum:0x2d165a76dd3e552df3860789331ab73c5a3d7f92': SUPPLY_ASSET_ICON_CDN + 'ETH-GM.0b7d447f3c11298af07411c926352c71.svg',
    'arbitrum:0x20d51cb520c4622dcc3d7e35003dbab07d547e7e': SUPPLY_ASSET_ICON_CDN + 'UNI-GM.8a4dfd0dc79f5b60138039338d28a6c7.svg',
    'arbitrum:0x24c9121c75c099b38d40020872b8a0d2c27c614d': SUPPLY_ASSET_ICON_CDN + 'gmAAVE.e032a2febd818f511cf782e09b12f212.svg',
    'arbitrum:0x1beed3b7d1237b7773b5c4c249933e3ca5e027c1': SUPPLY_ASSET_ICON_CDN + 'gmDOGE.36090e2ebcd305890c779e005d41d331.svg',
    'arbitrum:0x5c99f6cf6069698d234d50bf69ebd2f53e45ed1c': SUPPLY_ASSET_ICON_CDN + 'gmGMX.2c5cb2e0f1769629b38580607b77ecbc.svg',
    'arbitrum:0x1ebb1c7023addbb2b6e30e6f4c8d4a4440bfd412': SUPPLY_ASSET_ICON_CDN + 'gmSOL.73d56a4a2dcf3d39fc5c946b8c65c631.svg',
    'arbitrum:0xc587646f67b38739006ed0200e2e0a26fdb01c9b': SUPPLY_ASSET_ICON_CDN + 'wstETH.2e97640d284bbe78da3776549d27ec47.svg',
    'arbitrum:0xcf248baf933c7b1b876b997246f25021a65383b3': SUPPLY_ASSET_ICON_CDN + 'gmGMX.2c5cb2e0f1769629b38580607b77ecbc.svg',
    'arbitrum:0xe5d6fe410c69b44c357403a1936b3bfaddbe340b': SUPPLY_ASSET_ICON_CDN + 'gmPENDLE.cd8acede00414f70056c0fb9aa2baa7c.svg',
    'arbitrum:0x6586f1db71513daf94b0431156d225a46c00f20b': SUPPLY_ASSET_ICON_CDN + 'gmPEPE.966f4beb1b823729066c29c52921b664.svg',
    'arbitrum:0xf5063b40fa66ab2fbda2e6807ac5759a41a1b0c3': SUPPLY_ASSET_ICON_CDN + 'gmWIF.8dfcfc27c0c56651a2e523e97c7fdcb4.svg',
    'arbitrum:0x7e584529bb40220a2bd5d0c13e3d65abd4a47f0e': SUPPLY_ASSET_ICON_CDN + 'GLV-BTC.c576682a1343bbfde84710a572b5a68e.svg',
    'arbitrum:0x11f4532c05fb8ea6320b1dc155bfdc2498a5d8b4': SUPPLY_ASSET_ICON_CDN + 'GLV-ETH.092b4c8a9412efd58d3542d26bc5a522.svg',
    'arbitrum:0x51fc0f6660482ea73330e414efd7808811a57fa2': SUPPLY_ASSET_ICON_CDN + 'PREMIA.6c5c2339f3179353bb163b4e53d8dfa1.svg',
    'berachain:0xe946dd7d03f6f5c440f68c84808ca88d26475fc5': SUPPLY_ASSET_ICON_CDN + 'WBTC.f3c8718835179e7543b5.png',
    'berachain:0x1fcca65fb6ae3b2758b9b2b394cb227eae404e1e': SUPPLY_ASSET_ICON_CDN + 'PumpBTC.aa48de36289e8439daf0456c4252dd27.svg',
  };
  const supplyAssetSymbolIconFallbacks = {
    stBTC: SUPPLY_ASSET_ICON_CDN + 'stBTC.3935aab6a35bd55630f244a1f56631ba.svg',
    rswETH: SUPPLY_ASSET_ICON_CDN + 'rswETH.fc4bdb76a764bf110676766fd0185dfe.svg',
    'pumpBTC.bera': SUPPLY_ASSET_ICON_CDN + 'PumpBTC.aa48de36289e8439daf0456c4252dd27.svg',
    PREMIA: SUPPLY_ASSET_ICON_CDN + 'PREMIA.6c5c2339f3179353bb163b4e53d8dfa1.svg',
    ylBTCLST: SUPPLY_ASSET_ICON_CDN + 'WBTC.f3c8718835179e7543b5.png',
    'SolvBTC.BBN': SUPPLY_ASSET_ICON_CDN + 'solvBTC.326d594ebd54e4317f078b70f72a58b4.svg',
  };


  function getIconPath(token) {
    const iconKey = token?.chain && token?.tokenId
      ? `${String(token.chain).toLowerCase()}:${String(token.tokenId).toLowerCase()}`
      : '';
    if (iconKey && supplyAssetIconOverrides[iconKey]) {
      return supplyAssetIconOverrides[iconKey];
    }
    try {
      if (token && typeof getTokenIcon === 'function' && typeof truncateTokenName === 'function') {
        return getTokenIcon(token.symbol)
          || getTokenIcon(truncateTokenName(token.symbol))
          || supplyAssetSymbolIconFallbacks[token.symbol]
          || '';
      }
    } catch (error) {}
    return supplyAssetSymbolIconFallbacks[token?.symbol] || '';
  }

  function setSelectorUi(token, pending) {
    const text = document.getElementById('selected-asset-text');
    const icon = document.getElementById('selected-asset-icon');
    if (!token || !text) return;
    text.textContent = token.symbol;
    const iconPath = getIconPath(token);
    if (icon && iconPath) {
      icon.src = iconPath;
      icon.style.display = 'block';
    } else if (icon) {
      icon.style.display = 'none';
    }
    document.body.classList.toggle('supply-has-pending-asset', !!pending);
  }

  function syncEmptyState() {
    const title = document.querySelector('.supply-draft-empty-title');
    const copy = document.querySelector('.supply-draft-empty-copy');
    if (!title || !copy) return;
    const token = stagedAssetId ? getSupplyToken(stagedAssetId) : null;
    if (token && !appliedAssetId) {
      title.textContent = `${token.symbol} selected`;
      copy.textContent = 'Confirm the market to load the leaderboard, liquidity chart, and activity feed.';
    } else {
      title.textContent = 'Select an asset to open market overview';
      copy.textContent = 'The leaderboard, liquidity chart, and activity feed will load after the market is chosen.';
    }
  }

  function syncApplyButton() {
    const button = document.getElementById('supply-asset-apply-btn');
    if (!button) return;
    const token = stagedAssetId ? getSupplyToken(stagedAssetId) : null;
    const hasPending = !!(stagedAssetId && stagedAssetId !== appliedAssetId);
    button.disabled = !hasPending;
    button.classList.toggle('is-applied', !!(appliedAssetId && stagedAssetId === appliedAssetId));
    button.classList.toggle('is-pending', !!hasPending);
    if (hasPending && token) {
      button.innerHTML = `${applyIcon}<span>Apply ${token.symbol}</span>`;
    } else if (appliedAssetId && stagedAssetId === appliedAssetId && token) {
      button.innerHTML = `${applyIcon}<span>Applied</span>`;
    } else {
      button.innerHTML = `${applyIcon}<span>Select asset</span>`;
    }
    syncEmptyState();
  }

  function installApplyButton() {
    const assetContainer = document.getElementById('supply-asset-container');
    if (!assetContainer || document.getElementById('supply-asset-apply-btn')) return;
    const wrap = document.createElement('div');
    wrap.className = 'supply-draft-apply-container';
    wrap.innerHTML = `
      <label class="premium-supply-label">Confirm</label>
      <button type="button" class="supply-draft-apply-btn" id="supply-asset-apply-btn" disabled>${applyIcon}<span>Select asset</span></button>
    `;
    assetContainer.insertAdjacentElement('afterend', wrap);
    wrap.querySelector('button').addEventListener('click', () => applyStagedAsset());
    organizeSupplyControls();
  }

  function installAssetSearchClear() {
    const input = document.getElementById('asset-search-input');
    if (!input || input.dataset.supplyDraftClear === 'true') return;

    input.dataset.supplyDraftClear = 'true';
    const shell = input.parentElement;
    if (!shell) return;
    shell.classList.add('supply-asset-search-shell', 'no-clear');
    shell.querySelector('.supply-asset-search-clear')?.remove();

    const sync = () => shell.classList.toggle('has-value', input.value.trim().length > 0);
    input.addEventListener('input', sync);
    sync();
  }

  function setAssetPlaceholder() {
    if (document.body.classList.contains('supply-has-asset')) return;
    if (stagedAssetId) {
      syncApplyButton();
      return;
    }
    const hidden = document.getElementById('supply-asset-select');
    const text = document.getElementById('selected-asset-text');
    const icon = document.getElementById('selected-asset-icon');
    if (hidden) hidden.value = '';
    if (text && /fetching|loading/i.test(text.textContent || '')) text.textContent = 'Select asset';
    if (icon) icon.style.display = 'none';
    syncApplyButton();
  }

  function markStagedOption(tokens) {
    const container = document.getElementById('asset-options-container');
    if (!container) return;
    Array.from(container.children).forEach((child, index) => {
      const token = Array.isArray(tokens) ? tokens[index] : null;
      if (token && token.id) child.dataset.assetId = token.id;
      child.classList.toggle('active', !!(child.dataset.assetId && child.dataset.assetId === stagedAssetId));
    });
  }

  function patchOptionsRenderer() {
    if (optionsPatched || typeof window.renderSupplyAssetOptions !== 'function') return;
    optionsPatched = true;
    const originalRenderSupplyAssetOptions = window.renderSupplyAssetOptions;
    window.renderSupplyAssetOptions = function supplyDraftRenderSupplyAssetOptions(tokens) {
      const result = originalRenderSupplyAssetOptions.apply(this, arguments);
      markStagedOption(tokens);
      installAssetSearchClear();
      setTimeout(autoApplyDefaultSupplyAsset, 0);
      return result;
    };
  }

  function stageSupplyAsset(id) {
    const token = getSupplyToken(id);
    if (!token) return;
    stagedAssetId = id;
    const hidden = document.getElementById('supply-asset-select');
    if (hidden) hidden.value = id;
    setSelectorUi(token, id !== appliedAssetId);
    markStagedOption();
    syncApplyButton();
    const dropdown = document.getElementById('custom-asset-dropdown');
    if (dropdown) dropdown.style.display = 'none';
    if (chainDefaultAutoApplyArmed && stagedAssetId !== appliedAssetId) {
      setTimeout(autoApplyArmedDefaultSupplyAsset, 0);
    }
  }

  function applyStagedAsset(options = {}) {
    if (!stagedAssetId || stagedAssetId === appliedAssetId || !originalSelectAsset) return;
    chainDefaultAutoApplyArmed = false;
    appliedAssetId = stagedAssetId;
    document.body.classList.remove('supply-has-pending-asset');
    setAssetState(true);
    originalSelectAsset.call(window, stagedAssetId, options.auto ? { auto: true } : undefined);
    syncApplyButton();
  }

  function patchSelectionFunctions() {
    if (selectionPatched || typeof window.selectSupplyAsset !== 'function' || typeof window.selectSupplyChain !== 'function') return;
    selectionPatched = true;

    originalSelectAsset = originalSelectAsset || window.selectSupplyAsset;
    const originalSelectChain = window.selectSupplyChain;

    window.selectSupplyAsset = function supplyDraftSelectAsset(id, options = {}) {
      if (options.auto) {
        const token = getSupplyToken(id);
        if (!token || !originalSelectAsset) return false;
        stagedAssetId = id;
        appliedAssetId = id;
        document.body.classList.remove('supply-has-pending-asset');
        setAssetState(true);
        setSelectorUi(token, false);
        originalSelectAsset.call(window, id, { auto: true });
        syncApplyButton();
        return false;
      }
      stageSupplyAsset(id);
      return false;
    };

    window.selectSupplyChain = function supplyDraftSelectChain() {
      stagedAssetId = '';
      appliedAssetId = '';
      chainDefaultAutoApplyArmed = true;
      setAssetState(false);
      document.body.classList.remove('supply-has-pending-asset');
      const result = originalSelectChain.apply(this, arguments);
      syncSupplyChainOptions();
      syncApplyButton();
      setTimeout(setAssetPlaceholder, 0);
      setTimeout(autoApplyDefaultSupplyAsset, 0);
      setTimeout(syncSupplyChainOptions, 0);
      setTimeout(setAssetPlaceholder, 300);
      setTimeout(() => {
        if (stagedAssetId && stagedAssetId !== appliedAssetId) {
          applyStagedAsset({ auto: true });
        } else if (!autoApplyArmedDefaultSupplyAsset()) {
          autoApplyDefaultSupplyAsset();
        }
      }, 320);
      return result;
    };
  }

  function captureOriginalSelectAsset() {
    if (!originalSelectAsset && typeof window.selectSupplyAsset === 'function' && !selectionPatched) {
      originalSelectAsset = window.selectSupplyAsset;
    }
  }

  function boot() {
    document.body.classList.add('supply-draft-route');
    enhanceSupplyFrame();
    markResultCards();
    installEmptyState();
    installAssetSearchClear();
    installApplyButton();
    organizeSupplyControls();
    captureOriginalSelectAsset();
    patchOptionsRenderer();
    patchSelectionFunctions();
    patchSupplyHistoryRenderer();
    patchSupplyTableRenderer();
    patchSupplyActivityRenderer();
    patchSupplySort();
    polishExistingSupplyUi();
    syncSupplyChainOptions();
    syncAppliedAssetFromHidden();
    autoApplyDefaultSupplyAsset();
    setAssetState(!!document.getElementById('supply-asset-select')?.value || !!appliedAssetId);
    syncApplyButton();
    setTimeout(() => {
      captureOriginalSelectAsset();
      patchOptionsRenderer();
      patchSelectionFunctions();
      patchSupplyHistoryRenderer();
      patchSupplyTableRenderer();
      patchSupplyActivityRenderer();
      patchSupplySort();
      markResultCards();
      installApplyButton();
      organizeSupplyControls();
      syncSupplyChainOptions();
      polishExistingSupplyUi();
      syncAppliedAssetFromHidden();
      autoApplyDefaultSupplyAsset();
      setAssetPlaceholder();
    }, 250);
    setTimeout(() => {
      polishExistingSupplyUi();
      syncAppliedAssetFromHidden();
      autoApplyDefaultSupplyAsset();
      setAssetPlaceholder();
    }, 900);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
})();
