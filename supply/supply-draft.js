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
  const defaultSupplyAssetSymbol = String(window.__DOLO_SUPPLY_DEFAULT_ASSET || 'USD1').toUpperCase();
  const activityTypeOptions = [
    { type: 'deposit', label: 'Deposits' },
    { type: 'withdraw', label: 'Withdrawals' },
    { type: 'transfer', label: 'Transfers' },
    { type: 'liquidation', label: 'Liquidations' },
  ];

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
    document.getElementById('supply-activity-card')?.classList.add('supply-draft-activity-card');
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
    if (titleLine && !document.getElementById('supply-history-change-badge')) {
      const badge = document.createElement('span');
      badge.id = 'supply-history-change-badge';
      badge.className = 'supply-history-change-badge';
      titleLine.appendChild(badge);
    }
  }

  function syncSupplyHistoryBadge() {
    const badge = document.getElementById('supply-history-change-badge');
    if (!badge) return;
    let label = 'All Time';
    try {
      const points = currentSupplyOverview?.historyPoints || [];
      if (supplyHistoryRange === 'custom' && Array.isArray(points) && points.length > 1 && typeof getSupplyHistoryWindow === 'function' && typeof supplyFormatShortRange === 'function') {
        const range = getSupplyHistoryWindow(points);
        label = supplyFormatShortRange(range.startTs, range.endTs);
      }
    } catch (error) {}
    badge.textContent = label;
    badge.className = 'supply-history-change-badge visible';
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

  function installSupplyHistoryCompactHover(points) {
    const chart = document.getElementById('supply-history-chart');
    const tooltip = document.getElementById('supply-history-tooltip');
    const svg = chart?.querySelector('svg');
    const hoverLine = document.getElementById('supply-history-hover-line');
    const hoverDot = document.getElementById('supply-history-hover-dot');
    if (!chart || !tooltip || !svg || !hoverLine || !hoverDot) return;

    const historyMetric = currentSupplyOverview?.historyMetric || 'token';
    let visiblePoints = [];
    try {
      visiblePoints = getVisibleSupplyHistoryPoints(points || []).map(point => ({
        ...point,
        value: Math.max(0, Number(point?.value || 0)),
        usdValue: Number.isFinite(Number(point?.usdValue)) ? Math.max(0, Number(point.usdValue)) : point?.usdValue,
        tokenValue: Number.isFinite(Number(point?.tokenValue)) ? Math.max(0, Number(point.tokenValue)) : point?.tokenValue,
      }));
    } catch (error) {
      visiblePoints = [];
    }
    if (visiblePoints.length < 2) return;

    tooltip.classList.add('supply-history-tooltip-compact');
    tooltip.innerHTML = `
      <div class="supply-history-tooltip-date"></div>
      <div class="supply-history-tooltip-primary"></div>
    `;
    const tipDate = tooltip.querySelector('.supply-history-tooltip-date');
    const tipPrimary = tooltip.querySelector('.supply-history-tooltip-primary');

    const width = 1000;
    const height = 320;
    const padRight = 20;
    const padTop = 20;
    const padBottom = 20;
    const values = visiblePoints.map(point => Number(point.value || 0));
    const rawMin = Math.max(0, Math.min(...values));
    const rawMax = Math.max(0, Math.max(...values));
    const rawSpread = rawMax - rawMin;
    const vertPad = rawSpread > 0 ? rawSpread * 0.08 : Math.max(rawMax * 0.02, 1);
    const min = Math.max(0, rawMin - vertPad);
    const max = Math.max(rawMax + vertPad, min + Math.max(rawMax * 0.02, 1));
    const safeSpread = max - min;
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

    const pathPoints = visiblePoints.map((point, index) => {
      const x = padLeft + (innerW * index) / Math.max(1, visiblePoints.length - 1);
      const normalized = (Number(point.value || 0) - min) / safeSpread;
      const y = height - padBottom - normalized * innerH;
      return { x, y };
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
      const screenX = (svgRect.left - chartRect.left) + (point.x / width) * svgRect.width;
      const screenY = (svgRect.top - chartRect.top) + (point.y / height) * svgRect.height;
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
      const rx = ((event.clientX - svgRect.left) / svgRect.width) * width;
      if (!Number.isFinite(rx) || rx < padLeft || rx > plotRight) {
        hideHover();
        return;
      }
      const index = Math.max(0, Math.min(visiblePoints.length - 1, Math.round(((rx - padLeft) / innerW) * (visiblePoints.length - 1))));
      showHover(index);
    };
    chart.onpointerleave = hideHover;
  }

  function syncSupplyHistoryHeaderCopy() {
    const meta = document.getElementById('supply-history-meta');
    const delta = document.getElementById('supply-history-delta');
    if (meta) meta.textContent = '';
    if (delta) {
      delta.innerHTML = '<span class="supply-history-pulse"></span><span>drag the window below to zoom</span>';
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
      if (addressWrap && !walletCell.querySelector('.supply-wallet')) {
        addressWrap.classList.add('supply-wallet-bottom');
        addressWrap.removeAttribute('style');
        const wallet = document.createElement('div');
        wallet.className = 'supply-wallet';
        const top = document.createElement('div');
        top.className = 'supply-wallet-top';
        top.innerHTML = '<span class="addr-name addr-generic">Wallet</span>';
        wallet.appendChild(top);
        wallet.appendChild(addressWrap);
        walletCell.textContent = '';
        walletCell.appendChild(wallet);
      }

      walletCell.querySelectorAll('.addr-tooltip-wrap').forEach(link => {
        link.classList.add('addr-mono');
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
        link.classList.add('addr-mono');
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
        event.stopPropagation();
        const isOpen = dropdown.classList.toggle('open');
        trigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
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

    if (toolbar.dataset.supplyDraftClickClose !== 'true') {
      toolbar.dataset.supplyDraftClickClose = 'true';
      document.addEventListener('click', event => {
        const current = document.getElementById('supply-activity-type-filter');
        if (current && !current.contains(event.target)) {
          current.classList.remove('open');
          current.querySelector('.supply-activity-type-trigger')?.setAttribute('aria-expanded', 'false');
        }
      });
    }

    syncActivityTypeDropdown();
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

  function renderSupplyDraftActivityPagination(totalLen) {
    const el = document.getElementById('supply-activity-pagination');
    if (!el) return;
    let page = 1;
    let perPage = 12;
    try {
      page = supplyActivityPage || 1;
      perPage = SUPPLY_ACTIVITY_PAGE_SIZE || 12;
    } catch (error) {}
    const totalPages = Math.max(1, Math.ceil((Number(totalLen) || 0) / perPage));
    page = Math.max(1, Math.min(page, totalPages));
    const start = totalLen > 0 ? ((page - 1) * perPage + 1) : 0;
    const end = totalLen > 0 ? Math.min(totalLen, page * perPage) : 0;
    const safeTotal = (Number(totalLen) || 0).toLocaleString('en-US');
    const range = totalLen > 0 ? `${start.toLocaleString('en-US')}-${end.toLocaleString('en-US')} of ${safeTotal}` : 'No matches';
    el.innerHTML = `
      <span class="supply-page-range">${range}</span>
      <span class="supply-pager-controls">
        <button class="flow-pager-btn" onclick="supplyGoActivityPage(1)" ${page === 1 ? 'disabled' : ''}>«</button>
        <button class="flow-pager-btn" onclick="supplyGoActivityPage(${page - 1})" ${page === 1 ? 'disabled' : ''}>‹</button>
        <span class="flow-pager-info">${page} / ${totalPages}</span>
        <button class="flow-pager-btn" onclick="supplyGoActivityPage(${page + 1})" ${page === totalPages ? 'disabled' : ''}>›</button>
        <button class="flow-pager-btn" onclick="supplyGoActivityPage(${totalPages})" ${page === totalPages ? 'disabled' : ''}>»</button>
      </span>
      <span class="flow-pager-total">${safeTotal} events</span>
    `;
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
    let perPage = 12;
    try {
      page = supplyPage || 1;
      perPage = SUPPLY_PER_PAGE || 12;
    } catch (error) {}
    const totalPages = Math.max(1, Math.ceil((Number(totalLen) || 0) / perPage));
    page = Math.max(1, Math.min(page, totalPages));
    const start = totalLen > 0 ? ((page - 1) * perPage + 1) : 0;
    const end = totalLen > 0 ? Math.min(totalLen, page * perPage) : 0;
    const safeTotal = (Number(totalLen) || 0).toLocaleString('en-US');
    const range = totalLen > 0 ? `${start.toLocaleString('en-US')}-${end.toLocaleString('en-US')} of ${safeTotal}` : 'No matches';
    el.innerHTML = `
      <span class="supply-page-range">${range}</span>
      <span class="supply-pager-controls">
        <button class="flow-pager-btn" onclick="supply_goPage(1)" ${page === 1 ? 'disabled' : ''}>«</button>
        <button class="flow-pager-btn" onclick="supply_goPage(${page - 1})" ${page === 1 ? 'disabled' : ''}>‹</button>
        <span class="flow-pager-info">${page} / ${totalPages}</span>
        <button class="flow-pager-btn" onclick="supply_goPage(${page + 1})" ${page === totalPages ? 'disabled' : ''}>›</button>
        <button class="flow-pager-btn" onclick="supply_goPage(${totalPages})" ${page === totalPages ? 'disabled' : ''}>»</button>
      </span>
      <span class="flow-pager-total">${safeTotal} suppliers</span>
    `;
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
      const result = originalRenderSupplyActivityTable.apply(this, arguments);
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

  function getDefaultSupplyToken() {
    const matchesDefault = token => String(token?.symbol || '').toUpperCase() === defaultSupplyAssetSymbol;
    try {
      if (typeof currentSupplyTokensList !== 'undefined' && Array.isArray(currentSupplyTokensList)) {
        const token = currentSupplyTokensList.find(matchesDefault);
        if (token) return token;
      }
    } catch (error) {}
    try {
      if (typeof currentSupplyTokensMap !== 'undefined' && currentSupplyTokensMap) {
        return Object.values(currentSupplyTokensMap).find(matchesDefault) || null;
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
    originalSelectAsset.call(window, token.id);
    syncApplyButton();
  }

  function getIconPath(token) {
    try {
      if (token && typeof getTokenIcon === 'function' && typeof truncateTokenName === 'function') {
        return getTokenIcon(truncateTokenName(token.symbol));
      }
    } catch (error) {}
    return '';
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
  }

  function applyStagedAsset() {
    if (!stagedAssetId || stagedAssetId === appliedAssetId || !originalSelectAsset) return;
    appliedAssetId = stagedAssetId;
    document.body.classList.remove('supply-has-pending-asset');
    setAssetState(true);
    originalSelectAsset.call(window, stagedAssetId);
    syncApplyButton();
  }

  function patchSelectionFunctions() {
    if (selectionPatched || typeof window.selectSupplyAsset !== 'function' || typeof window.selectSupplyChain !== 'function') return;
    selectionPatched = true;

    originalSelectAsset = originalSelectAsset || window.selectSupplyAsset;
    const originalSelectChain = window.selectSupplyChain;

    window.selectSupplyAsset = function supplyDraftSelectAsset(id) {
      stageSupplyAsset(id);
      return false;
    };

    window.selectSupplyChain = function supplyDraftSelectChain() {
      stagedAssetId = '';
      appliedAssetId = '';
      setAssetState(false);
      document.body.classList.remove('supply-has-pending-asset');
      const result = originalSelectChain.apply(this, arguments);
      syncSupplyChainOptions();
      syncApplyButton();
      setTimeout(setAssetPlaceholder, 0);
      setTimeout(autoApplyDefaultSupplyAsset, 0);
      setTimeout(syncSupplyChainOptions, 0);
      setTimeout(setAssetPlaceholder, 300);
      setTimeout(autoApplyDefaultSupplyAsset, 320);
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
