(function () {
  const navSvg = {
    assets: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2 2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>',
    tvl: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-6 4 4 5-8"/></svg>',
    earn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/></svg>',
    liquidation: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 10h18"/><path d="M12 13v4"/><path d="m9.5 15 2.5 2.5L14.5 15"/></svg>',
    supply: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
    revenue: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-4 3 3 5-7"/><path d="M16 7h3v3"/></svg>',
    history: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 3v6h6"/><path d="M12 7v5l3 2"/></svg>'
  };

  function forceEarnView() {
    try {
      if (typeof switchView === "function") switchView("earn");
    } catch (error) {
      console.warn("Earn draft switchView failed:", error);
    }
    document.querySelectorAll(".view-section").forEach(section => {
      section.classList.toggle("active", section.id === "view-earn");
    });
    document.querySelectorAll(".nav-tab").forEach(tab => tab.classList.remove("active"));
    const tab = document.getElementById("tab-earn");
    if (tab) tab.classList.add("active");
    document.title = "Dolomite · Earn";
  }

  function addPremiumHeader() {
    if (document.querySelector(".earn-premium-top")) return;
    const header = document.createElement("header");
    header.className = "earn-premium-top";
    header.innerHTML = `
      <div class="earn-premium-brand">
        <img src="TwojeKrypto%20dashboard.webp" alt="Dolomite Dashboard · Powered by TwojeKrypto">
      </div>
      <div class="earn-premium-nav-wrap">
        <nav class="earn-premium-nav" aria-label="Dolomite dashboard sections">
          <a class="earn-premium-nav-item" href="./assets/">${navSvg.assets}<span>Assets</span></a>
          <a class="earn-premium-nav-item" href="./tvl/">${navSvg.tvl}<span>TVL</span></a>
          <a class="earn-premium-nav-item" href="./dolo/"><img src="dolo-logo.svg" alt="" onerror="this.style.display='none'"><span>DOLO</span></a>
          <a class="earn-premium-nav-item" href="./odolo/"><img src="odolo-logo-official.svg" alt="" onerror="this.style.display='none'"><span>oDOLO</span></a>
          <a class="earn-premium-nav-item" href="./vedolo/"><img src="vedolo-logo.svg" alt="" onerror="this.style.display='none'"><span>veDOLO</span></a>
          <a class="earn-premium-nav-item active" href="./earn/" aria-current="page">${navSvg.earn}<span>Earn</span></a>
          <a class="earn-premium-nav-item" href="./borrow/">${navSvg.liquidation}<span>Borrow</span></a>
          <a class="earn-premium-nav-item" href="./supply/">${navSvg.supply}<span>Supply</span></a>
          <a class="earn-premium-nav-item" href="./revenue/">${navSvg.revenue}<span>Revenue</span></a>
          <a class="earn-premium-nav-item" href="./history/">${navSvg.history}<span>History</span></a>
        </nav>
      </div>`;
    const wrapper = document.querySelector(".page-wrapper");
    if (wrapper && wrapper.parentNode) {
      wrapper.parentNode.insertBefore(header, wrapper);
    } else {
      document.body.insertBefore(header, document.body.firstChild);
    }
  }

  function tuneHeroMarkup() {
    const hero = document.querySelector("#view-earn .earn-hero-inner");
    if (!hero || hero.dataset.earnDraftTuned === "true") return;
    hero.dataset.earnDraftTuned = "true";
    const label = hero.querySelector(":scope > div:nth-child(3)");
    const title = hero.querySelector(":scope > div:nth-child(4)");
    const subtitle = hero.querySelector(":scope > div:nth-child(5)");
    if (label) label.classList.add("earn-draft-kicker");
    if (title) title.classList.add("earn-draft-title");
    if (subtitle) subtitle.classList.add("earn-draft-subtitle");
    const input = hero.querySelector(".earn-input-card");
    if (input) {
      const live = document.createElement("div");
      live.className = "earn-draft-live";
      live.innerHTML = '<span class="earn-draft-live-dot"></span><span>On-chain wallet lookup</span>';
      hero.insertBefore(live, input);
    }
  }

  // NOTE: the protocol footer is owned exclusively by protocol-footer.js
  // (it removes any legacy .earn-dolo-proto and renders its own section);
  // the previous duplicate builder here was dead work + content drift.

  let earnTooltipCleanupObserver = null;
  const EARN_ACTION_TIP_RE = /^(copy address|copy ca|view on|open in|open transaction|clear(?: search| filter| chain filter| category filter| hf filter| collateral filter| debt filter)?|hide dust positions|show asset details|hide asset details|show position details|hide position details|sort by\b)/i;

  function isEarnActionTip(el, value) {
    const text = String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
    if (!text) return false;
    const isActionEl = el.matches("button,a,[role='button'],[onclick],.copy-addr-icon,.debank-icon,.search-clear,.earn-row-details-button,.earn-section-collapse-button,.dust-pill");
    return isActionEl || EARN_ACTION_TIP_RE.test(text);
  }

  function cleanEarnActionTooltips(root = document) {
    const view = document.getElementById("view-earn");
    if (!view) return;
    const scope = root.nodeType === 1 ? root : document;
    const scanRoot = scope === document ? view : scope;
    const nodes = scanRoot.matches && scanRoot.matches("[title], [data-tip]") ? [scanRoot] : [];
    scanRoot.querySelectorAll?.("[title], [data-tip]").forEach(node => nodes.push(node));
    nodes.forEach(el => {
      if (!view.contains(el)) return;
      const title = el.getAttribute("title");
      if (title && isEarnActionTip(el, title)) el.removeAttribute("title");
      const tip = el.getAttribute("data-tip");
      if (tip && isEarnActionTip(el, tip)) el.removeAttribute("data-tip");
    });
  }

  function installEarnTooltipCleanup() {
    cleanEarnActionTooltips();
    if (earnTooltipCleanupObserver) return;
    earnTooltipCleanupObserver = new MutationObserver(records => {
      records.forEach(record => {
        record.addedNodes.forEach(node => {
          if (node.nodeType === 1) cleanEarnActionTooltips(node);
        });
      });
    });
    earnTooltipCleanupObserver.observe(document.body, { childList: true, subtree: true });
  }

  function boot() {
    document.body.classList.add("earn-draft-route");
    addPremiumHeader();
    tuneHeroMarkup();
    installEarnTooltipCleanup();
    forceEarnView();
    setTimeout(forceEarnView, 250);
    setTimeout(tuneHeroMarkup, 250);
    setTimeout(cleanEarnActionTooltips, 250);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();
