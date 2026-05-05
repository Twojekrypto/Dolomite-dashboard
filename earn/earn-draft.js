(function () {
  const navSvg = {
    assets: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2 2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>',
    tvl: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-6 4 4 5-8"/></svg>',
    earn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/></svg>',
    liquidation: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 10h18"/><path d="M12 13v4"/><path d="m9.5 15 2.5 2.5L14.5 15"/></svg>',
    supply: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>'
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
        <img src="TwojeKrypto%20dashboard.png" alt="Dolomite Dashboard · Powered by TwojeKrypto">
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
      live.innerHTML = '<span class="earn-draft-live-dot"></span><span>Verified wallet lookup</span>';
      hero.insertBefore(live, input);
    }
  }

  function installDoloProtocolInfo() {
    const view = document.getElementById("view-earn");
    if (!view || view.querySelector(".earn-dolo-proto")) return;

    const legacyCard = view.querySelector(":scope > .protocol-info-card");
    const contracts = [
      { token: "DOLO", logo: "dolo-logo.svg", chain: "Ethereum", chainKey: "ethereum", addr: "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654", explorer: "https://etherscan.io" },
      { token: "DOLO", logo: "dolo-logo.svg", chain: "Berachain", chainKey: "berachain", addr: "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654", explorer: "https://berascan.com" },
      { token: "oDOLO", logo: "odolo-logo-official.svg", chain: "Berachain", chainKey: "berachain", addr: "0x02E513b5B54eE216Bf836ceb471507488fC89543", explorer: "https://berascan.com" },
      { token: "veDOLO", logo: "vedolo-logo.svg", chain: "Berachain", chainKey: "berachain", addr: "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4", explorer: "https://berascan.com" }
    ];

    const copyIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>';
    const checkIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>';
    const externalIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>';
    const xIcon = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>';
    const telegramIcon = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M11.944 0A12 12 0 000 12a12 12 0 0012 12 12 12 0 0012-12A12 12 0 0012 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 01.171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/></svg>';
    const discordIcon = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M20.317 4.37a19.791 19.791 0 00-4.885-1.515.074.074 0 00-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 00-5.487 0 12.64 12.64 0 00-.617-1.25.077.077 0 00-.079-.037 19.736 19.736 0 00-4.885 1.515.07.07 0 00-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 00.031.057 19.9 19.9 0 005.993 3.03.078.078 0 00.084-.028 14.09 14.09 0 001.226-1.994.076.076 0 00-.041-.106 13.107 13.107 0 01-1.872-.892.077.077 0 01-.008-.128 10.2 10.2 0 00.372-.292.074.074 0 01.077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 01.078.01c.12.098.246.198.373.292a.077.077 0 01-.006.127 12.299 12.299 0 01-1.873.892.077.077 0 00-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 00.084.028 19.839 19.839 0 006.002-3.03.077.077 0 00.032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 00-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.086 2.157 2.419 0 1.334-.955 2.419-2.157 2.419zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.086 2.157 2.419 0 1.334-.946 2.419-2.157 2.419z"/></svg>';
    const globeIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg>';
    const docsIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 014 4v14a3 3 0 00-3-3H2z"/><path d="M22 3h-6a4 4 0 00-4 4v14a3 3 0 013-3h7z"/></svg>';
    const githubIcon = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/></svg>';
    const auditIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>';
    const shortenAddress = addr => `${addr.slice(0, 6)}&hellip;${addr.slice(-4)}`;

    const contractRows = contracts.map((contract, index) => {
      const url = `${contract.explorer}/token/${contract.addr}`;
      return `
        <div class="earn-dolo-proto-addr" data-proto-idx="${index}">
          <div class="earn-dolo-proto-addr-left">
            <div class="earn-dolo-proto-token">
              <img src="${contract.logo}" alt="${contract.token}" onerror="this.style.display='none'">
              <span class="earn-dolo-proto-token-name">${contract.token}</span>
            </div>
            <span class="earn-dolo-proto-chain-chip"><span class="earn-dolo-proto-dot earn-dolo-proto-dot-${contract.chainKey}"></span>${contract.chain}</span>
          </div>
          <div class="earn-dolo-proto-addr-right">
            <span class="earn-dolo-proto-addr-hex addr-tooltip-wrap" data-full-addr="${contract.addr}">${shortenAddress(contract.addr)}</span>
            <button class="earn-dolo-proto-copy" type="button" data-copy="${contract.addr}" aria-label="Copy address">${copyIcon}</button>
            <button class="earn-dolo-proto-explore" type="button" data-url="${url}" aria-label="Open in explorer">${externalIcon}</button>
          </div>
        </div>`;
    }).join("");

    const section = document.createElement("section");
    section.className = "earn-dolo-proto";
    section.innerHTML = `
      <div class="earn-dolo-proto-head">
        <div class="earn-dolo-proto-brand">
          <div class="earn-dolo-proto-brand-mark">
            <img src="dolo-logo.svg" alt="DOLO" onerror="this.outerHTML='<span style=&quot;color:var(--earn-gold);font-weight:800;font-size:16px&quot;>◆</span>'">
          </div>
          <div class="earn-dolo-proto-brand-text">
            <div class="earn-dolo-proto-brand-name">Dolomite</div>
            <div class="earn-dolo-proto-brand-sub">Lending &amp; margin &middot; <b>7 chains</b> &middot; <b>104 markets</b></div>
          </div>
        </div>
        <div class="earn-dolo-proto-links">
          <div class="earn-dolo-proto-links-group">
            <span class="earn-dolo-proto-links-label">Community</span>
            <a class="earn-dolo-proto-pill" href="https://twitter.com/Dolomite_io" target="_blank" rel="noopener">${xIcon}Twitter</a>
            <a class="earn-dolo-proto-pill" href="https://t.me/dolomite_official" target="_blank" rel="noopener">${telegramIcon}Telegram</a>
            <a class="earn-dolo-proto-pill" href="https://discord.com/invite/uDRzrB2YgP" target="_blank" rel="noopener">${discordIcon}Discord</a>
          </div>
          <span class="earn-dolo-proto-links-sep"></span>
          <div class="earn-dolo-proto-links-group">
            <span class="earn-dolo-proto-links-label">Resources</span>
            <a class="earn-dolo-proto-pill" href="https://dolomite.io" target="_blank" rel="noopener">${globeIcon}Website</a>
            <a class="earn-dolo-proto-pill" href="https://docs.dolomite.io/" target="_blank" rel="noopener">${docsIcon}Docs</a>
            <a class="earn-dolo-proto-pill" href="https://github.com/dolomite-exchange" target="_blank" rel="noopener">${githubIcon}GitHub</a>
            <a class="earn-dolo-proto-pill" href="https://docs.dolomite.io/security/audits" target="_blank" rel="noopener">${auditIcon}Audits</a>
          </div>
        </div>
      </div>
      <div class="earn-dolo-proto-body">
        <div class="earn-dolo-proto-body-head">Contract Addresses</div>
        <div class="earn-dolo-proto-addrs">${contractRows}</div>
      </div>`;

    if (legacyCard) legacyCard.replaceWith(section);
    else view.appendChild(section);

    section.querySelectorAll(".earn-dolo-proto-addr").forEach(row => {
      const contract = contracts[Number(row.dataset.protoIdx)];
      row.addEventListener("click", event => {
        if (event.target.closest(".earn-dolo-proto-copy") || event.target.closest(".earn-dolo-proto-explore")) return;
        window.open(`${contract.explorer}/token/${contract.addr}`, "_blank", "noopener");
      });
    });
    section.querySelectorAll(".earn-dolo-proto-explore").forEach(button => {
      button.addEventListener("click", event => {
        event.stopPropagation();
        const url = button.dataset.url;
        if (url) window.open(url, "_blank", "noopener");
      });
    });
    section.querySelectorAll(".earn-dolo-proto-copy").forEach(button => {
      button.addEventListener("click", async event => {
        event.stopPropagation();
        try { await navigator.clipboard.writeText(button.dataset.copy); } catch (error) {}
        const original = button.innerHTML;
        button.classList.add("copied");
        button.innerHTML = checkIcon;
        setTimeout(() => {
          button.classList.remove("copied");
          button.innerHTML = original;
        }, 1400);
      });
    });
  }

  function boot() {
    document.body.classList.add("earn-draft-route");
    addPremiumHeader();
    tuneHeroMarkup();
    installDoloProtocolInfo();
    forceEarnView();
    setTimeout(forceEarnView, 250);
    setTimeout(tuneHeroMarkup, 250);
    setTimeout(installDoloProtocolInfo, 250);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();
