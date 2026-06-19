(function () {
  const VERSION = "protocol-footer-20260619-mobile";
  const FOOTER_SELECTOR = ".dolo-protocol-footer";
  const LEGACY_SELECTOR = ".proto, .earn-dolo-proto, .protocol-info-card, footer.foot, div.foot";
  const SUBTITLE = 'Lending &amp; margin · <b>66 live markets</b> · <b>7 chains</b>';
  const COPY_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
  const CHECK_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>';
  const EXT_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>';
  const X_ICON = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>';
  const TELEGRAM_ICON = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/></svg>';
  const DISCORD_ICON = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037 19.736 19.736 0 0 0-4.885 1.515.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028 14.09 14.09 0 0 0 1.226-1.994.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.086 2.157 2.419 0 1.334-.955 2.419-2.157 2.419zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.086 2.157 2.419 0 1.334-.946 2.419-2.157 2.419z"/></svg>';
  const GLOBE_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>';
  const DOCS_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>';
  const GITHUB_ICON = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0 0 24 12c0-6.63-5.37-12-12-12z"/></svg>';
  const AUDIT_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>';
  const CONTRACTS = [
    { token: "DOLO", logo: "dolo-logo.svg", chain: "Ethereum", chainKey: "ethereum", addr: "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654", explorer: "https://etherscan.io" },
    { token: "DOLO", logo: "dolo-logo.svg", chain: "Berachain", chainKey: "berachain", addr: "0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654", explorer: "https://berascan.com" },
    { token: "oDOLO", logo: "odolo-logo-official.svg", chain: "Berachain", chainKey: "berachain", addr: "0x02E513b5B54eE216Bf836ceb471507488fC89543", explorer: "https://berascan.com" },
    { token: "veDOLO", logo: "vedolo-logo.svg", chain: "Berachain", chainKey: "berachain", addr: "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4", explorer: "https://berascan.com" }
  ];

  let rendering = false;
  let scheduled = false;

  function esc(value) {
    return String(value ?? "").replace(/[&<>"']/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  }

  function shorten(addr) {
    return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
  }

  function contractRows() {
    return CONTRACTS.map((contract, index) => `
      <div class="dolo-protocol-footer-addr" data-protocol-contract="${index}" role="link" tabindex="0" aria-label="Open ${esc(contract.token)} contract on ${esc(contract.chain)} explorer">
        <div class="dolo-protocol-footer-addr-left">
          <div class="dolo-protocol-footer-token">
            <img src="${esc(contract.logo)}" alt="${esc(contract.token)}" onerror="this.style.display='none'">
            <span class="dolo-protocol-footer-token-name">${esc(contract.token)}</span>
          </div>
          <span class="dolo-protocol-footer-chain"><span class="dolo-protocol-footer-chain-dot ${esc(contract.chainKey)}"></span>${esc(contract.chain)}</span>
        </div>
        <div class="dolo-protocol-footer-addr-right">
          <span class="dolo-protocol-footer-hex addr-tooltip-wrap" data-full-addr="${esc(contract.addr)}">${esc(shorten(contract.addr))}</span>
          <button class="dolo-protocol-footer-copy" type="button" data-copy="${esc(contract.addr)}" aria-label="Copy address">${COPY_ICON}</button>
          <button class="dolo-protocol-footer-explore" type="button" data-url="${esc(contract.explorer)}/token/${esc(contract.addr)}" aria-label="Open in explorer">${EXT_ICON}</button>
        </div>
      </div>
    `).join("");
  }

  function buildFooter() {
    const section = document.createElement("section");
    section.className = "dolo-protocol-footer";
    section.dataset.protocolFooterVersion = VERSION;
    section.setAttribute("aria-label", "Dolomite protocol information");
    section.innerHTML = `
      <div class="dolo-protocol-footer-head">
        <div class="dolo-protocol-footer-brand">
          <div class="dolo-protocol-footer-mark">
            <img src="dolo-logo.svg" alt="DOLO" onerror="this.outerHTML='<span style=&quot;color:var(--gold,#c9a227);font-weight:800;font-size:16px&quot;>◆</span>'">
          </div>
          <div class="dolo-protocol-footer-brand-text">
            <div class="dolo-protocol-footer-name">Dolomite</div>
            <div class="dolo-protocol-footer-sub">${SUBTITLE}</div>
          </div>
        </div>
        <div class="dolo-protocol-footer-links">
          <div class="dolo-protocol-footer-links-group">
            <span class="dolo-protocol-footer-links-label">Community</span>
            <a class="dolo-protocol-footer-pill" href="https://twitter.com/Dolomite_io" target="_blank" rel="noopener">${X_ICON}Twitter</a>
            <a class="dolo-protocol-footer-pill" href="https://t.me/dolomite_official" target="_blank" rel="noopener">${TELEGRAM_ICON}Telegram</a>
            <a class="dolo-protocol-footer-pill" href="https://discord.com/invite/uDRzrB2YgP" target="_blank" rel="noopener">${DISCORD_ICON}Discord</a>
          </div>
          <span class="dolo-protocol-footer-links-sep"></span>
          <div class="dolo-protocol-footer-links-group">
            <span class="dolo-protocol-footer-links-label">Resources</span>
            <a class="dolo-protocol-footer-pill" href="https://dolomite.io" target="_blank" rel="noopener">${GLOBE_ICON}Website</a>
            <a class="dolo-protocol-footer-pill" href="https://docs.dolomite.io/" target="_blank" rel="noopener">${DOCS_ICON}Docs</a>
            <a class="dolo-protocol-footer-pill" href="https://github.com/dolomite-exchange" target="_blank" rel="noopener">${GITHUB_ICON}GitHub</a>
            <a class="dolo-protocol-footer-pill" href="https://docs.dolomite.io/security/audits" target="_blank" rel="noopener">${AUDIT_ICON}Audits</a>
          </div>
        </div>
      </div>
      <div class="dolo-protocol-footer-body">
        <div class="dolo-protocol-footer-body-head">Contract Addresses</div>
        <div class="dolo-protocol-footer-addrs">${contractRows()}</div>
      </div>
    `;
    return section;
  }

  function wireFooter(section) {
    section.querySelectorAll(".dolo-protocol-footer-addr").forEach(row => {
      const contract = CONTRACTS[Number(row.dataset.protocolContract)];
      const open = () => {
        if (contract) window.open(`${contract.explorer}/token/${contract.addr}`, "_blank", "noopener");
      };
      row.addEventListener("click", event => {
        if (event.target.closest(".dolo-protocol-footer-copy") || event.target.closest(".dolo-protocol-footer-explore")) return;
        open();
      });
      row.addEventListener("keydown", event => {
        if (event.key !== "Enter" && event.key !== " ") return;
        if (event.target.closest(".dolo-protocol-footer-copy") || event.target.closest(".dolo-protocol-footer-explore")) return;
        event.preventDefault();
        open();
      });
    });
    section.querySelectorAll(".dolo-protocol-footer-explore").forEach(button => {
      button.addEventListener("click", event => {
        event.stopPropagation();
        const url = button.dataset.url;
        if (url) window.open(url, "_blank", "noopener");
      });
    });
    section.querySelectorAll(".dolo-protocol-footer-copy").forEach(button => {
      button.addEventListener("click", async event => {
        event.stopPropagation();
        try {
          await navigator.clipboard.writeText(button.dataset.copy || "");
        } catch (error) {}
        button.classList.add("copied");
        button.innerHTML = CHECK_ICON;
        setTimeout(() => {
          button.classList.remove("copied");
          button.innerHTML = COPY_ICON;
        }, 1400);
      });
    });
  }

  function legacyTargets() {
    return Array.from(document.querySelectorAll(LEGACY_SELECTOR))
      .filter(node => !node.classList.contains("dolo-protocol-footer"))
      .filter(node => !node.closest(FOOTER_SELECTOR));
  }

  function nodeHasLayout(node) {
    if (!node || !node.isConnected) return false;
    const rect = node.getBoundingClientRect();
    return rect.width > 0 && rect.height >= 0;
  }

  function fallbackParent() {
    return document.querySelector("#view-earn.active")
      || document.querySelector(".view-section.active")
      || document.querySelector(".page-wrapper")
      || document.querySelector(".wrap")
      || document.querySelector(".wrap main")
      || document.querySelector("main")
      || document.body;
  }

  function renderFooter() {
    if (rendering || !document.body) return;
    rendering = true;
    try {
      const existing = document.querySelector(FOOTER_SELECTOR);
      const parent = fallbackParent();
      let footer = existing;
      if (!footer) {
        footer = buildFooter();
        parent.appendChild(footer);
        wireFooter(footer);
      } else if (footer.parentNode !== parent || footer.nextElementSibling || !nodeHasLayout(footer)) {
        parent.appendChild(footer);
      }
      legacyTargets().forEach(node => node.remove());
    } finally {
      rendering = false;
    }
  }

  function scheduleRender() {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(() => {
      scheduled = false;
      renderFooter();
    });
  }

  function boot() {
    renderFooter();
    setTimeout(scheduleRender, 300);
    setTimeout(scheduleRender, 900);
    setTimeout(scheduleRender, 2200);
    window.addEventListener("load", scheduleRender, { once: true });
    const observer = new MutationObserver(records => {
      for (const record of records) {
        for (const node of record.addedNodes) {
          if (node.nodeType !== 1) continue;
          if (node.matches?.(LEGACY_SELECTOR) || node.querySelector?.(LEGACY_SELECTOR)) {
            scheduleRender();
            return;
          }
        }
      }
    });
    observer.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();
