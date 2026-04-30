(function () {
  const navSvg = {
    assets: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2 2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>',
    tvl: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-6 4 4 5-8"/></svg>',
    earn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/></svg>'
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

  function boot() {
    document.body.classList.add("earn-draft-route");
    addPremiumHeader();
    tuneHeroMarkup();
    forceEarnView();
    setTimeout(forceEarnView, 250);
    setTimeout(tuneHeroMarkup, 250);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();
