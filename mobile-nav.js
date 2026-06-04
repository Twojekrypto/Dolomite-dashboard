(function () {
    const ICONS = {
        assets: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2 2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>',
        tvl: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-6 4 4 5-8"/></svg>',
        dolo: '<img src="dolo-logo.svg" alt="" onerror="this.style.display=\'none\'">',
        odolo: '<img src="odolo-logo-official.svg" alt="" onerror="this.style.display=\'none\'">',
        vedolo: '<img src="vedolo-logo.svg" alt="" onerror="this.style.display=\'none\'">',
        earn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
        borrow: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 10h18"/><path d="M12 13v4"/><path d="m9.5 15 2.5 2.5L14.5 15"/></svg>',
        supply: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
        revenue: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-4 3 3 5-7"/><path d="M16 7h3v3"/></svg>',
        history: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 3v6h6"/><path d="M12 7v5l3 2"/></svg>'
    };

    const NAV_ITEMS = [
        { tab: "assets", label: "Assets", href: "./assets/" },
        { tab: "tvl", label: "TVL", href: "./tvl/" },
        { tab: "dolo", label: "DOLO", href: "./dolo/" },
        { tab: "odolo", label: "oDOLO", href: "./odolo/" },
        { tab: "vedolo", label: "veDOLO", href: "./vedolo/" },
        { tab: "earn", label: "Earn", href: "./earn/" },
        { tab: "borrow", label: "Borrow", href: "./borrow/" },
        { tab: "supply", label: "Supply", href: "./supply/" },
        { tab: "revenue", label: "Revenue", href: "./revenue/" },
        { tab: "history", label: "History", href: "./history/" }
    ];

    const TAB_ALIASES = {
        liquidation: "borrow",
        liquidations: "borrow",
        lending: "borrow"
    };

    const LABELS = {
        assets: "Assets",
        tvl: "TVL",
        dolo: "DOLO",
        odolo: "oDOLO",
        vedolo: "veDOLO",
        earn: "Earn",
        borrow: "Borrow",
        supply: "Supply",
        history: "History",
        revenue: "Revenue",
        liquidation: "Borrow",
        liquidations: "Borrow",
        lending: "Borrow"
    };

    function normalizeTab(value) {
        const tab = String(value || "").trim().toLowerCase();
        return TAB_ALIASES[tab] || tab;
    }

    function cleanLabel(value) {
        const text = String(value || "").replace(/\s+/g, " ").trim();
        if (!text) return "Menu";
        const lower = text.toLowerCase();
        return LABELS[lower] || text;
    }

    function tabFromHref(href) {
        if (!href) return "";
        try {
            const url = new URL(href, window.location.href);
            const first = url.pathname.split("/").filter(Boolean).pop() || "";
            return first.toLowerCase();
        } catch (error) {
            return String(href).replace(/[./#]/g, "").toLowerCase();
        }
    }

    function tabFromItem(item) {
        if (!item) return "";
        return normalizeTab((item.dataset && item.dataset.tab) || tabFromHref(item.getAttribute("href")) || item.id?.replace(/^tab-/, "") || "");
    }

    function isVisibleItem(item) {
        if (!item) return false;
        if (item.getAttribute("aria-hidden") === "true") return false;
        if (item.hasAttribute("hidden")) return false;
        if (item.style && item.style.display === "none") return false;
        return true;
    }

    function iconHtml(item) {
        const icon = item?.querySelector("svg,img");
        if (!icon) return "";
        const clone = icon.cloneNode(true);
        clone.removeAttribute("id");
        clone.removeAttribute("class");
        return clone.outerHTML;
    }

    function canonicalIcon(tab) {
        return ICONS[normalizeTab(tab)] || "";
    }

    function itemLabel(item) {
        const tab = tabFromItem(item);
        if (LABELS[tab]) return LABELS[tab];
        const span = item?.querySelector("span");
        return cleanLabel(span ? span.textContent : item?.textContent);
    }

    function activeTabByLocation() {
        const path = window.location.pathname.toLowerCase();
        const route = normalizeTab(window.__DOLO_ROUTE || "");
        if (route && LABELS[route]) return route;
        const match = NAV_ITEMS.find(item => path.includes("/" + item.tab + "/"));
        if (match) return match.tab;
        if (/\/dolomite-dashboard\/?$|\/dolo\/?$|\/$/.test(path)) return "dolo";
        return "";
    }

    function activeByLocation(items) {
        const activeTab = activeTabByLocation();
        return items.find(item => tabFromItem(item) === activeTab);
    }

    function activeTabFromItems(items) {
        const locationTab = activeTabByLocation();
        if (locationTab) return locationTab;
        const active = items.find(item => item.classList.contains("active") || item.getAttribute("aria-current") === "page");
        return tabFromItem(active || items[0]);
    }

    function itemClassFromSelector(selector) {
        return selector.split(",")[0].trim().replace(/^\./, "") || "nav-item";
    }

    function setNativeNavActive(nav, activeTab) {
        nav.querySelectorAll("[data-tab], .nav-item, .nav-tab, .earn-premium-nav-item").forEach(item => {
            const tab = tabFromItem(item);
            const active = tab === activeTab;
            item.classList.toggle("active", active);
            if (active) {
                item.setAttribute("aria-current", "page");
            } else {
                item.removeAttribute("aria-current");
            }
        });
    }

    function normalizeNativeNav(candidate, activeTab) {
        if (!candidate || !candidate.nav) return;
        const itemClass = itemClassFromSelector(candidate.itemSelector);
        if (candidate.nav.dataset.mobileNavCanonical !== "true") {
            candidate.nav.innerHTML = NAV_ITEMS.map(item => {
                const active = item.tab === activeTab;
                const idAttr = itemClass === "nav-tab" ? ` id="tab-${item.tab}"` : "";
                return `<a class="${itemClass}${active ? " active" : ""}"${idAttr} href="${item.href}" data-tab="${item.tab}"${active ? ' aria-current="page"' : ""}>${canonicalIcon(item.tab)}<span>${item.label}</span></a>`;
            }).join("");
            candidate.nav.dataset.mobileNavCanonical = "true";
        }
        setNativeNavActive(candidate.nav, activeTab);
    }

    function getNavCandidate() {
        const candidates = [
            { wrap: document.querySelector(".earn-premium-nav-wrap"), nav: document.querySelector(".earn-premium-nav"), itemSelector: ".earn-premium-nav-item" },
            { wrap: document.querySelector(".site-nav-wrap"), nav: document.querySelector(".site-nav"), itemSelector: ".nav-item" },
            { wrap: document.querySelector(".header-nav-bar"), nav: document.querySelector(".nav-tabs"), itemSelector: ".nav-tab" }
        ];
        return candidates.find(candidate => candidate.wrap && candidate.nav);
    }

    function syncNav(wrap) {
        if (!wrap || !wrap.mobileNavState) return;
        const state = wrap.mobileNavState;
        const items = Array.from(state.nav.querySelectorAll(state.itemSelector)).filter(isVisibleItem);
        if (!items.length) return;
        const activeTab = activeTabFromItems(items);
        const activeItem = NAV_ITEMS.find(item => item.tab === activeTab) || NAV_ITEMS[0];
        setNativeNavActive(state.nav, activeTab);
        state.icon.innerHTML = canonicalIcon(activeItem.tab) || iconHtml(items.find(item => tabFromItem(item) === activeTab));
        state.label.textContent = activeItem.label || itemLabel(items.find(item => tabFromItem(item) === activeTab));
        state.panel.querySelectorAll(".mobile-nav-link").forEach(link => {
            link.classList.toggle("active", link.dataset.tab === activeTab);
            if (link.dataset.tab === activeTab) {
                link.setAttribute("aria-current", "page");
            } else {
                link.removeAttribute("aria-current");
            }
        });
    }

    function closeNav(wrap) {
        if (!wrap || !wrap.mobileNavState) return;
        wrap.classList.remove("mobile-nav-open");
        wrap.mobileNavState.trigger.setAttribute("aria-expanded", "false");
    }

    function enhanceNav() {
        const candidate = getNavCandidate();
        if (!candidate || candidate.wrap.dataset.mobileNavReady === "true") return false;
        const items = Array.from(candidate.nav.querySelectorAll(candidate.itemSelector)).filter(isVisibleItem);
        if (items.length < 2) return false;
        const activeTab = activeTabFromItems(items);
        normalizeNativeNav(candidate, activeTab);

        const panelId = "mobile-dashboard-nav-" + Math.random().toString(36).slice(2, 8);
        const trigger = document.createElement("button");
        trigger.type = "button";
        trigger.className = "mobile-nav-trigger";
        trigger.setAttribute("aria-expanded", "false");
        trigger.setAttribute("aria-controls", panelId);
        trigger.setAttribute("aria-label", "Open dashboard navigation");
        trigger.innerHTML = '<span class="mobile-nav-current"><span class="mobile-nav-current-icon"></span><span class="mobile-nav-current-label">Menu</span></span><span class="mobile-nav-bars" aria-hidden="true"><span></span><span></span><span></span></span>';

        const panel = document.createElement("div");
        panel.className = "mobile-nav-panel";
        panel.id = panelId;

        NAV_ITEMS.forEach(item => {
            const link = document.createElement("a");
            link.className = "mobile-nav-link";
            link.href = item.href;
            link.dataset.tab = item.tab;
            link.innerHTML = '<span class="mobile-nav-link-icon">' + canonicalIcon(item.tab) + '</span><span class="mobile-nav-link-label">' + item.label + '</span>';
            link.addEventListener("click", () => closeNav(candidate.wrap));
            panel.appendChild(link);
        });

        if (!panel.children.length) return false;
        candidate.wrap.insertBefore(trigger, candidate.nav);
        candidate.wrap.insertBefore(panel, candidate.nav.nextSibling);
        candidate.wrap.classList.add("mobile-nav-ready");
        candidate.wrap.dataset.mobileNavReady = "true";
        candidate.wrap.mobileNavState = {
            nav: candidate.nav,
            itemSelector: candidate.itemSelector,
            trigger,
            panel,
            icon: trigger.querySelector(".mobile-nav-current-icon"),
            label: trigger.querySelector(".mobile-nav-current-label")
        };

        trigger.addEventListener("click", event => {
            event.stopPropagation();
            const isOpen = candidate.wrap.classList.toggle("mobile-nav-open");
            trigger.setAttribute("aria-expanded", isOpen ? "true" : "false");
            if (isOpen) syncNav(candidate.wrap);
        });

        document.addEventListener("click", event => {
            if (!candidate.wrap.contains(event.target)) closeNav(candidate.wrap);
        });
        document.addEventListener("keydown", event => {
            if (event.key === "Escape") closeNav(candidate.wrap);
        });

        const observer = new MutationObserver(() => syncNav(candidate.wrap));
        observer.observe(candidate.nav, { attributes: true, subtree: true, attributeFilter: ["class", "aria-current"] });
        syncNav(candidate.wrap);
        return true;
    }

    function boot() {
        if (enhanceNav()) return;
        const observer = new MutationObserver(() => {
            if (enhanceNav()) observer.disconnect();
        });
        observer.observe(document.documentElement, { childList: true, subtree: true });
        window.setTimeout(() => observer.disconnect(), 5000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot, { once: true });
    } else {
        boot();
    }
})();
