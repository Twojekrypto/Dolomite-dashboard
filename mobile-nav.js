(function () {
    const LABELS = {
        assets: "Assets",
        tvl: "TVL",
        dolo: "DOLO",
        odolo: "oDOLO",
        vedolo: "veDOLO",
        earn: "Earn",
        borrow: "Borrow",
        supply: "Supply",
        liquidation: "Borrow",
        liquidations: "Borrow",
        lending: "Borrow"
    };

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
        return (item.dataset && item.dataset.tab) || tabFromHref(item.getAttribute("href")) || item.id?.replace(/^tab-/, "") || "";
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

    function itemLabel(item) {
        const tab = tabFromItem(item);
        if (LABELS[tab]) return LABELS[tab];
        const span = item?.querySelector("span");
        return cleanLabel(span ? span.textContent : item?.textContent);
    }

    function activeByLocation(items) {
        const path = window.location.pathname.toLowerCase();
        const route = String(window.__DOLO_ROUTE || "").toLowerCase();
        return items.find(item => {
            const tab = tabFromItem(item);
            if (route && (tab === route || (route === "supply" && tab === "supply") || (route === "liquidations" && tab === "borrow"))) return true;
            if (tab && path.includes("/" + tab + "/")) return true;
            if ((tab === "dolo" || tab === "") && /\/dolomite-dashboard\/?$|\/dolo\/?$/.test(path)) return true;
            return false;
        });
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
        const active = items.find(item => item.classList.contains("active") || item.getAttribute("aria-current") === "page") || activeByLocation(items) || items[0];
        const activeTab = tabFromItem(active);
        state.icon.innerHTML = iconHtml(active);
        state.label.textContent = itemLabel(active);
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

        items.forEach(item => {
            const href = item.getAttribute("href");
            if (!href) return;
            const link = document.createElement("a");
            link.className = "mobile-nav-link";
            link.href = href;
            link.dataset.tab = tabFromItem(item);
            link.innerHTML = '<span class="mobile-nav-link-icon">' + iconHtml(item) + '</span><span class="mobile-nav-link-label">' + itemLabel(item) + '</span>';
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
