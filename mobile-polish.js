(function () {
    const MOBILE_QUERY = window.matchMedia("(max-width: 760px)");
    const SCROLL_SHELL_SELECTOR = [
        ".tbl-wrap",
        ".flows-wrap",
        ".table-scroll",
        ".table-container",
        ".table-shell",
        ".liquidation-table-container",
        ".earn-table-scroll",
        ".earn-section-table-shell",
        ".earn-past-body",
        ".supply-table-scroll",
        ".supply-table-shell"
    ].join(",");
    const DROPDOWN_PANEL_SELECTOR = [
        ".dd-panel.show",
        ".dropdown-panel.show",
        ".custom-dropdown-menu.show",
        ".premium-supply-dropdown-menu",
        ".premium-supply-dropdown-menu.show",
        ".filter-menu.show",
        ".earn-chain-dropdown.open .earn-chain-menu",
        ".tvl-dd-panel.show",
        ".supply-activity-type-filter.open .supply-activity-type-menu",
        ".chain-filter.open .chain-menu",
        ".risk-dropdown.open .risk-menu",
        ".liquidation-sim-dropdown.open .sim-asset-popover"
    ].join(",");

    let scheduled = false;

    function ensureSiteCredit() {
        if (!document.body) return;
        let bar = document.getElementById("site-source-credit");
        if (!bar) {
            bar = document.createElement("div");
            document.body.appendChild(bar);
        }
        bar.id = "site-source-credit";
        bar.className = "site-source-credit";
        bar.innerHTML = '<a href="https://x.com/twojekrypto" target="_blank" rel="noopener noreferrer" aria-label="Unofficial Dolomite dashboard by TwojeKrypto on X"><span class="site-source-credit-group"><span class="site-source-credit-dolo" aria-hidden="true"><img src="dolo-logo.svg" alt=""></span><span class="site-source-credit-full">Unofficial Dolomite dashboard</span><span class="site-source-credit-short">Unofficial dashboard</span></span><span class="site-source-credit-sep" aria-hidden="true"></span><span class="site-source-credit-group site-source-credit-author"><span class="site-source-credit-muted">Source by</span><span class="site-source-credit-x" aria-hidden="true">X</span><strong>TwojeKrypto</strong></span></a>';
        document.body.classList.add("has-site-credit");
    }

    function setBodyState() {
        document.body?.classList.toggle("mobile-polished", MOBILE_QUERY.matches);
    }

    function canFocus(element) {
        return element.hasAttribute("tabindex") || /^(A|BUTTON|INPUT|SELECT|TEXTAREA)$/i.test(element.tagName);
    }

    function prepareShell(shell) {
        if (!shell || shell.dataset.mobilePolishShell === "true") return;
        shell.dataset.mobilePolishShell = "true";
        shell.classList.add("mobile-scroll-shell");
        if (!canFocus(shell)) shell.setAttribute("tabindex", "0");
    }

    function shellForTable(table) {
        const existing = table.closest(SCROLL_SHELL_SELECTOR);
        if (existing) return existing;
        const parent = table.parentElement;
        if (!parent || parent === document.body || parent === document.documentElement) return null;
        return parent;
    }

    function updateTables() {
        if (!document.body) return;
        setBodyState();
        updateExpiryLabels();
        if (!MOBILE_QUERY.matches) return;

        document.querySelectorAll(SCROLL_SHELL_SELECTOR).forEach(shell => {
            prepareShell(shell);
            shell.classList.remove("mobile-scrollable");
        });
        document.querySelectorAll("table").forEach(table => {
            const shell = shellForTable(table);
            if (!shell) return;
            const shellWidth = shell.clientWidth || shell.getBoundingClientRect().width;
            const tableWidth = table.scrollWidth || table.getBoundingClientRect().width;
            if (tableWidth > shellWidth + 2) {
                prepareShell(shell);
                shell.classList.add("mobile-scrollable");
            }
        });
        updateDropdownPanels();
    }

    function updateExpiryLabels() {
        document.querySelectorAll(".expiry-label").forEach(label => {
            if (!label.dataset.mobilePolishFullLabel) {
                label.dataset.mobilePolishFullLabel = label.textContent.trim();
            }
            if (!MOBILE_QUERY.matches) {
                label.textContent = label.dataset.mobilePolishFullLabel;
                return;
            }
            label.textContent = label.dataset.mobilePolishFullLabel.replace(/\b(Q[1-4])\s+20(\d{2})\b/g, "$1\n'$2");
        });
    }

    function getTranslateX(style) {
        if (!style || !style.transform || style.transform === "none") return 0;
        try {
            const Matrix = window.DOMMatrixReadOnly || window.DOMMatrix || window.WebKitCSSMatrix;
            return Matrix ? (new Matrix(style.transform).m41 || 0) : 0;
        } catch (error) {
            return 0;
        }
    }

    function updateDropdownPanels() {
        const edge = 8;
        const maxRight = window.innerWidth - edge;
        document.querySelectorAll(DROPDOWN_PANEL_SELECTOR).forEach(panel => {
            const style = window.getComputedStyle(panel);
            if (style.display === "none" || style.visibility === "hidden") return;
            panel.style.removeProperty("max-height");
            let rect = panel.getBoundingClientRect();
            const bottomOverflow = rect.bottom - window.innerHeight + edge;
            const shouldAvoidPageScroll = panel.matches(".sim-asset-popover, .sim-multi-token-popover");
            if (bottomOverflow > 0 && style.position !== "fixed" && !shouldAvoidPageScroll) {
                window.__mobilePolishDropdownScroll = true;
                window.__mobilePolishDropdownScrollUntil = Date.now() + 300;
                window.scrollBy(0, Math.ceil(bottomOverflow));
                window.setTimeout(() => { window.__mobilePolishDropdownScroll = false; }, 300);
                rect = panel.getBoundingClientRect();
            }
            const availableBelow = window.innerHeight - rect.top - edge;
            if (availableBelow > 72) {
                panel.style.setProperty("max-height", `${Math.floor(availableBelow)}px`, "important");
                rect = panel.getBoundingClientRect();
            }
            const currentTranslateX = getTranslateX(style);
            const unshiftedLeft = rect.left - currentTranslateX;
            const unshiftedRight = rect.right - currentTranslateX;
            let shift = 0;
            if (unshiftedLeft < edge) shift = edge - unshiftedLeft;
            if (unshiftedRight + shift > maxRight) shift += maxRight - (unshiftedRight + shift);
            panel.style.setProperty("--mobile-panel-shift", `${Math.round(shift)}px`);
        });
    }

    function scheduleUpdate() {
        if (scheduled) return;
        scheduled = true;
        window.requestAnimationFrame(() => {
            scheduled = false;
            updateTables();
        });
    }

    function boot() {
        ensureSiteCredit();
        setBodyState();
        scheduleUpdate();
        MOBILE_QUERY.addEventListener?.("change", scheduleUpdate);
        window.addEventListener("resize", scheduleUpdate, { passive: true });
        window.addEventListener("orientationchange", scheduleUpdate, { passive: true });
        window.addEventListener("load", scheduleUpdate, { once: true });
        document.addEventListener("click", scheduleUpdate, true);

        const observer = new MutationObserver(scheduleUpdate);
        observer.observe(document.documentElement, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ["class", "aria-expanded"]
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot, { once: true });
    } else {
        boot();
    }
})();
