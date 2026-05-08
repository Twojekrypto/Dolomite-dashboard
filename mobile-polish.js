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
        ".filter-menu.show"
    ].join(",");

    let scheduled = false;

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
        if (!MOBILE_QUERY.matches) return;

        document.querySelectorAll(SCROLL_SHELL_SELECTOR).forEach(prepareShell);
        document.querySelectorAll("table").forEach(table => {
            const shell = shellForTable(table);
            if (!shell) return;
            const shellWidth = shell.clientWidth || shell.getBoundingClientRect().width;
            const tableWidth = table.scrollWidth || table.getBoundingClientRect().width;
            if (tableWidth > shellWidth + 2) prepareShell(shell);
        });
        updateDropdownPanels();
    }

    function updateDropdownPanels() {
        const edge = 8;
        const maxRight = window.innerWidth - edge;
        document.querySelectorAll(DROPDOWN_PANEL_SELECTOR).forEach(panel => {
            const style = window.getComputedStyle(panel);
            if (style.display === "none" || style.visibility === "hidden") return;
            panel.style.setProperty("--mobile-panel-shift", "0px");
            panel.style.removeProperty("max-height");
            let rect = panel.getBoundingClientRect();
            const availableBelow = window.innerHeight - rect.top - edge;
            if (availableBelow > 120) {
                panel.style.setProperty("max-height", `${Math.floor(availableBelow)}px`, "important");
                rect = panel.getBoundingClientRect();
            }
            let shift = 0;
            if (rect.left < edge) shift = edge - rect.left;
            if (rect.right + shift > maxRight) shift += maxRight - (rect.right + shift);
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
