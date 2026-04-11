import re

with open('index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update Dropdown CSS & Dropdown Menu CSS
chain_menu = """        /* Dropdown menu */
        .assets-chain-dropdown-menu {
            display: none;
            position: absolute;
            top: calc(100% + 6px);
            left: 0;
            background: rgba(15, 23, 42, 0.97);
            border: 1px solid rgba(52, 211, 153, 0.2);
            border-radius: 14px;
            padding: 6px;
            z-index: 200;
            backdrop-filter: blur(20px);
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.55), 0 0 20px rgba(52, 211, 153, 0.06);
            min-width: 180px;
            max-height: 320px;
            overflow-y: auto;
        }"""
content = re.sub(r'        /\* Dropdown menu \*/.*?overflow-y: auto;\n        }', chain_menu, content, flags=re.DOTALL)

filter_menu = """        .assets-th-filter-dropdown {
            display: none;
            position: absolute;
            top: calc(100% + 4px);
            left: 0;
            background: rgba(15, 23, 42, 0.97);
            border: 1px solid rgba(52, 211, 153, 0.2);
            border-radius: 14px;
            padding: 6px;
            z-index: 100;
            backdrop-filter: blur(20px);
            box-shadow: 0 10px 36px rgba(0, 0, 0, 0.5), 0 0 16px rgba(52, 211, 153, 0.05);
            min-width: 110px;
        }"""
content = re.sub(r'        \.assets-th-filter-dropdown \{.*?min-width: 110px;\n        }', filter_menu, content, flags=re.DOTALL)

content = content.replace("        .assets-chain-dd-opt:hover {\n            background: rgba(255, 255, 255, 0.04);", "        .assets-chain-dd-opt:hover {\n            background: rgba(255, 255, 255, 0.05);")
content = content.replace("        .assets-th-filter-opt:hover {\n            background: rgba(255, 255, 255, 0.04);", "        .assets-th-filter-opt:hover {\n            background: rgba(255, 255, 255, 0.05);")

# 2. Add Settings CSS
settings_css = """        /* ═══ Premium Rewards Settings Dropdown ═══ */
        .assets-settings-btn {
            display: flex;
            align-items: center;
            gap: 6px;
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 20px;
            padding: 5px 12px 5px 10px;
            cursor: pointer;
            color: rgba(255,255,255,0.5);
            font-size: 11px;
            font-weight: 600;
            transition: all 0.25s ease;
            user-select: none;
        }

        .assets-settings-btn:hover {
            border-color: rgba(255,255,255,0.15);
            background: rgba(255,255,255,0.06);
        }

        .assets-settings-btn.open {
            border-color: rgba(99,102,241,0.4);
            background: rgba(99,102,241,0.08);
            color: #c7d2fe;
            box-shadow: 0 0 12px rgba(99,102,241,0.15);
        }

        .settings-gear-icon {
            width: 14px;
            height: 14px;
            transition: transform 0.3s ease;
            opacity: 0.7;
        }

        .assets-settings-btn.open .settings-gear-icon {
            transform: rotate(45deg);
            opacity: 1;
        }

        /* Context dots */
        .settings-status-dots {
            display: flex;
            gap: 2px;
            margin-right: -2px;
        }

        .settings-status-dot {
            width: 5px;
            height: 5px;
            border-radius: 50%;
            transition: all 0.25s ease;
        }

        .settings-status-dot.active.odolo { background: #f59e0b; box-shadow: 0 0 4px rgba(245,158,11,0.5); }
        .settings-status-dot.active.yield { background: #60a5fa; box-shadow: 0 0 4px rgba(96,165,250,0.5); }
        .settings-status-dot.active.lending { background: rgba(255,255,255,0.8); box-shadow: 0 0 4px rgba(255,255,255,0.3); }

        .settings-status-dot.inactive {
            background: rgba(255,255,255,0.15);
            box-shadow: none;
        }

        /* Dropdown panel */
        .assets-settings-dropdown {
            display: none;
            position: absolute;
            top: calc(100% + 8px);
            right: 0;
            min-width: 230px;
            background: rgba(15, 23, 42, 0.97);
            border: 1px solid rgba(99,102,241,0.2);
            border-radius: 14px;
            padding: 6px;
            backdrop-filter: blur(20px);
            box-shadow: 0 8px 32px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.03);
            z-index: 1500;
            animation: settingsSlideIn 0.2s ease-out;
        }

        @keyframes settingsSlideIn {
            from { opacity: 0; transform: translateY(-6px); }
            to { opacity: 1; transform: translateY(0); }
        }

        .assets-settings-dropdown.show {
            display: block;
        }

        .assets-settings-dropdown-header {
            padding: 6px 16px 8px;
            font-size: 9px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 1.6px;
            color: rgba(255,255,255,0.3);
        }

        .assets-settings-dropdown-divider {
            height: 1px;
            background: rgba(255,255,255,0.06);
            margin: 4px 12px;
        }

        /* Reward checkbox rows */
        .assets-reward-row {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 8px 12px;
            cursor: pointer;
            transition: background 0.15s ease;
            user-select: none;
            border-radius: 8px;
        }

        .assets-reward-row:hover {
            background: rgba(255,255,255,0.05);
        }

        /* Custom checkbox */
        .assets-reward-check {
            width: 16px;
            height: 16px;
            border-radius: 4px;
            border: 1.5px solid rgba(255,255,255,0.2);
            background: transparent;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
            transition: all 0.2s ease;
        }

        .assets-reward-check svg {
            width: 10px;
            height: 10px;
            stroke: #fff;
            transform: scale(0);
            transition: transform 0.2s cubic-bezier(0.34, 1.56, 0.64, 1);
        }

        .assets-reward-row.checked .assets-reward-check {
            background: rgba(52, 211, 153, 0.15);
            border-color: rgba(52, 211, 153, 0.4);
        }

        .assets-reward-row.checked .assets-reward-check svg {
            transform: scale(1);
            stroke: #34d399;
        }

        /* Color dot icon for row */
        .assets-reward-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            flex-shrink: 0;
        }
        #settings-row-odolo .assets-reward-dot { background: #f59e0b; box-shadow: 0 0 6px rgba(245,158,11,0.5); }
        #settings-row-yield .assets-reward-dot { background: #60a5fa; box-shadow: 0 0 6px rgba(96,165,250,0.5); }
        #settings-row-lending .assets-reward-dot { background: #e2e8f0; box-shadow: 0 0 6px rgba(255,255,255,0.3); }

        .assets-reward-row.checked .assets-reward-label {
            color: rgba(255,255,255,0.9);
        }

        /* Excluded state — dim only */
        .assets-reward-row:not(.checked) .assets-reward-label {
            color: rgba(255,255,255,0.25);
        }

        .assets-reward-row:not(.checked) .assets-reward-dot {
            opacity: 0.25;
        }

        /* Text wrapper for label + description */
        .assets-reward-text {
            display: flex;
            flex-direction: column;
            gap: 1px;
            flex: 1;
            min-width: 0;
        }

        .assets-reward-desc {
            font-size: 10px;
            color: rgba(255,255,255,0.25);
            font-weight: 400;
            line-height: 1.3;
        }

        .assets-reward-row:not(.checked) .assets-reward-desc {
            opacity: 0.5;
        }

        /* Active indicator dots on gear button */
        .assets-settings-btn .settings-status-dots {
            display: flex;
            gap: 2px;
            margin-right: 4px;
        }

        .assets-settings-btn .settings-chevron {
            opacity: 0.4;
            transition: transform 0.25s ease;
        }
        .assets-settings-btn.open .settings-chevron {
            transform: rotate(180deg);
            opacity: 0.8;
        }"""
content = re.sub(r'(        /\* ═══ Dropdowns ═══ \*/)', r'\1\n' + settings_css + '\n', content)

# 3. Fix line tooltips cursor CSS
content = content.replace("        .gm-part[title] {\n            cursor: help;\n        }", "        .gm-part[title] {\n            cursor: default;\n        }")
content = content.replace("        .assets-apy-breakdown .lending-part {", "        .assets-apy-breakdown span {\n            cursor: default;\n        }\n\n        .assets-apy-breakdown .lending-part {")
content = content.replace("        .assets-apy-breakdown .gm-part {\n            color: #60a5fa;\n            cursor: help;\n        }", "        .assets-apy-breakdown .gm-part {\n            color: #60a5fa;\n        }")

# 4. Modify the Toolbar HTML
old_html = """                            <div class="assets-modifier-sep"></div>
                            <div class="odolo-toggle-wrap" id="assets-odolo-toggle"
                                data-tooltip="Show or hide protocol emission APRs">
                                <img class="odolo-toggle-icon" src="odolo-logo-official.svg" alt="oDOLO">
                                <span class="odolo-toggle-label">oDOLO Rewards</span>
                            </div>
                            <div class="odolo-toggle-wrap gm-toggle-wrap" id="assets-yield-toggle"
                                data-tooltip="Show or hide external yield (e.g. GM performance)">
                                <span class="odolo-toggle-label">External Yield</span>
                            </div>
                            <div class="odolo-toggle-wrap d-toggle-wrap" id="assets-lending-toggle"
                                data-tooltip="Show or hide base lending interest">
                                <span class="odolo-toggle-label">Lending Interest</span>
                            </div>"""

new_html = """                            <div class="assets-modifier-sep"></div>
                            <div style="position: relative;">
                                <button class="assets-settings-btn" id="assets-settings-btn" onclick="assets_toggleSettings(event)">
                                    <svg class="settings-gear-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"></circle><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path></svg>
                                    Rewards
                                    <div class="settings-status-dots">
                                        <div class="settings-status-dot active odolo" id="settings-dot-odolo"></div>
                                        <div class="settings-status-dot active yield" id="settings-dot-yield"></div>
                                        <div class="settings-status-dot active lending" id="settings-dot-lending"></div>
                                    </div>
                                    <svg class="settings-chevron" width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><polyline points="6 9 12 15 18 9"/></svg>
                                </button>
                                <div class="assets-settings-dropdown" id="assets-settings-dropdown">
                                    <div class="assets-settings-dropdown-header">Supply APR Sources</div>
                                    <div class="assets-reward-row checked" data-reward="odolo" id="settings-row-odolo">
                                        <div class="assets-reward-check">
                                            <svg viewBox="0 0 12 12" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="2 6 5 9 10 3"/></svg>
                                        </div>
                                        <div class="assets-reward-dot"></div>
                                        <div class="assets-reward-text">
                                            <span class="assets-reward-label">oDOLO Rewards</span>
                                            <span class="assets-reward-desc">Incentive emissions from protocol</span>
                                        </div>
                                    </div>
                                    <div class="assets-settings-dropdown-divider"></div>
                                    <div class="assets-reward-row checked" data-reward="yield" id="settings-row-yield">
                                        <div class="assets-reward-check">
                                            <svg viewBox="0 0 12 12" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="2 6 5 9 10 3"/></svg>
                                        </div>
                                        <div class="assets-reward-dot"></div>
                                        <div class="assets-reward-text">
                                            <span class="assets-reward-label">External Yield</span>
                                            <span class="assets-reward-desc">Staking, GM Performance, native APR</span>
                                        </div>
                                    </div>
                                    <div class="assets-settings-dropdown-divider"></div>
                                    <div class="assets-reward-row checked" data-reward="lending" id="settings-row-lending">
                                        <div class="assets-reward-check">
                                            <svg viewBox="0 0 12 12" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="2 6 5 9 10 3"/></svg>
                                        </div>
                                        <div class="assets-reward-dot"></div>
                                        <div class="assets-reward-text">
                                            <span class="assets-reward-label">Lending Interest</span>
                                            <span class="assets-reward-desc">Interest earned from borrowers</span>
                                        </div>
                                    </div>
                                </div>
                            </div>"""
content = content.replace(old_html, new_html)

# Set dust to active
content = content.replace('odolo-toggle-wrap dust-toggle-wrap', 'odolo-toggle-wrap dust-toggle-wrap active')
content = content.replace('let assets_hideDust = false;', 'let assets_hideDust = true;')

# Add 20px matching radiuses to others
content = content.replace("border-radius: 22px;\n            padding: 6px 14px;", "border-radius: 20px;\n            padding: 5px 14px;") # Search
content = content.replace("border-radius: 14px;\n            padding: 3px 10px 3px 8px;", "border-radius: 20px;\n            padding: 5px 10px 5px 8px;") # Filter BTN
content = content.replace("background: rgba(99, 102, 241, 0.04);\n            border-radius: 20px;\n            border: 1px solid rgba(99, 102, 241, 0.18);", "background: rgba(99, 102, 241, 0.03);\n            border-radius: 20px;\n            border: 1px solid rgba(99, 102, 241, 0.15);") # APR pill

# 5. JS updates
js_code = """        function assets_toggleSettings(e) {
            e.stopPropagation();
            // Close chain dropdown and filter if open
            const cWrap = document.getElementById('assets-chain-dropdown');
            const cMenu = document.getElementById('assets-chain-dropdown-menu');
            if (cWrap) cWrap.classList.remove('open');
            if (cMenu) cMenu.classList.remove('show');
            const fdd = document.getElementById('assets-th-filter-dropdown');
            const fbtn = document.getElementById('assets-th-filter-btn');
            if (fdd) fdd.classList.remove('show');
            if (fbtn) fbtn.classList.remove('open');
            // Toggle settings
            const sBtn = document.getElementById('assets-settings-btn');
            const sDD = document.getElementById('assets-settings-dropdown');
            const isOpen = sDD.classList.toggle('show');
            sBtn.classList.toggle('open', isOpen);
        }

        function assets_closeSettings() {
            const sBtn = document.getElementById('assets-settings-btn');
            const sDD = document.getElementById('assets-settings-dropdown');
            if (sDD) sDD.classList.remove('show');
            if (sBtn) sBtn.classList.remove('open');
        }

        function assets_toggleChainDropdown(e) {
            e.stopPropagation();
            // Close category filter and settings if open
            const fdd = document.getElementById('assets-th-filter-dropdown');
            const fbtn = document.getElementById('assets-th-filter-btn');
            if (fdd) fdd.classList.remove('show');
            if (fbtn) fbtn.classList.remove('open');
            assets_closeSettings();
            // Toggle chain dropdown
            const wrap = document.getElementById('assets-chain-dropdown');
            const menu = document.getElementById('assets-chain-dropdown-menu');
            const isOpen = wrap.classList.toggle('open');
            menu.classList.toggle('show', isOpen);
        }

        // Close all dropdowns on outside click
        document.addEventListener('click', function() {
            const wrap = document.getElementById('assets-chain-dropdown');
            const menu = document.getElementById('assets-chain-dropdown-menu');
            if (wrap) wrap.classList.remove('open');
            if (menu) menu.classList.remove('show');
            const fdd = document.getElementById('assets-th-filter-dropdown');
            const fbtn = document.getElementById('assets-th-filter-btn');
            if (fdd) fdd.classList.remove('show');
            if (fbtn) fbtn.classList.remove('open');
            assets_closeSettings();
        });"""

old_js1 = """        function assets_toggleChainDropdown(e) {
            e.stopPropagation();
            // Close category filter if open
            const fdd = document.getElementById('assets-th-filter-dropdown');
            const fbtn = document.getElementById('assets-th-filter-btn');
            if (fdd) fdd.classList.remove('show');
            if (fbtn) fbtn.classList.remove('open');

            const wrap = document.getElementById('assets-chain-dropdown');
            const menu = document.getElementById('assets-chain-dropdown-menu');
            const isOpen = wrap.classList.toggle('open');
            menu.classList.toggle('show', isOpen);
        }

        // Close dropdowns on outside click
        document.addEventListener('click', function() {
            const wrap = document.getElementById('assets-chain-dropdown');
            const menu = document.getElementById('assets-chain-dropdown-menu');
            if (wrap) wrap.classList.remove('open');
            if (menu) menu.classList.remove('show');
            
            const fdd = document.getElementById('assets-th-filter-dropdown');
            const fbtn = document.getElementById('assets-th-filter-btn');
            if (fdd) fdd.classList.remove('show');
            if (fbtn) fbtn.classList.remove('open');
        });"""
content = content.replace(old_js1, js_code)

old_js2 = """            const cbEl = document.getElementById('assets-exclude-odolo');
            const toggleEl = document.getElementById('assets-odolo-toggle');
            if (cbEl && toggleEl) {
                toggleEl.addEventListener('click', () => {
                    cbEl.checked = !cbEl.checked;
                    assets_excludeOdolo = cbEl.checked;
                    toggleEl.classList.toggle('active', cbEl.checked);
                    assets_render();
                });
            }
            // Yield toggle
            const yieldToggle = document.getElementById('assets-yield-toggle');
            if (yieldToggle) {
                yieldToggle.addEventListener('click', () => {
                    assets_excludeYield = !assets_excludeYield;
                    yieldToggle.classList.toggle('active', assets_excludeYield);
                    assets_render();
                });

                // Lending toggle
                const lendingToggle = document.getElementById('assets-lending-toggle');
                lendingToggle.addEventListener('click', () => {
                    assets_excludeLending = !assets_excludeLending;
                    lendingToggle.classList.toggle('active', assets_excludeLending);
                    assets_render();
                });
            }"""

new_js2 = """            function assets_updateSettingsStatusDots() {
                const dotO = document.getElementById('settings-dot-odolo');
                const dotY = document.getElementById('settings-dot-yield');
                const dotL = document.getElementById('settings-dot-lending');
                if (dotO) { dotO.className = 'settings-status-dot ' + (assets_excludeOdolo ? 'inactive' : 'active odolo'); }
                if (dotY) { dotY.className = 'settings-status-dot ' + (assets_excludeYield ? 'inactive' : 'active yield'); }
                if (dotL) { dotL.className = 'settings-status-dot ' + (assets_excludeLending ? 'inactive' : 'active lending'); }
            }

            // oDOLO reward row
            const rowOdolo = document.getElementById('settings-row-odolo');
            if (rowOdolo) {
                rowOdolo.addEventListener('click', (e) => {
                    e.stopPropagation();
                    assets_excludeOdolo = !assets_excludeOdolo;
                    rowOdolo.classList.toggle('checked', !assets_excludeOdolo);
                    // Sync hidden checkbox
                    const cbEl = document.getElementById('assets-exclude-odolo');
                    if (cbEl) cbEl.checked = assets_excludeOdolo;
                    assets_updateSettingsStatusDots();
                    assets_render();
                });
            }

            // Yield reward row
            const rowYield = document.getElementById('settings-row-yield');
            if (rowYield) {
                rowYield.addEventListener('click', (e) => {
                    e.stopPropagation();
                    assets_excludeYield = !assets_excludeYield;
                    rowYield.classList.toggle('checked', !assets_excludeYield);
                    assets_updateSettingsStatusDots();
                    assets_render();
                });
            }

            // Lending reward row
            const rowLending = document.getElementById('settings-row-lending');
            if (rowLending) {
                rowLending.addEventListener('click', (e) => {
                    e.stopPropagation();
                    assets_excludeLending = !assets_excludeLending;
                    rowLending.classList.toggle('checked', !assets_excludeLending);
                    assets_updateSettingsStatusDots();
                    assets_render();
                });
            }"""
content = content.replace(old_js2, new_js2)

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(content)

