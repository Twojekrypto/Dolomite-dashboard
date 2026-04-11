import re

with open('index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. CHAIN DROPDOWN HTML
old_chain = '<div id="assets-chain-filter" class="dolo-chain-pills"></div>'
new_chain = """<div style="position: relative;" id="assets-chain-dropdown" class="assets-chain-dd-wrap">
                            <button class="assets-chain-dropdown-btn" onclick="assets_toggleChainDropdown(event)">
                                <img src="dolomite-logo.svg" alt="All Chains" style="width:14px;height:14px">
                                All Chains
                                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><polyline points="6 9 12 15 18 9"></polyline></svg>
                            </button>
                            <div class="assets-chain-dropdown-menu" id="assets-chain-dropdown-menu">
                                <button class="assets-chain-dd-opt" onclick="assets_filterChain('all')"><img src="dolomite-logo.svg" alt="All" style="width:14px;height:14px"> All Chains</button>
                                <button class="assets-chain-dd-opt" onclick="assets_filterChain('arbitrum')"><img src="icons/arbitrum.png" alt="Arb" style="width:14px;height:14px;border-radius:50%"> Arbitrum</button>
                                <button class="assets-chain-dd-opt" onclick="assets_filterChain('berachain')"><img src="icons/berachain.png" alt="Bera" style="width:14px;height:14px;border-radius:50%"> Berachain</button>
                                <button class="assets-chain-dd-opt" onclick="assets_filterChain('mantle')"><img src="icons/mantle.png" alt="Mnt" style="width:14px;height:14px;border-radius:50%"> Mantle</button>
                                <button class="assets-chain-dd-opt" onclick="assets_filterChain('base')"><img src="icons/base.png" alt="Base" style="width:14px;height:14px;border-radius:50%"> Base</button>
                                <button class="assets-chain-dd-opt" onclick="assets_filterChain('polygon')"><img src="icons/polygon.png" alt="Poly" style="width:14px;height:14px;border-radius:50%"> Polygon</button>
                            </div>
                        </div>"""
content = content.replace(old_chain, new_chain)

# 2. SETTINGS DROPDOWN HTML & DUST TOGGLE
old_toggles = """<div class="odolo-toggle-wrap" id="assets-odolo-toggle"
                                data-tip="Toggle oDOLO rewards in Supply APR. When ON, oDOLO incentive rewards are included. When OFF, only lending and external yield are shown.">
                                <img class="odolo-toggle-icon" src="odolo-logo-official.svg" alt="oDOLO">
                                <span class="odolo-toggle-label">oDOLO</span>
                                <div class="odolo-switch">
                                    <div class="knob"></div>
                                </div>
                            </div>
                            <div class="odolo-toggle-wrap gm-toggle-wrap" id="assets-yield-toggle"
                                data-tip="Include/exclude external yield sources from Supply APR. Sources: ETH Staking, Sky Savings Rate, GM Performance APR, and other native yields. When OFF, only Dolomite lending rate is shown.">
                                <span class="odolo-toggle-label gm-toggle-label">Yield</span>
                                <div class="odolo-switch gm-switch">
                                    <div class="knob"></div>
                                </div>
                            </div>
                            <div class="odolo-toggle-wrap lending-toggle-wrap" id="assets-lending-toggle"
                                data-tip="Toggle base Dolomite lending APR in Supply APR column. This is interest earned from borrowers. When OFF, only external yield and oDOLO rewards are shown.">
                                <span class="odolo-toggle-label lending-toggle-label">Lending</span>
                                <div class="odolo-switch lending-switch">
                                    <div class="knob"></div>
                                </div>
                            </div>
                            <div class="odolo-toggle-wrap dust-toggle-wrap" id="assets-dust-toggle"
                                data-tip="Hide dust markets — assets with very low liquidity where APR is misleading. A market is 'dust' when depositing $1,000 would change total supply by more than 10%.">"""

new_toggles = """<div class="assets-modifier-sep"></div>
                            <div style="position: relative;">
                                <button class="assets-settings-btn" id="assets-settings-btn" onclick="assets_toggleSettings(event)">
                                    <svg class="settings-gear-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"></circle><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path></svg>
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
                            </div>
                            <div class="odolo-toggle-wrap dust-toggle-wrap active" id="assets-dust-toggle"
                                data-tip="Hide dust markets — assets with very low liquidity where APR is misleading.">"""
content = content.replace(old_toggles, new_toggles)

# 3. CSS ADDITIONS
css_additions = """        /* ═══ Premium Dropdowns ═══ */
        .assets-chain-dropdown-btn {
            display: flex;
            align-items: center;
            gap: 6px;
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 20px;
            padding: 5px 12px 5px 10px;
            cursor: pointer;
            color: rgba(255,255,255,0.8);
            font-size: 13px;
            font-weight: 500;
            transition: all 0.25s ease;
            user-select: none;
        }
        .assets-chain-dropdown-btn:hover { background: rgba(255, 255, 255, 0.05); }
        .assets-chain-dropdown-btn svg { opacity: 0.5; transition: transform 0.2s ease; }
        .assets-chain-dd-wrap.open .assets-chain-dropdown-btn svg { transform: rotate(180deg); }
        .assets-chain-dd-wrap.open .assets-chain-dropdown-btn {
            border-color: rgba(52, 211, 153, 0.4);
            background: rgba(52, 211, 153, 0.08);
            color: #34d399;
        }

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
        }
        .assets-chain-dropdown-menu.show { display: flex; flex-direction: column; }
        .assets-chain-dd-opt {
            display: flex;
            align-items: center;
            gap: 8px;
            width: 100%;
            padding: 8px 10px;
            background: transparent;
            border: none;
            color: rgba(255,255,255,0.7);
            font-size: 13px;
            cursor: pointer;
            border-radius: 8px;
            text-align: left;
            transition: all 0.2s;
        }
        .assets-chain-dd-opt:hover {
            background: rgba(255, 255, 255, 0.05);
            color: #fff;
        }

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
        .assets-settings-btn:hover { background: rgba(255,255,255,0.05); }
        .assets-settings-btn.open {
            border-color: rgba(99,102,241,0.4);
            background: rgba(99,102,241,0.08);
            color: #c7d2fe;
        }
        .settings-gear-icon { width: 14px; height: 14px; transition: transform 0.3s ease; opacity: 0.7; }
        .assets-settings-btn.open .settings-gear-icon { transform: rotate(45deg); opacity: 1; }
        .settings-status-dots { display: flex; gap: 2px; margin-right: -2px; }
        .settings-status-dot { width: 5px; height: 5px; border-radius: 50%; transition: all 0.25s ease; }
        .settings-status-dot.active.odolo { background: #f59e0b; box-shadow: 0 0 4px rgba(245,158,11,0.5); }
        .settings-status-dot.active.yield { background: #60a5fa; box-shadow: 0 0 4px rgba(96,165,250,0.5); }
        .settings-status-dot.active.lending { background: rgba(255,255,255,0.8); box-shadow: 0 0 4px rgba(255,255,255,0.3); }
        .settings-status-dot.inactive { background: rgba(255,255,255,0.15); box-shadow: none; }

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
        }
        .assets-settings-dropdown.show { display: block; }
        .assets-settings-dropdown-header { padding: 6px 16px 8px; font-size: 9px; font-weight: 700; text-transform: uppercase; color: rgba(255,255,255,0.3); }
        .assets-settings-dropdown-divider { height: 1px; background: rgba(255,255,255,0.06); margin: 4px 12px; }
        .assets-reward-row { display: flex; align-items: center; gap: 10px; padding: 8px 12px; cursor: pointer; border-radius: 8px; transition: background 0.15s ease; }
        .assets-reward-row:hover { background: rgba(255,255,255,0.05); }
        .assets-reward-check { width: 16px; height: 16px; border-radius: 4px; border: 1.5px solid rgba(255,255,255,0.2); display: flex; align-items: center; justify-content: center; }
        .assets-reward-check svg { width: 10px; height: 10px; stroke: #fff; transform: scale(0); transition: transform 0.2s; }
        .assets-reward-row.checked .assets-reward-check { background: rgba(52, 211, 153, 0.15); border-color: rgba(52, 211, 153, 0.4); }
        .assets-reward-row.checked .assets-reward-check svg { transform: scale(1); stroke: #34d399; }
        .assets-reward-dot { width: 8px; height: 8px; border-radius: 50%; }
        #settings-row-odolo .assets-reward-dot { background: #f59e0b; box-shadow: 0 0 6px rgba(245,158,11,0.5); }
        #settings-row-yield .assets-reward-dot { background: #60a5fa; box-shadow: 0 0 6px rgba(96,165,250,0.5); }
        #settings-row-lending .assets-reward-dot { background: #e2e8f0; box-shadow: 0 0 6px rgba(255,255,255,0.3); }
        .assets-reward-row:not(.checked) .assets-reward-label { color: rgba(255,255,255,0.25); }
        .assets-reward-row:not(.checked) .assets-reward-dot { opacity: 0.25; }
        .assets-reward-text { display: flex; flex-direction: column; }
        .assets-reward-desc { font-size: 10px; color: rgba(255,255,255,0.25); }
        .assets-reward-row:not(.checked) .assets-reward-desc { opacity: 0.5; }
        .assets-settings-btn .settings-status-dots { margin-right: 4px; }
        .settings-chevron { opacity: 0.4; transition: transform 0.25s; }
        .assets-settings-btn.open .settings-chevron { transform: rotate(180deg); opacity: 0.8; }
        .assets-th-filter-dropdown { background: rgba(15, 23, 42, 0.97); border: 1px solid rgba(52, 211, 153, 0.2); border-radius: 14px; backdrop-filter: blur(20px); }
        .assets-th-filter-opt:hover { background: rgba(255, 255, 255, 0.05); }
        .assets-th-filter-btn { border-radius: 20px; padding: 5px 10px 5px 8px; }
        .assets-th-search { border-radius: 20px; padding: 5px 14px; }
        .apr-pill { background: rgba(99, 102, 241, 0.03); border-radius: 20px; border: 1px solid rgba(99, 102, 241, 0.15); }
"""
content = re.sub(r'(        /\* ═══ TOOLBARY ═══ \*/)', r'\1\n' + css_additions + '\n', content)

# Remove help cursor logic
content = content.replace("        .gm-part[title], .lending-part[title] {\n            cursor: help;\n        }", "        .gm-part[title], .lending-part[title] {\n            cursor: default;\n        }")
content = content.replace("        .assets-apy-breakdown .gm-part {\n            color: #60a5fa;\n            cursor: help;\n        }", "        .assets-apy-breakdown .gm-part {\n            color: #60a5fa;\n        }")

# 4. JS ADDITIONS
old_js_chains = """            const pills = document.querySelectorAll('.dolo-chain-pill');
            pills.forEach(p => {
                p.addEventListener('click', () => {
                    pills.forEach(x => x.classList.remove('active'));
                    p.classList.add('active');
                    assets_currentChain = p.dataset.chain;
                    window._assetsPage = 1;
                    window._assetsEarnPage = 1;
                    assets_render();
                });
            });"""

new_js_chains = """            function assets_toggleChainDropdown(e) { e.stopPropagation(); assets_closeSettings(); const menu = document.getElementById('assets-chain-dropdown-menu'); if (!menu) return; menu.classList.toggle('show'); }
            window.assets_toggleChainDropdown = assets_toggleChainDropdown;
            
            function assets_toggleSettings(e) { e.stopPropagation(); const dd = document.getElementById('assets-settings-dropdown'); if(!dd) return; dd.classList.toggle('show'); }
            window.assets_toggleSettings = assets_toggleSettings;
            
            function assets_closeSettings() { const dd = document.getElementById('assets-settings-dropdown'); if(dd) dd.classList.remove('show'); document.getElementById('assets-chain-dropdown-menu')?.classList.remove('show'); }
            window.assets_closeSettings = assets_closeSettings;
            
            document.addEventListener('click', assets_closeSettings);

            function assets_filterChain(ch) {
                assets_currentChain = ch;
                const btn = document.querySelector('.assets-chain-dropdown-btn');
                if (btn) btn.innerHTML = ch === 'all' 
                    ? '<img src="dolomite-logo.svg" alt="All Chains" style="width:14px;height:14px"> All Chains <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><polyline points="6 9 12 15 18 9"></polyline></svg>'
                    : `<img src="icons/${ch}.png" alt="${ch}" style="width:14px;height:14px;border-radius:50%"> ${ch.charAt(0).toUpperCase() + ch.slice(1)} <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><polyline points="6 9 12 15 18 9"></polyline></svg>`;
                window._assetsPage = 1; window._assetsEarnPage = 1;
                assets_render();
                assets_closeSettings();
            }
            window.assets_filterChain = assets_filterChain;"""
content = content.replace(old_js_chains, new_js_chains)

# REWARD TOGGLES
old_js_toggles = """            const cbEl = document.getElementById('assets-exclude-odolo');
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

new_js_toggles = """            function assets_updateSettingsStatusDots() {
                const dotO = document.getElementById('settings-dot-odolo');
                const dotY = document.getElementById('settings-dot-yield');
                const dotL = document.getElementById('settings-dot-lending');
                if (dotO) dotO.className = 'settings-status-dot ' + (assets_excludeOdolo ? 'inactive' : 'active odolo');
                if (dotY) dotY.className = 'settings-status-dot ' + (assets_excludeYield ? 'inactive' : 'active yield');
                if (dotL) dotL.className = 'settings-status-dot ' + (assets_excludeLending ? 'inactive' : 'active lending');
            }

            const rowOdolo = document.getElementById('settings-row-odolo');
            if (rowOdolo) rowOdolo.addEventListener('click', (e) => { e.stopPropagation(); assets_excludeOdolo = !assets_excludeOdolo; rowOdolo.classList.toggle('checked', !assets_excludeOdolo); const cbEl = document.getElementById('assets-exclude-odolo'); if (cbEl) cbEl.checked = assets_excludeOdolo; assets_updateSettingsStatusDots(); assets_render(); });

            const rowYield = document.getElementById('settings-row-yield');
            if (rowYield) rowYield.addEventListener('click', (e) => { e.stopPropagation(); assets_excludeYield = !assets_excludeYield; rowYield.classList.toggle('checked', !assets_excludeYield); assets_updateSettingsStatusDots(); assets_render(); });

            const rowLending = document.getElementById('settings-row-lending');
            if (rowLending) rowLending.addEventListener('click', (e) => { e.stopPropagation(); assets_excludeLending = !assets_excludeLending; rowLending.classList.toggle('checked', !assets_excludeLending); assets_updateSettingsStatusDots(); assets_render(); });"""
content = content.replace(old_js_toggles, new_js_toggles)

# 5. DUST DEFAULT
content = content.replace('let assets_hideDust = false;', 'let assets_hideDust = true;')
content = content.replace('let doloHideDust = false;', 'let doloHideDust = true;')

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("SCRIPT COMPLETED")
