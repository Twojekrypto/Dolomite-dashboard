import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO_HTML = ROOT / "portfolio-preview.html"
PORTFOLIO_ROUTE = ROOT / "portfolio" / "index.html"
SHARED_TOOLTIPS = ROOT / "shared-hover-tooltips.js"
EXERCISERS_JSON = ROOT / "exercisers_by_address.json"
GENERATE_EXERCISERS = ROOT / "generate_exercisers.py"


class PortfolioPreviewContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = PORTFOLIO_HTML.read_text(encoding="utf-8")
        cls.route = PORTFOLIO_ROUTE.read_text(encoding="utf-8")
        cls.shared_tooltips = SHARED_TOOLTIPS.read_text(encoding="utf-8")

    def test_open_borrows_uses_risk_positions_ux(self):
        borrow_render = self.html.split("function renderBorrowPositionsTable()", 1)[1].split("function exerciseSummaryItem", 1)[0]
        self.assertIn("data/liquidation-risk", self.html)
        self.assertIn("loadRiskForWallet", self.html)
        self.assertNotIn('loadJson("liquidation_risk.json")', self.html)
        self.assertIn("buildRiskBorrowRows", self.html)
        self.assertIn('class="pf-table pf-borrow-positions"', self.html)
        self.assertIn('data-sort="hf" data-table="bor"', self.html)
        self.assertNotIn('data-sort="address" data-table="bor">Address', self.html)
        self.assertIn('>Account ID<span class="pf-table-head-info"', self.html)
        self.assertIn('aria-label="About Account ID"', self.html)
        self.assertIn('tabindex="0"', self.html)
        self.assertIn("Dolomite Account ID identifies", self.html)
        self.assertIn("It can contain multiple collateral and debt assets", self.html)
        self.assertIn("document.addEventListener('focusin'", self.shared_tooltips)
        self.assertIn("document.addEventListener('focusout'", self.shared_tooltips)
        self.assertIn("function accountNumberCell(account)", self.html)
        self.assertIn("function shortAccountNumber(account)", self.html)
        self.assertIn('return number.length > 6 ? `${number.slice(0, 3)}…${number.slice(-3)}` : number;', self.html)
        self.assertIn('width:72px;min-width:72px', self.html)
        self.assertIn("${accountNumberCell(r.account)}", self.html)
        self.assertIn("Dolomite account number", self.html)
        self.assertIn('data-copy="${esc(number)}"', self.html)
        self.assertIn('account: p.accountNumber == null ? "" : String(p.accountNumber)', self.html)
        self.assertNotIn('account: String(p.accountNumber || "")', self.html)
        self.assertIn('data-sort="emode" data-table="bor"', self.html)
        self.assertIn("function emodeCell(active)", self.html)
        self.assertIn("pf-emode-badge", self.html)
        self.assertIn("const EMODE_ICON =", self.html)
        self.assertIn('class="pf-emode-icon"', self.html)
        self.assertIn('class="pf-emode-flame"', self.html)
        self.assertIn('fill="#fb923c"', self.html)
        self.assertIn('fill="#fbbf24"', self.html)
        self.assertIn('fill="#fff1c1"', self.html)
        self.assertIn('.pf-emode-icon .pf-emode-flame{width:14px;height:14px', self.html)
        self.assertIn('width:18px;height:18px', self.html)
        self.assertIn('min-height:24px', self.html)
        self.assertNotIn("EMODE_FLAME", self.html)
        self.assertNotIn('<path d="M15 8H9v8h6M9 12h5"/>', self.html)
        self.assertIn("E-Mode applies special risk parameters", self.html)
        self.assertNotIn("${portfolioAddressCell(r.address)}", borrow_render)
        self.assertNotIn("function portfolioAddressCell(addr)", self.html)
        self.assertNotIn("holder-wallet", self.html)
        self.assertIn("data-copy", self.html)
        self.assertIn("navigator.clipboard.writeText(value)", self.html)
        self.assertNotIn(">Open borrow</div>", self.html)
        self.assertNotIn("positionLabel(r)", self.html)
        self.assertNotIn("Position #", self.html)
        self.assertIn("'dGM' : DOLO_CDN + 'GMX.", self.html)
        self.assertIn("'dGMX' : DOLO_CDN + 'GMX.", self.html)
        self.assertNotIn(">Position<span", self.html)
        self.assertNotIn('pf-mini-chip">E-mode', self.html)
        self.assertIn("buildFallbackBorrowRows", self.html)
        self.assertIn("riskBorrows.length ? riskBorrows : buildFallbackBorrowRows(chainResults, addr)", self.html)
        self.assertIn(".pf-hf-badge.unknown", self.html)
        self.assertIn("pf-hf-dot", self.html)
        self.assertIn("${hfText(r.hf)}", self.html)
        self.assertIn('td colspan="7"', borrow_render)
        self.assertNotIn('td colspan="6"', borrow_render)
        self.assertIn("collateralTokens", self.html)
        self.assertIn("debtTokens", self.html)
        self.assertIn("BORROW_DUST_USD = 1", self.html)
        self.assertIn('borFilter: { chains: new Set(), q: "", hideDust: true }', self.html)
        self.assertIn("data-borrow-hide-dust", self.html)
        self.assertIn(">Hide Dust<", self.html)
        self.assertIn("TELEGRAM_ICON", self.html)
        self.assertIn("set-alert-header-btn", self.html)
        self.assertIn("Dolomite_SuperGus_bot", self.html)
        self.assertIn(">Set Alert<", self.html)
        self.assertIn("pf-collateral-main", self.html)
        self.assertIn("pf-debt-main", self.html)
        self.assertIn("color:#34d399", self.html)
        self.assertIn("color:#f87171", self.html)
        self.assertIn("No non-dust borrow positions match the filter.", self.html)
        self.assertIn(
            '#pf-borrows-section .pf-table thead th{\n  padding:10px 12px;\n  font-size:9.5px;\n  letter-spacing:1.2px;\n  background:var(--bg-1);',
            self.html,
        )
        self.assertNotIn("portfolio-layout-editor", self.html)
        for key in ("chain", "account", "health", "emode", "spacer", "collateral", "debt"):
            self.assertIn(f'data-column="{key}"', self.html)
        self.assertIn(
            '<colgroup><col data-column="chain"><col data-column="account"><col data-column="health"><col data-column="emode"><col data-column="spacer"><col data-column="collateral"><col data-column="debt"></colgroup>',
            self.html,
        )
        for column, width in (
            ("chain", "11"),
            ("account", "12.037612"),
            ("health", "10.509403"),
            ("emode", "10.469765"),
            ("spacer", "34.714138"),
            ("collateral", "10.967973"),
            ("debt", "10.301109"),
        ):
            self.assertIn(
                f'.pf-borrow-positions col[data-column="{column}"]{{width:{width}%}}',
                self.html,
            )
        self.assertIn('<td data-column="spacer" aria-hidden="true"></td>', borrow_render)
        self.assertIn('data-column="collateral"><div class="pf-money-cell">', borrow_render)
        self.assertIn('data-column="debt"><div class="pf-money-cell">', borrow_render)
        self.assertNotIn('#pf-borrows-section .pf-table tbody td:nth-child(5)', self.html)

    def test_wallet_summary_tables_keep_saved_column_contracts(self):
        expected = {
            "pf-deposits-section": ("chain", "asset", "amount"),
            "pf-borrows-section": ("chain", "account", "health", "emode", "spacer", "collateral", "debt"),
            "pf-exercises-section": ("date", "route", "spacer", "vedolo", "paired", "paid", "price", "lock"),
        }
        for section, keys in expected.items():
            source = self.html.split(f'id="{section}"', 1)[1].split('</section>', 1)[0]
            for key in keys:
                self.assertIn(f'data-column="{key}"', source)
        deposits_section = self.html.split('id="pf-deposits-section"', 1)[1].split('</section>', 1)[0]
        self.assertNotIn('data-column="value"', deposits_section)
        self.assertIn('<colgroup><col data-column="chain" style="width:11.245641%"><col data-column="asset" style="width:51.7084%"><col data-column="amount" style="width:37.045959%"></colgroup>', self.html)
        self.assertIn('<colgroup><col data-column="date" style="width:12.147272%"><col data-column="route" style="width:13.847996%"><col data-column="spacer" style="width:27.020602%"><col data-column="vedolo" style="width:9.34471%"><col data-column="paired" style="width:11.286907%"><col data-column="paid" style="width:10.415274%"><col data-column="price" style="width:9.076469%"><col data-column="lock" style="width:6.86077%"></colgroup>', self.html)

    def test_wallet_summary_alignment_uses_context_status_and_numeric_lanes(self):
        self.assertIn('#pf-borrows-section .pf-table [data-column="chain"]{\n  text-align:left;', self.html)
        self.assertIn('#pf-borrows-section .pf-table [data-column="account"],\n#pf-borrows-section .pf-table [data-column="health"],\n#pf-borrows-section .pf-table [data-column="emode"]{\n  text-align:center;', self.html)
        self.assertIn('#pf-borrows-section .pf-table [data-column="collateral"],\n#pf-borrows-section .pf-table [data-column="debt"]{\n  text-align:right;', self.html)
        self.assertIn('.pf-borrow-positions .pf-money-cell{\n  align-items:flex-end;', self.html)
        self.assertIn('#pf-exercises-section .flow-route-head,#pf-exercises-section .pf-route-cell,#pf-exercises-section .pf-table [data-column="lock"]{text-align:center}', self.html)

    def test_vedolo_activity_uses_distinct_asset_colors(self):
        self.assertIn('#pf-exercises-section .pf-ex-ve{color:var(--gold-hi)}', self.html)
        self.assertIn('#pf-exercises-section .pf-ex-usdc{color:var(--pf-ex-green)}', self.html)
        self.assertIn('#pf-exercises-section .pf-ex-pair{color:var(--fg-1)}', self.html)
        self.assertIn('#pf-exercises-section .pf-exercise-summary-item.primary .pf-exercise-summary-value{color:var(--fg-1)}', self.html)
        self.assertIn('#pf-exercises-section .pf-exercise-summary-sub .accent-money{color:var(--pf-ex-green)', self.html)
        self.assertIn('<span class="pf-ex-pair">${fmtCompact(claimVe)}</span>', self.html)
        self.assertNotIn('<span class="pf-ex-pair">${fmtCompact(claimVe)} <span class="unit">veDOLO</span></span>', self.html)

    def test_open_borrows_expands_hidden_assets(self):
        self.assertIn("expandedBorrows: new Set()", self.html)
        self.assertIn("function borrowRowKey(row)", self.html)
        self.assertIn("return `${row.chain}:${row.accountId || row.account || row.search}`;", self.html)
        self.assertIn("function hasBorrowTokenOverflow(row)", self.html)
        self.assertIn("collateralTokens.length > 3 || debtTokens.length > 3", self.html)
        self.assertIn("function tokenPills(tokens, chain, options = {})", self.html)
        self.assertIn("list.slice(0, limit)", self.html)
        self.assertIn("list.slice(limit)", self.html)
        self.assertIn('class="pf-token-pill-extra-wrap" aria-hidden="${expanded ? "false" : "true"}"', self.html)
        self.assertIn('class="pf-token-pill pf-token-more">+${list.length - limit}</span>', self.html)
        self.assertIn('data-borrow-row-key="${esc(rowKey)}"', self.html)
        self.assertIn('tabindex="0" aria-expanded="${isExpanded ? "true" : "false"}"', self.html)
        self.assertIn("function shouldIgnoreBorrowExpandClick(target)", self.html)
        self.assertIn("a,button,input,select,textarea,[data-copy]", self.html)
        self.assertIn("function setBorrowRowExpanded(row, expanded)", self.html)
        self.assertIn("function toggleBorrowRowExpanded(row)", self.html)
        self.assertIn('e.key !== "Enter" && e.key !== " "', self.html)
        self.assertIn("state.expandedBorrows.clear();", self.html)
        self.assertIn("tr.has-token-overflow", self.html)
        self.assertIn("tr.position-row-expanded", self.html)

    def test_odolo_pending_summary_does_not_count_paired_dolo(self):
        self.assertIn("const total = held + ve + vOTok;", self.html)
        self.assertNotIn("const total = held + ve + vPair;", self.html)
        self.assertIn("DOLO paired · returned on claim", self.html)
        self.assertIn("const exClaimTxs = exTxs.filter(t => t && t.usdc != null);", self.html)
        self.assertIn("const exVe = exClaimTxs.reduce", self.html)
        self.assertNotIn("const exVe = exTxs.reduce", self.html)
        self.assertIn("option cost", self.html)
        self.assertNotIn("oDOLO converted into veDOLO", self.html)
        self.assertNotIn("DOLO paid ·", self.html)
        self.assertNotIn("DOLO paired · in veDOLO", self.html)

    def test_wallet_summary_uses_compact_lock_and_exercise_details(self):
        summary = self.html.split("function renderSummary(d, odoloBal)", 1)[1].split("function portfolioTimestampMs", 1)[0]
        self.assertIn('`${fmtCompact(d.vedolo.total_vote_weight)} vote power`', summary)
        self.assertNotIn("latest_lock_end", summary)
        self.assertNotIn("${exCount} option exercise", summary)
        self.assertIn("${fmtUSD(exCostUsd)} option cost", summary)
        self.assertIn("avg $${exAvg.toFixed(4)} · oDOLO", summary)
        self.assertNotIn("oDOLO converted into veDOLO", summary)

    def test_wallet_summary_keeps_usd_next_to_dolo_with_safe_mobile_wrapping(self):
        self.assertIn('function setPortfolioMetric(el, value)', self.html)
        self.assertIn('window.CountUpMetric', self.html)
        self.assertIn('class="pf-sum-total-number" data-count-value="0"', self.html)
        self.assertIn('class="pf-count-number" data-count-value="0"', self.html)
        self.assertIn('.pf-sum-headline{display:flex;align-items:flex-end;gap:18px;flex-wrap:wrap;', self.html)
        self.assertIn('.pf-sum-headline{align-items:flex-start;gap:10px}', self.html)
        self.assertIn('display:flex;align-items:baseline;gap:8px;', self.html)
        self.assertIn('pf-sum-usd .lbl', self.html)
        self.assertIn('<span class="lbl">wallet value</span>', self.html)

    def test_deposited_assets_reuses_asset_identity_and_combines_compact_amount_value(self):
        fetch_positions = self.html.split("async function fetchChainPositions(chain, addr)", 1)[1].split("// DOLO/oDOLO wallet balances", 1)[0]
        token_cell = self.html.split("function tokenCell(row)", 1)[1].split("function tokenPills", 1)[0]
        deposits_render = self.html.split("function renderTable(bodyId, countId, rows, f, sort, tableKey)", 1)[1].split("function renderBorrowPositionsTable", 1)[0]
        self.assertIn('name: t.name || t.symbol || "Unknown"', fetch_positions)
        self.assertIn("name: r.name", fetch_positions)
        self.assertIn("function tokenExplorer(chainKey, address)", self.html)
        self.assertIn('tokenIcon(sym, { chain: row.chain, addr: row.addr })', token_cell)
        self.assertIn('class="token-ca addr-tooltip-wrap"', token_cell)
        self.assertIn('data-full-addr="${esc(row.addr)}"', token_cell)
        self.assertIn('class="token-ca-copy"', token_cell)
        self.assertIn('data-copy="${esc(row.addr)}"', token_cell)
        self.assertIn('class="tok-long"', token_cell)
        self.assertIn('const fmtDepositToken = n => {', self.html)
        self.assertIn('const fmtDepositUSD = n => {', self.html)
        self.assertIn('const compactDepositSymbol = sym => {', self.html)
        self.assertIn('<td data-column="amount" class="num pf-deposit-balance">', deposits_render)
        self.assertIn('<div class="pf-deposit-amount">${fmtDepositToken(r.amount)} <span class="pf-deposit-symbol">${esc(compactDepositSymbol(r.sym))}</span></div>', deposits_render)
        self.assertIn('<div class="pf-deposit-usd">${fmtDepositUSD(r.usd)}</div>', deposits_render)
        self.assertNotIn('data-column="value"', deposits_render)
        self.assertIn('colspan="3"', deposits_render)
        self.assertIn('#pf-deposits-section .pf-deposit-usd{color:var(--up)', self.html)

    def test_portfolio_table_headers_show_source_aware_relative_freshness(self):
        headers = {
            "pf-deposits-section": "pf-deposits-updated",
            "pf-borrows-section": "pf-borrows-updated",
            "pf-exercises-section": "pf-exercises-updated",
        }
        for section_id, label_id in headers.items():
            section = self.html.split(f'id="{section_id}"', 1)[1].split('</section>', 1)[0]
            self.assertIn('<span class="pf-meta-label">Data updated</span>', section)
            self.assertIn(f'id="{label_id}"', section)
        self.assertIn('function portfolioUpdatedAt(value)', self.html)
        self.assertIn('function setPortfolioTableUpdated(id, value)', self.html)
        self.assertIn('const el = $(`#${id}`);', self.html)
        self.assertIn('el.textContent = `· ${portfolioUpdatedAt(value)}`;', self.html)
        self.assertIn('updatedAt: manifest && (manifest.generatedAtISO || manifest.generatedAt),', self.html)
        self.assertIn('const livePositionsUpdatedAt = Date.now();', self.html)
        self.assertIn('const borrowsUpdatedAt = riskBorrows.length ? riskJson.updatedAt : livePositionsUpdatedAt;', self.html)
        self.assertIn('const exercisesUpdatedAt = oldestPortfolioUpdatedAt(', self.html)
        self.assertIn('setPortfolioTableUpdated("pf-deposits-updated", livePositionsUpdatedAt);', self.html)
        self.assertIn('setPortfolioTableUpdated("pf-borrows-updated", borrowsUpdatedAt);', self.html)
        self.assertIn('setPortfolioTableUpdated("pf-exercises-updated", exercisesUpdatedAt);', self.html)
        for removed_id in ("pf-deposits-total", "pf-borrows-total", "pf-exercises-total"):
            self.assertNotIn(removed_id, self.html)
        self.assertNotIn("Total supplied", self.html)
        self.assertNotIn("Total debt", self.html)
        self.assertNotIn("Current veDOLO", self.html)
        self.assertIn('#pf-deposits-section .pf-table thead th{background:var(--bg-1)}', self.html)

    def test_portfolio_route_refreshes_inline_usd_and_freshness_labels(self):
        self.assertIn('compact-deposit-symbol-20260716', self.route)

    def test_portfolio_address_hero_omits_live_onchain_label(self):
        hero = self.html.split('<!-- HERO + ADDRESS INPUT -->', 1)[1].split('</section>', 1)[0]
        self.assertNotIn("Live onchain", hero)

    def test_vedolo_activity_footer_only_reports_visible_range(self):
        render = self.html.split("function renderExerciseTable()", 1)[1].split("function renderAll()", 1)[0]
        self.assertIn('`${start + 1}–${Math.min(start + PAGE_SIZE, rows.length)} of ${rows.length.toLocaleString("en-US")}`', render)
        self.assertNotIn("totalPaid", render)
        self.assertNotIn("totalPaired", render)

    def test_vedolo_odolo_exercises_exclude_pairing_noise(self):
        self.assertIn('id="pf-exercises-section"', self.html)
        self.assertIn("veDOLO Position Activity", self.html)
        self.assertIn("Data updated", self.html)
        self.assertIn('class="pf-table pf-exercise-table"', self.html)
        self.assertIn("vedolo_flows.json", self.html)
        self.assertIn("buildExerciseRows(cardData.exer, vedoloFlows, addr, cardData.vedolo)", self.html)
        self.assertIn("currentVedolo", self.html)
        self.assertIn("seenTokenIds", self.html)
        self.assertIn("flowLocks = (flows && flows.locks) || []", self.html)
        self.assertIn("lockByTokenId", self.html)
        self.assertIn("function isDoloPairExerciseTx(tx)", self.html)
        self.assertIn('if (paidTokenKey(tx) === "DOLO") return true;', self.html)
        self.assertIn('if (methodId === "0xf3621c90") return true;', self.html)
        self.assertIn("return doloPaid > 0 && tx.usdc == null;", self.html)
        self.assertIn("function isStableExerciseTx(tx)", self.html)
        self.assertIn("function pairedDoloAmount(tx)", self.html)
        self.assertIn(".filter(tx => isStableExerciseTx(tx) || isDoloPairExerciseTx(tx))", self.html)
        self.assertIn('action: isPair ? "Pair oDOLO + DOLO" : "Exercise oDOLO"', self.html)
        self.assertIn('route: isPair ? "pair" : "odolo"', self.html)
        self.assertIn("pairedDolo: doloPaired", self.html)
        self.assertIn("Paired oDOLO with DOLO before veDOLO claim", self.html)
        self.assertIn("if (tokenId && !isPair) seenTokenIds.add(tokenId);", self.html)
        self.assertIn('if (lock.isOdolo) return;', self.html)
        self.assertIn('"Deposit veDOLO"', self.html)
        self.assertIn('"direct"', self.html)
        self.assertIn("DOLO locked directly, no purchase price", self.html)
        self.assertIn('action: isAirdrop ? "Claim Airdrop" : "Deposit veDOLO"', self.html)
        self.assertIn('route: isAirdrop ? "airdrop" : "direct"', self.html)
        self.assertIn("Airdrop allocation locked as 2-year veDOLO", self.html)
        self.assertIn('action: "Transfer veDOLO"', self.html)
        self.assertIn('route: "transfer"', self.html)
        self.assertIn('transferDirection: direction', self.html)
        self.assertIn('isTransfer: true', self.html)
        self.assertIn("flows && flows.transfers", self.html)
        self.assertIn("currentVedolo && currentVedolo.token_details", self.html)
        self.assertIn("fallbackCurrent: true", self.html)
        self.assertIn("Current holder of position #", self.html)
        self.assertIn("position date · tx pending", self.html)
        self.assertIn('id="pf-exercises-filters"', self.html)
        self.assertIn("EXERCISE_ROUTES", self.html)
        self.assertIn("buildExerciseFilters(state.exercises, state.exFilter, renderAll)", self.html)
        self.assertIn("const routeCounts = rows.reduce", self.html)
        self.assertIn('class="dd-opt-count"', self.html)
        self.assertIn('<colgroup><col data-column="date" style="width:12.147272%"><col data-column="route" style="width:13.847996%"><col data-column="spacer" style="width:27.020602%"><col data-column="vedolo" style="width:9.34471%"><col data-column="paired" style="width:11.286907%"><col data-column="paid" style="width:10.415274%"><col data-column="price" style="width:9.076469%"><col data-column="lock" style="width:6.86077%"></colgroup>', self.html)
        self.assertIn("#pf-exercises-section .flow-route-head,#pf-exercises-section .pf-route-cell,#pf-exercises-section .pf-table [data-column=\"lock\"]{text-align:center}", self.html)
        self.assertIn("#pf-exercises-section .pf-route-tag.cyan", self.html)
        self.assertIn("#pf-exercises-section .pf-route-tag.pair", self.html)
        self.assertIn("#pf-exercises-section .pf-route-tag.up", self.html)
        self.assertIn("#pf-exercises-section .pf-route-tag.gold", self.html)
        self.assertIn("#pf-exercises-section .pf-route-tag.airdrop", self.html)
        self.assertIn('data-sort="vedolo" data-table="ex">veDOLO', self.html)
        self.assertIn('data-sort="route" data-table="ex">Route', self.html)
        self.assertNotIn('data-sort="action" data-table="ex">Action', self.html)
        self.assertIn('data-sort="pair" data-table="ex">DOLO Paired', self.html)
        self.assertIn('"date","vedolo","pair","lock"', self.html)
        self.assertIn('data-sort="paid" data-table="ex">USDC Paid', self.html)
        self.assertIn('data-sort="price" data-table="ex">Price', self.html)
        self.assertIn('data-sort="lock" data-table="ex">Lock', self.html)
        self.assertNotIn('data-sort="paid" data-table="ex">Cost', self.html)
        self.assertIn('<td data-column="paired" class="num">${pairHtml}</td>\n        <td data-column="paid" class="num">${paidHtml}</td>\n        <td data-column="price" class="num">${priceHtml}</td>\n        <td data-column="lock" class="num">${lockHtml}</td>', self.html)
        self.assertNotIn('<td class="num">${lockHtml}</td>\n        <td class="num">${paidHtml}</td>', self.html)
        self.assertIn('const paidHtml = r.isStableClaim ? `<span class="pf-ex-val pf-ex-usdc">${fmtUSD(r.paid)}</span>`', self.html)
        self.assertIn('const pairHtml = r.isPair ? `<span class="pf-ex-val pf-ex-pair">${fmtToken(r.pairedDolo)}</span>`', self.html)
        self.assertNotIn('${esc(r.paidToken)}</span>` : "—";', self.html)
        self.assertIn("applyExerciseFilter(state.exercises, state.exFilter)", self.html)
        self.assertIn("exerciseRouteTag(r.route, r)", self.html)
        self.assertIn("flow-source-tag pf-route-tag", self.html)
        self.assertIn("oDOLO Exercise", self.html)
        self.assertIn("Pair oDOLO + DOLO", self.html)
        self.assertIn("DOLO Airdrop Claim", self.html)
        self.assertIn("Direct veDOLO", self.html)
        self.assertIn("veDOLO Transfer", self.html)
        self.assertIn('const label = isTransfer ? `Transfer ${row && row.transferDirection === "out" ? "Out" : "In"}` : meta.label;', self.html)
        self.assertIn("Exercise", self.html)
        self.assertIn("Pair", self.html)
        self.assertIn("Airdrop", self.html)
        self.assertIn("Direct", self.html)
        self.assertIn("Transfer", self.html)
        self.assertIn('let opts = "";', self.html)
        self.assertNotIn('data-route="all"', self.html)
        self.assertNotIn('dd-opt-name">All Routes', self.html)
        self.assertIn("if (filterState.routes.size === 0) filterState.routes = defaultExerciseRoutes();", self.html)
        self.assertIn("pf-ex-val pf-ex-ve", self.html)
        self.assertIn("pf-ex-val pf-ex-usdc", self.html)
        self.assertIn("pf-ex-val pf-ex-price", self.html)
        self.assertIn("pf-ex-lock", self.html)
        self.assertIn("exerciseLockBucket(r.lockDays)", self.html)
        self.assertIn("pf-latest-date", self.html)
        self.assertIn("fmtExerciseDateMeta(r)", self.html)
        self.assertIn("fmtExerciseRelativeTime", self.html)
        self.assertIn("fmtExerciseClockTime", self.html)
        self.assertIn("const exerciseSortTimeMs = row =>", self.html)
        self.assertIn('return num > 0 ? (num > 1e12 ? num : num * 1000) : 0;', self.html)
        self.assertIn("flowByHash", self.html)
        self.assertIn("timestamp: exerciseTimeMs(tx) || exerciseTimeMs(flow)", self.html)
        self.assertIn("timestamp: exerciseTimeMs(lock)", self.html)
        self.assertNotIn("exerciseDateMeta", self.html)
        self.assertIn("pf-ex-green", self.html)
        self.assertIn("txExplorer(r.chain, r.hash)", self.html)
        self.assertIn('id="pf-exercises-summary"', self.html)
        self.assertIn("renderExerciseSummary(rows)", self.html)
        self.assertIn("grid-template-columns:repeat(4,minmax(0,1fr))", self.html)
        self.assertIn("#pf-exercises-section .pf-exercise-summary-item:first-child{border-left:0}", self.html)
        self.assertNotIn("min-height:78px", self.html)
        self.assertIn("pf-exercise-summary-item.accent", self.html)
        self.assertIn("accent-money", self.html)
        self.assertIn("#pf-exercises-section .pf-exercise-summary-sub .accent-money{color:var(--pf-ex-green)", self.html)
        self.assertIn("#pf-exercises-section .pf-exercise-summary-value .accent-price{color:#9ab7c2", self.html)
        self.assertIn("#pf-exercises-section .pf-exercise-route-filter .dd-btn", self.html)
        self.assertIn("#pf-exercises-section .pf-exercise-route-filter .dd-panel", self.html)
        self.assertIn("#pf-exercises-section .pf-exercise-route-filter .dd-opt.active .dd-opt-check", self.html)
        self.assertIn("background:var(--pf-ex-green)", self.html)
        self.assertIn("const claimRows = rows.filter(r => r.isStableClaim);", self.html)
        self.assertIn("state.vedoloCurrent", self.html)
        self.assertIn('const activityLocked = rows.filter(r => r.route !== "transfer" && r.route !== "pair").reduce', self.html)
        self.assertIn("const lockedVe = currentVe || activityLocked;", self.html)
        self.assertIn("Total locked", self.html)
        self.assertIn("oDOLO Exercises", self.html)
        self.assertIn("Avg price", self.html)
        self.assertIn("USDC per veDOLO", self.html)
        self.assertIn('<span class="accent-money">${fmtUSD(claimPaid)} USDC paid</span>', self.html)
        self.assertIn('<span class="accent-price">${fmtExercisePrice(avgClaimPrice)}</span>', self.html)
        self.assertIn("Vote power", self.html)
        self.assertIn("Current voting weight", self.html)
        self.assertNotIn('totalPaired ? ` · ${fmtToken(totalPaired)} DOLO paired` : ""', self.html)
        self.assertNotIn('exerciseSummaryItem(\n        "Airdrops"', self.html)
        self.assertNotIn('exerciseSummaryItem(\n        "Direct"', self.html)
        self.assertNotIn('exerciseSummaryItem(\n        "Transfers"', self.html)
        self.assertNotIn("direct DOLO lock", self.html)
        self.assertNotIn("Not mixed with paired DOLO", self.html)

    def test_odolo_pair_snapshot_matches_transaction_rows(self):
        data = json.loads(EXERCISERS_JSON.read_text(encoding="utf-8"))
        txs = [
            tx
            for exerciser in data.get("exercisers", [])
            for tx in exerciser.get("txs", [])
        ]
        pair_txs = [tx for tx in txs if str(tx.get("paid_token", "")).upper() == "DOLO"]
        self.assertGreater(len(pair_txs), 0)
        self.assertEqual(len(pair_txs), data.get("total_dolo_pair_exercises"))
        self.assertAlmostEqual(
            sum(float(tx.get("vedolo") or 0) for tx in pair_txs),
            float(data.get("total_dolo_pair_vedolo") or 0),
            places=1,
        )
        self.assertTrue(all(tx.get("dolo_paid") for tx in pair_txs))
        self.assertTrue(all(isinstance(tx.get("timestamp"), int) and tx["timestamp"] > 0 for tx in txs))
        generator = GENERATE_EXERCISERS.read_text(encoding="utf-8")
        self.assertIn('"timestamp": timestamp', generator)

    def test_hash_update_stays_on_portfolio_route_under_base_tag(self):
        self.assertIn('history.replaceState(null, "", `${location.pathname}${location.search}#${addr}`);', self.html)
        self.assertNotIn('history.replaceState(null, "", "#" + addr);', self.html)


if __name__ == "__main__":
    unittest.main()
