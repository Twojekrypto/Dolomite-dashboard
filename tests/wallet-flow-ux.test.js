import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const pages = Object.fromEntries(
  ["dolo", "odolo", "vedolo"].map(name => [name, fs.readFileSync(`${name}-preview.html`, "utf8")]),
);

test("all three Flow pages load the shared wallet UX assets", () => {
  Object.entries(pages).forEach(([name, html]) => {
    assert.match(html, /wallet-table-ux\.css\?v=20260815-wallet-flow-consistency/, `${name} shared CSS`);
    assert.match(html, /wallet-table-ux\.js\?v=20260815-wallet-flow-consistency/, `${name} shared JS`);
  });
});

test("DOLO and oDOLO flow adapters preserve optional exact transaction metadata", () => {
  assert.match(pages.dolo, /latestTxHash:item\.latest_tx_hash/);
  assert.match(pages.dolo, /latestTxTimestamp:item\.latest_tx_timestamp/);
  assert.match(pages.dolo, /latestTxChain:item\.latest_tx_chain/);
  assert.match(pages.odolo, /latestTxHash:item\.latest_tx_hash/);
  assert.match(pages.odolo, /latestTxTimestamp:item\.latest_tx_timestamp/);
  assert.match(pages.odolo, /latestTxChain:item\.latest_tx_chain/);
});

test("all three Flow renderers use the same wallet cell renderer", () => {
  assert.match(pages.dolo, /DoloWalletTableUX\.walletCellHtml\(/);
  assert.match(pages.odolo, /DoloWalletTableUX\.walletCellHtml\(/);
  assert.match(pages.vedolo, /DoloWalletTableUX\.walletCellHtml\(/);
});

test("flow rank columns use the same compact centered class", () => {
  Object.entries(pages).forEach(([name, html]) => {
    assert.match(html, /<th[^>]*wallet-rank-col[^>]*>[\s\S]{0,100}?#/, `${name} rank heading`);
    assert.match(html, /<td class="rank wallet-rank-col">/, `${name} rank cells`);
  });
});

test("oDOLO flow sort headers keep a stable marker slot and aria-sort", () => {
  assert.match(pages.odolo, /class="wallet-sort-marker sort" aria-hidden="true"/);
  assert.match(pages.odolo, /marker\.textContent = active \? \(sort\.dir === "asc" \? "▲" : "▼"\) : ""/);
  assert.match(pages.odolo, /th\.setAttribute\("aria-sort", active \? \(sort\.dir === "asc" \? "ascending" : "descending"\) : "none"\)/);
  assert.doesNotMatch(pages.odolo, /if\(marker\) marker\.remove\(\)/);
});

test("veDOLO Flow table uses the shared flow typography/header token", () => {
  assert.match(pages.dolo, /class="flows-tbl wallet-flow-table"/);
  assert.match(pages.odolo, /class="flows-tbl wallet-flow-table"/);
  assert.match(pages.vedolo, /class="tbl wallet-flow-table" id="locks-table"/);
  assert.match(pages.vedolo, /class="tbl wallet-flow-table" id="unlocks-table"/);
});

test("DOLO Holders keeps compact rank and stable inline-flex sort markers", () => {
  assert.match(pages.dolo, /id="tbl-holders"[\s\S]*?<th[^>]*data-sort="rank"[^>]*wallet-rank-col/);
  assert.match(pages.dolo, /class="wallet-sort-content"/);
  assert.match(pages.dolo, /class="wallet-sort-marker sort" aria-hidden="true"/);
  const holderSync = pages.dolo.slice(
    pages.dolo.indexOf('document.querySelectorAll("#tbl-holders thead th")'),
    pages.dolo.indexOf("const start = state.page * state.pageSize"),
  );
  assert.match(holderSync, /s\.textContent = k===state\.sort\.key \? \(state\.sort\.dir==="asc" \? "▲" : "▼"\) : ""/);
  assert.doesNotMatch(holderSync, /s\.remove\(\)/);
});

test("veDOLO navigation stays inside the viewport instead of widening the page", () => {
  assert.match(
    pages.vedolo,
    /\.site-nav-wrap\{[^}]*width:100%;[^}]*max-width:100%;[^}]*min-width:0;[^}]*overflow:hidden[^}]*\}/,
  );
  assert.match(
    pages.vedolo,
    /\.site-nav\{[^}]*max-width:100%;[^}]*min-width:0;[^}]*overflow-x:auto[^}]*\}/,
  );
});
