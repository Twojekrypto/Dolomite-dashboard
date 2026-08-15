import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const pages = Object.fromEntries(
  ["dolo", "odolo", "vedolo", "liquidation", "revenue"]
    .map(name => [name, fs.readFileSync(`${name}-preview.html`, "utf8")]),
);
const routes = [
  "index.html",
  "dolo/index.html",
  "odolo/index.html",
  "vedolo/index.html",
  "borrow/index.html",
  "liquidation/index.html",
  "supply/index.html",
  "revenue/index.html",
];

test("DOLO, oDOLO and veDOLO wallet tables use the canonical shared renderer", () => {
  ["dolo", "odolo", "vedolo"].forEach(name => {
    assert.match(pages[name], /DoloWalletTableUX\.walletCellHtml\(/, name);
  });
  assert.match(pages.dolo, /function holderWalletAddressCell\([\s\S]*?DoloWalletTableUX\.walletCellHtml/);
  assert.match(pages.dolo, /function renderFreshWallets\([\s\S]*?DoloWalletTableUX\.walletCellHtml/);
  assert.match(pages.dolo, /function walletCell\(row\)[\s\S]*?DoloWalletTableUX\.walletCellHtml/);
  assert.match(pages.odolo, /function odoloWalletCell\([\s\S]*?DoloWalletTableUX\.walletCellHtml/);
  assert.match(pages.vedolo, /function exitAddressCell\([\s\S]*?DoloWalletTableUX\.walletCellHtml/);
  assert.match(pages.vedolo, /function holderCell\([\s\S]*?DoloWalletTableUX\.walletCellHtml/);
});

test("unknown wallet rows do not manufacture Wallet or Smart Contract names", () => {
  assert.doesNotMatch(pages.vedolo, /vedoloAddressInfo\(address\)\?\.label \|\| "Wallet"/);
  assert.doesNotMatch(pages.odolo, /<span class="addr-name addr-generic">Wallet<\/span>/);
  assert.doesNotMatch(pages.dolo, /info\?\.label \|\| "Wallet"/);
  assert.match(pages.revenue, /return \{ name: identity\.known \? String\(identity\.label\) : "", generic: false \}/);
  assert.match(pages.liquidation, /if \(!info\) \{[\s\S]*?data-full-addr/);
  assert.doesNotMatch(pages.liquidation, /info\?\.label \|\| 'Wallet'/);
});

test("all audited renderers resolve names from the DOLO Holders source", () => {
  assert.match(pages.dolo, /labels:window\.DOLO_ADDR_LABELS/);
  assert.match(pages.odolo, /labels:ADDRESS_META/);
  assert.match(pages.vedolo, /labels:VEDOLO_ADDRESS_LABELS/);
  assert.match(pages.liquidation, /window\.resolveDoloWalletIdentity\(lower, \{\}, DOLO_ADDR_LABELS\)/);
  assert.match(pages.revenue, /window\.resolveDoloWalletIdentity\(row\?\.address, \{\}, VEBORROW_WALLET_LABELS\)/);
});

test("every changed production route refreshes the wallet-table UX release", () => {
  routes.forEach(route => {
    const html = fs.readFileSync(route, "utf8");
    assert.match(html, /"version": "[^"]*wallet-table-ux-20260815/, route);
  });
});
