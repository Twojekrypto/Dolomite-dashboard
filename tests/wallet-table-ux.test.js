import assert from "node:assert/strict";
import {createRequire} from "node:module";
import test from "node:test";

global.window = {};
await import("../dolo-address-labels.js");
const walletUx = createRequire(import.meta.url)("../wallet-table-ux.js");

const knownAddress = "0xf977814e90da44bfa03b6295a0616a897441acec";
const unknownAddress = "0x1111111111111111111111111111111111111111";

test("canonical resolver wins over page-local fallback labels", () => {
  const identity = window.resolveDoloWalletIdentity(knownAddress, {label:"Wallet", type:"eoa"});
  assert.equal(identity.known, true);
  assert.equal(identity.label, "Binance Hot Wallet 20");
  assert.equal(identity.type, "cex");
});

test("unknown addresses never receive a synthetic generic wallet label", () => {
  const identity = window.resolveDoloWalletIdentity(unknownAddress, {label:"Wallet", type:"eoa"});
  assert.equal(identity.known, false);
  assert.equal(identity.label, "");
  assert.equal(identity.address, unknownAddress);
});

test("generic contract descriptions are not accepted as wallet names", () => {
  const identity = window.resolveDoloWalletIdentity(unknownAddress, {
    label:"Smart Contract / CA",
    type:"ca",
    source:"inferred-contract-code",
  });
  assert.equal(identity.known, false);
  assert.equal(identity.label, "");
});

test("confirmed generated labels remain available when the canonical mapping has no entry", () => {
  const identity = window.resolveDoloWalletIdentity(unknownAddress, {
    label:"Verified Strategy Wallet",
    type:"protocol",
    source:"verified-generator",
    confidence:"confirmed",
  });
  assert.equal(identity.known, true);
  assert.equal(identity.label, "Verified Strategy Wallet");
  assert.equal(identity.source, "verified-generator");
});

test("wallet renderer uses the approved known-wallet hierarchy with exact transaction metadata", () => {
  const html = walletUx.walletCellHtml({
    address: knownAddress,
    txHash: `0x${"a".repeat(64)}`,
    txTimestamp: Date.UTC(2026, 7, 11) / 1000,
    txChain: "ethereum",
  });
  assert.match(html, /wallet-primary[^>]*>Binance Hot Wallet 20</);
  assert.match(html, /0xf977…acec/);
  assert.match(html, /11 Aug 2026/);
  assert.match(html, /https:\/\/etherscan\.io\/tx\/0x[a]{64}/);
  assert.match(html, /target="_blank" rel="noopener"/);
});

test("unknown wallet renderer preserves the wallet name hierarchy before address metadata", () => {
  const html = walletUx.walletCellHtml({
    address: unknownAddress,
    fallback: {label:"Wallet", type:"eoa"},
    txHash: `0x${"b".repeat(64)}`,
    txTimestamp: Date.UTC(2026, 7, 11) / 1000,
    txChain: "berachain",
  });
  assert.match(html, /wallet-primary[^>]*>Wallet</);
  assert.match(html, /0x1111…1111/);
  assert.match(html, /11 Aug 2026/);
  assert.match(html, /https:\/\/berascan\.com\/tx\/0x[b]{64}/);
});

test("incomplete transaction metadata fails closed", () => {
  const html = walletUx.walletCellHtml({
    address: unknownAddress,
    txHash: `0x${"c".repeat(64)}`,
    txChain: "ethereum",
  });
  assert.doesNotMatch(html, /wallet-tx-meta/);
  assert.doesNotMatch(html, /etherscan/);
});

test("Flow wallet renderer places exact transaction metadata beside the wallet name", () => {
  const html = walletUx.walletCellHtml({
    address: knownAddress,
    txHash: `0x${"d".repeat(64)}`,
    txTimestamp: Date.UTC(2026, 7, 11) / 1000,
    txChain: "ethereum",
    txInPrimaryLine: true,
  });

  assert.match(html, /<div class="wallet-primary-line">[\s\S]*?wallet-primary[\s\S]*?wallet-tx-meta[\s\S]*?<\/div><div class="wallet-secondary">[\s\S]*?wallet-address-actions[\s\S]*?<\/div>/);
  assert.doesNotMatch(html, /wallet-secondary-tx/);
  assert.doesNotMatch(html, /wallet-meta-separator/);
});

test("forced-down Portfolio dropdowns stay below the trigger and stop above the fixed credit bar", () => {
  assert.equal(typeof walletUx.dropdownPanelPlacement, "function");

  const nearFooter = walletUx.dropdownPanelPlacement({
    triggerTop: 342,
    triggerBottom: 378,
    viewportHeight: 720,
    naturalHeight: 330,
    creditTop: 673,
    forceDown: true,
  });
  const afterScroll = walletUx.dropdownPanelPlacement({
    triggerTop: 92,
    triggerBottom: 128,
    viewportHeight: 720,
    naturalHeight: 330,
    creditTop: 673,
    forceDown: true,
  });

  assert.deepEqual(nearFooter, { openUp: false, maxHeight: 281 });
  assert.deepEqual(afterScroll, { openUp: false, maxHeight: 330 });
});

test("Portfolio floats only the dropdown panel while its trigger stays in the natural toolbar", () => {
  assert.equal(typeof walletUx.positionFloatingDropdownPanel, "function");

  const styleBag = () => {
    const values = new Map();
    return {
      setProperty(name, value) { values.set(name, String(value)); },
      removeProperty(name) { values.delete(name); },
      getPropertyValue(name) { return values.get(name) || ""; },
    };
  };
  const classes = new Set();
  const trigger = {
    parentNode: null,
    getBoundingClientRect() {
      return { left: 51, right: 261, top: 342, bottom: 378, width: 210, height: 36 };
    },
  };
  const panel = { parentNode: null, scrollHeight: 330, style: styleBag() };
  const dropdown = {
    dataset: { doloDropdownDirection: "down" },
    style: styleBag(),
    classList: {
      add(name) { classes.add(name); },
      toggle(name, enabled) { enabled ? classes.add(name) : classes.delete(name); },
      contains(name) { return classes.has(name); },
    },
    querySelector(selector) {
      if (selector === ".dd-panel") return panel;
      if (selector === "[data-dd-btn]") return trigger;
      return null;
    },
  };
  trigger.parentNode = dropdown;
  panel.parentNode = dropdown;
  const credit = {
    getBoundingClientRect() { return { top: 673, bottom: 720, height: 47 }; },
  };
  const environment = {
    innerHeight: 720,
    document: {
      documentElement: { clientHeight: 720 },
      querySelector(selector) { return selector === "#site-source-credit" ? credit : null; },
    },
    getComputedStyle(node) {
      return node === credit ? { display: "flex", zIndex: "1350" } : {};
    },
  };

  walletUx.positionFloatingDropdownPanel(dropdown, environment);

  assert.equal(trigger.parentNode, dropdown);
  assert.equal(panel.parentNode, dropdown);
  assert.equal(dropdown.style.getPropertyValue("position"), "");
  assert.equal(dropdown.style.getPropertyValue("z-index"), "");
  assert.equal(panel.style.getPropertyValue("position"), "fixed");
  assert.equal(panel.style.getPropertyValue("left"), "51px");
  assert.equal(panel.style.getPropertyValue("top"), "384px");
  assert.equal(panel.style.getPropertyValue("max-height"), "281px");
  assert.equal(panel.style.getPropertyValue("z-index"), "1349");
  assert.equal(dropdown.classList.contains("dolo-dropdown-panel-floating"), true);

  Object.assign(window, environment);
  walletUx.positionFloatingDropdownPanel(dropdown, 0);
  assert.equal(panel.style.getPropertyValue("z-index"), "1349");
});
