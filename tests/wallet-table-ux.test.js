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
