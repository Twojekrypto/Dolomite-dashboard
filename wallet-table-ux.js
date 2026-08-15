(function(root, factory){
  const api = factory(root || {});
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.DoloWalletTableUX = api;
})(typeof window !== "undefined" ? window : globalThis, function(root){
  const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;
  const TX_RE = /^0x[a-fA-F0-9]{64}$/;
  const EXPLORERS = {
    ethereum:"https://etherscan.io/tx/",
    berachain:"https://berascan.com/tx/",
  };

  function escapeHtml(value){
    return String(value ?? "").replace(/[&<>"']/g, char => ({
      "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;",
    })[char]);
  }

  function shortAddress(address){
    const value = String(address || "");
    return ADDRESS_RE.test(value) ? `${value.slice(0, 6)}…${value.slice(-4)}` : value;
  }

  function formatTxDate(timestamp){
    const seconds = Number(timestamp);
    if(!Number.isInteger(seconds) || seconds <= 0) return "";
    return new Intl.DateTimeFormat("en-GB", {
      day:"2-digit", month:"short", year:"numeric", timeZone:"UTC",
    }).format(new Date(seconds * 1000)).replace(/^0/, "");
  }

  function formatTxDateValue(timestamp, dateValue){
    const exactTimestampDate = formatTxDate(timestamp);
    if(exactTimestampDate) return exactTimestampDate;
    if(!dateValue) return "";
    const parsed = new Date(dateValue);
    if(Number.isNaN(parsed.getTime())) return "";
    return new Intl.DateTimeFormat("en-GB", {
      day:"2-digit", month:"short", year:"numeric", timeZone:"UTC",
    }).format(parsed).replace(/^0/, "");
  }

  function txExplorerUrl(chain, txHash){
    const base = EXPLORERS[String(chain || "").toLowerCase()];
    return base && TX_RE.test(String(txHash || "")) ? `${base}${txHash}` : "";
  }

  function resolveIdentity(address, fallback, labels){
    if(typeof root.resolveDoloWalletIdentity === "function"){
      return root.resolveDoloWalletIdentity(address, fallback, labels);
    }
    const normalized = String(address || "").toLowerCase();
    return {address:normalized, known:false, label:"", type:fallback?.type || "", metadata:{}};
  }

  function walletCellHtml(options = {}){
    const address = String(options.address || "").toLowerCase();
    const safeAddress = escapeHtml(address);
    const identity = resolveIdentity(address, options.fallback || {}, options.labels);
    const date = formatTxDateValue(options.txTimestamp, options.txDate);
    const txUrl = date ? txExplorerUrl(options.txChain, options.txHash) : "";
    const displayLabel = identity.known ? identity.label : "Wallet";
    const primary = `<div class="wallet-primary-line"><div class="wallet-primary">${escapeHtml(displayLabel)}</div>${options.badgesHtml || ""}</div>`;
    const copyIcon = options.copyIcon || '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
    const externalIcon = options.externalIcon || '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 3h7v7"></path><path d="M10 14 21 3"></path><path d="M21 14v5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5"></path></svg>';
    const actions = `<span class="wallet-address-actions"><span class="wallet-address addr-mono addr-tooltip-wrap" data-full-addr="${safeAddress}">${escapeHtml(shortAddress(address))}</span><button class="addr-copy" type="button" data-copy="${safeAddress}" aria-label="Copy address">${copyIcon}</button><a class="addr-debank" href="https://debank.com/profile/${safeAddress}" target="_blank" rel="noopener" aria-label="View on DeBank" onclick="event.stopPropagation()"><img src="https://debank.com/favicon.ico" alt="DeBank"></a></span>`;
    const txMeta = txUrl
      ? `<a class="wallet-tx-meta" href="${escapeHtml(txUrl)}" target="_blank" rel="noopener" aria-label="Open transaction from ${escapeHtml(date)}" onclick="event.stopPropagation()"><span>${escapeHtml(date)}</span>${externalIcon}</a>`
      : "";
    const secondary = `<div class="wallet-secondary">${actions}${txMeta ? `<span class="wallet-meta-separator" aria-hidden="true">·</span>${txMeta}` : ""}</div>`;
    return `<div class="wallet-identity-cell" data-wallet-address="${safeAddress}">${primary}${secondary}</div>`;
  }

  return {escapeHtml, shortAddress, formatTxDate, formatTxDateValue, txExplorerUrl, walletCellHtml};
});
