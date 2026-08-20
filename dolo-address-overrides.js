(function(root){
  "use strict";
  if(!root) return;

  /*
   * Behavioural classifications only. These are deliberately not presented as
   * confirmed entity ownership unless a public explorer label supports it.
   */
  const overrides = {
    "0x4e5bc1cd2c421ecfef65395b3237f90a97178c55": {
      label:"BingX-Linked Operational Wallet",
      type:"cex",
      source:"public-explorer-flow-audit-20260820",
      confidence:"high",
      candidate:"BingX-linked operational / omnibus / sweep wallet",
      evidence:"Repeated synchronized multi-asset transfers to the publicly labelled BingX 21 wallet.",
    },
    "0x7bd27a0103e48e25acdb131cc190314562171fde": {
      label:"Automated Trading / Arbitrage",
      type:"trader",
      source:"behavioral-flow-audit-20260820",
      confidence:"probable",
      candidate:"automated trading, arbitrage, or market-making wallet",
      evidence:"High-frequency multi-chain swaps and repeated router interactions; no confirmed CEX ownership label.",
    },
    "0x26c2448c0038874f68cc0d388d96f8d218af3bdf": {
      label:"Probable Automated Execution",
      type:"bot",
      source:"behavioral-flow-audit-20260820",
      confidence:"probable",
      candidate:"automated execution wallet",
      evidence:"Execution-like transaction pattern; no confirmed public entity label located.",
    },
  };

  root.DOLO_ADDRESS_LABELS = Object.assign({}, root.DOLO_ADDRESS_LABELS || {}, overrides);
  root.DOLO_ADDR_LABELS = Object.assign({}, root.DOLO_ADDR_LABELS || {}, overrides);
  root.DOLO_ADDRESS_BEHAVIORAL_OVERRIDES = Object.freeze(Object.assign({}, overrides));
})(typeof window !== "undefined" ? window : globalThis);
