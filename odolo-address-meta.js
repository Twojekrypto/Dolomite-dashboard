(function(){
  const VISUAL_TYPE_BY_SHARED_TYPE = {
    eoa: "eoa",
    ca: "ca",
    multisig: "ca",
    safe: "ca",
    contract_wallet: "ca",
    protocol: "ca",
    contract: "ca",
    dead: "ca",
    lp: "ca",
    cex: "cex",
    investor: "investor",
    bot: "bot",
    liquidator: "bot",
    mm: "mm",
    watch: "mm",
  };

  const BADGE_BY_SHARED_TYPE = {
    eoa: "EOA",
    ca: "Contract",
    multisig: "Safe",
    safe: "Safe",
    contract_wallet: "Safe",
    protocol: "Protocol",
    contract: "Contract",
    dead: "Burn",
    lp: "LP",
    cex: "CEX",
    investor: "Investor",
    bot: "Bot",
    liquidator: "Liquidator",
    mm: "MM",
    watch: "Potential",
  };

  const SOURCE_LABELS = {
    "behavioral-label": "Behavioral label",
    "coingecko-tokenomics": "CoinGecko tokenomics",
    "dolomite-docs": "Dolomite documentation",
    "dolomite-docs-core-proxies": "Dolomite documentation",
    "dolomite-docs-module-dolo": "Dolomite documentation",
    "dolomite-known-address": "Dolomite known address",
    "ens-reverse": "ENS reverse",
    "etherscan-buildclaim": "Etherscan BuildClaim",
    "etherscan-public-label": "Etherscan public label",
    "flow-audit": "On-chain flow audit",
    "heuristic-flow-pattern": "Heuristic flow pattern",
    "manual-review": "Manual review",
    "official-claim-contract-transfer": "Official claim contract",
    "public-label": "Public address label",
    "public-pool-label": "Public pool label",
    "routescan-verified-algebra-pool": "Routescan verified pool",
    "standard-burn-address": "Standard burn address",
    "token-contract": "Token contract",
    "verified-team-allocation": "Verified team allocation",
  };

  function friendlyToken(value){
    return String(value || "")
      .replace(/[-_]+/g, " ")
      .replace(/\b\w/g, character => character.toUpperCase());
  }

  function formatOdoloAddressTooltip(meta){
    if(!meta?.label) return "";
    const parts = [meta.badgeLabel || "Known"];
    if(meta.source) parts.push(SOURCE_LABELS[meta.source] || friendlyToken(meta.source));
    if(meta.confidence){
      const confidenceLabel = meta.confidence === "potential"
        ? "Potential confidence"
        : meta.source === "ens-reverse" && meta.confidence === "confirmed"
          ? "Confirmed identity label"
          : friendlyToken(meta.confidence);
      parts.push(confidenceLabel);
    }
    return parts.join(" · ");
  }

  function normalizeOdoloAddressMeta(meta){
    if(!meta || typeof meta !== "object" || !meta.label) return null;
    const sharedType = String(meta.sharedType || meta.type || "eoa").toLowerCase();
    const normalized = {
      ...meta,
      type: VISUAL_TYPE_BY_SHARED_TYPE[sharedType] || "eoa",
      sharedType,
      badgeLabel: BADGE_BY_SHARED_TYPE[sharedType] || "Known",
    };
    normalized.tooltip = formatOdoloAddressTooltip(normalized);
    return normalized;
  }

  function buildOdoloAddressMeta(sharedLabels){
    const output = {};
    if(!sharedLabels || typeof sharedLabels !== "object") return output;
    Object.entries(sharedLabels).forEach(([address, meta]) => {
      const normalized = normalizeOdoloAddressMeta(meta);
      if(normalized) output[String(address).toLowerCase()] = normalized;
    });
    return output;
  }

  window.normalizeOdoloAddressMeta = normalizeOdoloAddressMeta;
  window.buildOdoloAddressMeta = buildOdoloAddressMeta;
  window.formatOdoloAddressTooltip = formatOdoloAddressTooltip;
})();
