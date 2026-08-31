(function(root, factory){
  const api = factory();
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.PortfolioDoloLiquidity = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function(){
  "use strict";

  const DEFAULT_MIN_POOL_LIQUIDITY_USD = 10000;

  function normalizeAddress(value){
    const address = String(value || "").trim().toLowerCase();
    return /^0x[0-9a-f]{40}$/.test(address) ? address : "";
  }

  function positionOwner(position){
    return normalizeAddress(position && (position.beneficialOwner || position.custodian));
  }

  function buildPortfolioLiquidityModel(data, wallet, options = {}){
    const address = normalizeAddress(wallet);
    const pools = Array.isArray(data && data.pools) ? data.pools : [];
    const activePositions = Array.isArray(data && data.activePositions) ? data.activePositions : [];
    const floor = Number.isFinite(options.minPoolLiquidityUsd)
      ? options.minPoolLiquidityUsd
      : DEFAULT_MIN_POOL_LIQUIDITY_USD;
    const poolsById = new Map(pools.map(pool => [String(pool && (pool.identifier || pool.id) || ""), pool]));

    const positions = address ? activePositions
      .filter(position => positionOwner(position) === address)
      .map(position => ({ ...position, pool: poolsById.get(String(position.poolId || "")) || null }))
      .filter(position => Number.isFinite(Number(position.pool && position.pool.liquidityUsd)) && Number(position.pool.liquidityUsd) >= floor)
      .sort((left, right) => (Number(right.valueUsd) || 0) - (Number(left.valueUsd) || 0) || String(left.id || "").localeCompare(String(right.id || "")))
      : [];

    return {
      visible: positions.length > 0,
      positions,
      generatedAt: data && data.generatedAt || "",
      totalValueUsd: positions.reduce((sum, position) => sum + (Number(position.valueUsd) || 0), 0),
    };
  }

  function validDecimals(decimals){
    return Number.isInteger(decimals) && decimals >= 0 && decimals <= 255;
  }

  function compactRawAmount(raw, decimals){
    if(raw === null || raw === undefined || !/^\d+$/.test(String(raw)) || !validDecimals(decimals)) return "Unavailable";
    const amount = BigInt(String(raw));
    if(amount === 0n) return "0";
    const unit = 10n ** BigInt(decimals);
    const compactUnits = [[1000000000n,"B"],[1000000n,"M"],[1000n,"K"]];
    for(let index = 0; index < compactUnits.length; index += 1){
      const [size, suffix] = compactUnits[index];
      const denominator = unit * size;
      if(amount < denominator) continue;
      const compact = amount >= denominator * 100n
        ? (amount + denominator / 2n) / denominator
        : (amount * 10n + denominator / 2n) / denominator;
      const scale = amount >= denominator * 100n ? 1n : 10n;
      const whole = compact / scale;
      const fraction = compact % scale;
      if(whole === 1000n && index > 0) return `1${compactUnits[index - 1][1]}`;
      return fraction ? `${whole}.${fraction}${suffix}` : `${whole}${suffix}`;
    }
    return roundedRawAmount(raw, decimals, amount >= unit ? 2 : amount * 100n >= unit ? 4 : 6).replace(/\.0+$/, "").replace(/(\.\d*?)0+$/, "$1");
  }

  function roundedRawAmount(raw, decimals, fractionDigits = 2){
    if(raw === null || raw === undefined || !/^\d+$/.test(String(raw)) || !validDecimals(decimals) || !Number.isInteger(fractionDigits) || fractionDigits < 0 || fractionDigits > 12) return "Unavailable";
    const unit = 10n ** BigInt(decimals);
    const scale = 10n ** BigInt(fractionDigits);
    const rounded = (BigInt(String(raw)) * scale + unit / 2n) / unit;
    const whole = rounded / scale;
    const grouped = whole.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    if(!fractionDigits) return grouped;
    return `${grouped}.${(rounded % scale).toString().padStart(fractionDigits, "0")}`;
  }

  function createPortfolioLiquidityController({ section, loadData, render, clear }){
    let revision = 0;
    return {
      async showWallet(wallet){
        const requestRevision = ++revision;
        if(section) section.hidden = true;
        try{
          const data = await loadData();
          if(requestRevision !== revision) return null;
          const model = buildPortfolioLiquidityModel(data, wallet);
          if(model.visible){
            render(model);
            if(section) section.hidden = false;
          }else{
            clear();
          }
          return model;
        }catch(_error){
          if(requestRevision === revision) clear();
          return null;
        }
      },
      hide(){
        revision += 1;
        if(section) section.hidden = true;
        clear();
      },
    };
  }

  return {
    DEFAULT_MIN_POOL_LIQUIDITY_USD,
    normalizeAddress,
    positionOwner,
    buildPortfolioLiquidityModel,
    compactRawAmount,
    roundedRawAmount,
    createPortfolioLiquidityController,
  };
});
