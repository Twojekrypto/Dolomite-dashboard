#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");
const OUTPUT_PATH = path.join(ROOT, "assets_live.json");

const REQUEST_TIMEOUT_MS = 25_000;
const SCALE = 10n ** 18n;

const LIVE_CHAINS = {
  berachain: {
    id: 80094,
    name: "Berachain",
    subgraph: "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
  },
  arbitrum: {
    id: 42161,
    name: "Arbitrum",
    subgraph: "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
  },
  ethereum: {
    id: 1,
    name: "Ethereum",
    subgraph: "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-ethereum/latest/gn",
  },
  botanix: {
    id: 3637,
    name: "Botanix",
    subgraph: "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-botanix/latest/gn",
  },
  mantle: {
    id: 5000,
    name: "Mantle",
    subgraph: "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-mantle/latest/gn",
  },
  polygonzkevm: {
    id: 1101,
    name: "Polygon zkEVM",
    subgraph: "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-polygon-zkevm/latest/gn",
  },
  xlayer: {
    id: 196,
    name: "X Layer",
    subgraph: "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs/dolomite-x-layer/latest/gn",
  },
};

const STABLE_SYMBOLS = new Set([
  "USDC",
  "USDT",
  "USDT0",
  "USD\u20ae0",
  "DAI",
  "USDC.E",
  "USDC.e",
  "HONEY",
  "USDE",
  "sUSDe",
  "SUSDE",
  "NECT",
  "RUSD",
  "rUSD",
  "SRUSD",
  "srUSD",
  "WSRUSD",
  "wsrUSD",
  "BYUSD",
  "MIM",
  "USDS",
  "sUSDS",
  "SUSDS",
  "USDA",
  "USDa",
  "SAVUSD",
  "savUSD",
  "USD1",
  "USDY",
  "CUSD",
  "cUSD",
  "STCUSD",
  "stcUSD",
]);
const BTC_SYMBOLS = new Set([
  "BTC",
  "WBTC",
  "LBTC",
  "EBTC",
  "eBTC",
  "SOLVBTC",
  "SolvBTC",
  "SOLVBTC.BBN",
  "TBTC",
  "tBTC",
  "UNIBTC",
  "uniBTC",
  "PUMPBTC",
  "PumpBTC",
  "CBBTC",
  "cbBTC",
  "FBTC",
  "STBTC",
  "stBTC",
  "SBTC",
  "pBTC",
]);
const ETH_SYMBOLS = new Set([
  "ETH",
  "WETH",
  "WSTETH",
  "wstETH",
  "STETH",
  "RETH",
  "rETH",
  "WEETH",
  "weETH",
  "BERAETH",
  "beraETH",
  "EZETH",
  "ezETH",
  "RSETH",
  "rsETH",
  "STONE",
  "WOETH",
  "METH",
  "mETH",
  "CMETH",
  "cmETH",
  "BERA",
  "WBERA",
  "iBERA",
  "sWBERA",
  "wgBERA",
  "iBGT",
  "diBGT",
  "oriBGT",
  "PT-iBGT-25DEC2025",
]);
const SKIP_PREFIXES = [
  "dPT-",
  "dYT-",
  "Dolomite Isolation",
  "djUSDC",
  "dfsGLP",
  "dplvGLP",
  "dARB",
  "dGMX",
  "mGLP",
  "sGLP",
];
const MONTH_INDEX = {
  JAN: 0,
  FEB: 1,
  MAR: 2,
  APR: 3,
  MAY: 4,
  JUN: 5,
  JUL: 6,
  AUG: 7,
  SEP: 8,
  OCT: 9,
  NOV: 10,
  DEC: 11,
};

function decimalToScaled(value, places = 18) {
  const scalePlaces = Math.max(0, Number(places || 0));
  let raw = String(value ?? "0").trim();
  if (!raw) return 0n;

  let negative = false;
  if (raw[0] === "+" || raw[0] === "-") {
    negative = raw[0] === "-";
    raw = raw.slice(1);
  }
  if (!raw) return 0n;

  const [wholeRaw, fracRaw = ""] = raw.split(".");
  const wholeDigits = wholeRaw.replace(/[^0-9]/g, "") || "0";
  const fracDigits = (fracRaw.replace(/[^0-9]/g, "") + "0".repeat(scalePlaces)).slice(0, scalePlaces) || "0";
  const scale = 10n ** BigInt(scalePlaces);
  const result = BigInt(wholeDigits) * scale + BigInt(fracDigits);
  return negative ? -result : result;
}

function scaledToNumber(value, places = 18, precision = 14) {
  let raw = BigInt(value || 0n);
  const negative = raw < 0n;
  if (negative) raw = -raw;
  const scalePlaces = Math.max(0, Number(places || 0));
  if (!scalePlaces) return Number(negative ? -raw : raw);

  let digits = raw.toString().padStart(scalePlaces + 1, "0");
  const whole = digits.slice(0, -scalePlaces) || "0";
  let frac = digits.slice(-scalePlaces).replace(/0+$/, "");
  if (frac.length > precision) frac = frac.slice(0, precision);
  const parsed = Number(frac ? `${whole}.${frac}` : whole);
  return negative ? -parsed : parsed;
}

function multiplyScaled(left, right) {
  return (BigInt(left || 0n) * BigInt(right || 0n)) / SCALE;
}

function decimalToNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function cleanNumber(value, digits = 12) {
  if (!Number.isFinite(value)) return 0;
  if (value === 0) return 0;
  return Number(value.toPrecision(digits));
}

function classifyAsset(sym) {
  if (STABLE_SYMBOLS.has(sym)) return "stable";
  if (BTC_SYMBOLS.has(sym)) return "btc";
  if (ETH_SYMBOLS.has(sym)) return "eth";
  return "eth";
}

function maturityDateFor(sym, name = "") {
  const match = `${sym || ""} ${name || ""}`.match(/(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(20\d{2})/i);
  if (!match) return null;
  return new Date(Date.UTC(+match[3], MONTH_INDEX[match[2].toUpperCase()], +match[1], 23, 59, 59));
}

function isExpiredMarket(sym, name = "") {
  const maturity = maturityDateFor(sym, name);
  return !!maturity && maturity < new Date();
}

function shouldSkipMarket(row) {
  const sym = row.token?.tokenSymbol || "";
  const name = row.token?.tokenName || "";
  if (isExpiredMarket(sym, name)) return true;
  return SKIP_PREFIXES.some((prefix) => sym.startsWith(prefix) || name.startsWith(prefix));
}

function parseYieldParts(parts) {
  let odolo = 0;
  let external = 0;
  const sources = [];
  for (const part of parts || []) {
    const raw = decimalToNumber(part.interestRate) * 100;
    if (raw === 0) continue;
    const label =
      String(part.label || "")
        .replace(/\s*\(.*?\)\s*/g, "")
        .replace(/\s+APR$/i, "")
        .replace(/\s+APY$/i, "")
        .trim() || "Yield";
    if (label.toLowerCase().includes("odolo")) {
      odolo += raw;
    } else if (part.category === "rewards" || part.category === "nativeYield") {
      external += raw;
      sources.push({ label, rate: cleanNumber(raw, 10), category: part.category || "yield" });
    }
  }
  return { odolo, external, sources };
}

async function fetchJson(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);
    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function graphQuery(chainKey, query) {
  const chain = LIVE_CHAINS[chainKey];
  const payload = await fetchJson(chain.subgraph, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (payload.errors?.length) {
    throw new Error(`${chainKey} GraphQL error: ${payload.errors[0].message || "unknown error"}`);
  }
  return payload.data || {};
}

async function fetchMarketDepth() {
  const query = `{
    totalPars(first: 1000) { id supplyPar borrowPar token { id } }
    marketRiskInfos(first: 1000) { id supplyMaxWei borrowMaxWei isBorrowingDisabled }
    interestIndexes(first: 1000) { id supplyIndex borrowIndex }
    oraclePrices(first: 1000) { id price token { id } }
  }`;
  const out = {};
  await Promise.all(
    Object.keys(LIVE_CHAINS).map(async (chainKey) => {
      const data = await graphQuery(chainKey, query);
      const indexes = {};
      for (const row of data.interestIndexes || []) {
        const addr = String(row.id || "").toLowerCase();
        if (!addr) continue;
        indexes[addr] = {
          supplyIndex: decimalToScaled(row.supplyIndex || "1", 18),
          borrowIndex: decimalToScaled(row.borrowIndex || "1", 18),
        };
      }

      const caps = {};
      for (const row of data.marketRiskInfos || []) {
        const addr = String(row.id || "").toLowerCase();
        if (!addr) continue;
        caps[addr] = {
          supplyCap: row.supplyMaxWei === null || row.supplyMaxWei === undefined ? 0n : decimalToScaled(row.supplyMaxWei, 18),
          borrowCap: row.borrowMaxWei === null || row.borrowMaxWei === undefined ? 0n : decimalToScaled(row.borrowMaxWei, 18),
          disabled: row.isBorrowingDisabled === true,
        };
      }

      const prices = {};
      for (const row of data.oraclePrices || []) {
        const addr = String(row.token?.id || row.id || "").toLowerCase();
        if (addr) prices[addr] = decimalToScaled(row.price || "0", 18);
      }

      for (const totalPar of data.totalPars || []) {
        const addr = String(totalPar.token?.id || "").toLowerCase();
        if (!addr) continue;
        const idx = indexes[addr] || { supplyIndex: SCALE, borrowIndex: SCALE };
        const cap = caps[addr] || { supplyCap: 0n, borrowCap: 0n, disabled: false };
        const suppliedScaled = multiplyScaled(decimalToScaled(totalPar.supplyPar || "0", 18), idx.supplyIndex);
        const borrowedScaled = multiplyScaled(decimalToScaled(totalPar.borrowPar || "0", 18), idx.borrowIndex);
        const availableScaled = suppliedScaled > borrowedScaled ? suppliedScaled - borrowedScaled : 0n;
        out[`${chainKey}:${addr}`] = {
          supplied: cleanNumber(scaledToNumber(suppliedScaled), 14),
          borrowed: cleanNumber(scaledToNumber(borrowedScaled), 14),
          availableLiquidity: cleanNumber(scaledToNumber(availableScaled), 14),
          supplyCap: cleanNumber(scaledToNumber(cap.supplyCap), 14),
          borrowCap: cleanNumber(scaledToNumber(cap.borrowCap), 14),
          borrowDisabled: cap.disabled,
          price: cleanNumber(scaledToNumber(prices[addr] || 0n), 14),
        };
      }
    }),
  );
  return out;
}

async function fetchRateRowsForChain(chainKey) {
  const chain = LIVE_CHAINS[chainKey];
  const payload = await fetchJson(`https://api.dolomite.io/tokens/${chain.id}/interest-rates?exclude-odolo=false`);
  const rows = [];
  for (const row of payload.interestRates || []) {
    if (shouldSkipMarket(row)) continue;
    const token = row.token || {};
    const sym = token.tokenSymbol || "UNKNOWN";
    const addr = String(token.tokenAddress || "").toLowerCase();
    const parts = parseYieldParts(row.outsideSupplyInterestRateParts);
    rows.push({
      key: `${chainKey}:${addr || String(token.marketId || sym).toLowerCase()}`,
      sym,
      name: token.tokenName || sym,
      addr,
      marketId: token.marketId,
      cat: classifyAsset(sym),
      chain: chainKey,
      chainName: chain.name,
      price: 0,
      odolo: cleanNumber(parts.odolo, 10),
      yield: cleanNumber(parts.external, 10),
      yieldSources: parts.sources,
      lending: cleanNumber(decimalToNumber(row.supplyInterestRate) * 100, 10),
      borrow: cleanNumber(decimalToNumber(row.borrowInterestRate) * 100, 10),
      supplyApr: cleanNumber(decimalToNumber(row.totalSupplyInterestRate) * 100, 10),
      lowerOptimalApr: cleanNumber(decimalToNumber(row.lowerOptimalRate) * 100, 10),
      upperOptimalApr: cleanNumber(decimalToNumber(row.upperOptimalRate) * 100, 10),
      optimalUtilizationRate: cleanNumber(decimalToNumber(row.optimalUtilizationRate), 10),
      tvl: 0,
      util: 0,
      dust: false,
      borrowDisabled: false,
      depthReady: false,
    });
  }
  if (!rows.length) throw new Error(`No asset rows returned for ${chainKey}`);
  return rows;
}

async function buildAssets() {
  const depthPromise = fetchMarketDepth();
  const chainEntries = Object.keys(LIVE_CHAINS);
  const chainRows = await Promise.all(chainEntries.map((chainKey) => fetchRateRowsForChain(chainKey)));
  const depth = await depthPromise;
  const rows = chainRows.flat();

  for (const row of rows) {
    const md = depth[row.key];
    if (!md) continue;
    row.depthReady = true;
    row.price = md.price || row.price || 0;
    row.supplied = md.supplied;
    row.borrowed = md.borrowed;
    row.availableLiquidity = md.availableLiquidity;
    row.supplyCap = md.supplyCap;
    row.borrowCap = md.borrowCap;
    row.borrowDisabled = md.borrowDisabled;
    row.util = md.supplied > 0 ? cleanNumber((md.borrowed / md.supplied) * 100, 10) : 0;
    row.tvl = cleanNumber((md.supplied || 0) * (row.price || 0), 12);
    row.borrowedUsd = cleanNumber((md.borrowed || 0) * (row.price || 0), 12);
    row.dust = row.depthReady && row.tvl < 10000;
  }

  rows.sort((a, b) => {
    const chainDiff = chainEntries.indexOf(a.chain) - chainEntries.indexOf(b.chain);
    if (chainDiff) return chainDiff;
    return String(a.sym || "").localeCompare(String(b.sym || ""), "en", { sensitivity: "base" })
      || String(a.name || "").localeCompare(String(b.name || ""), "en", { sensitivity: "base" })
      || String(a.addr || "").localeCompare(String(b.addr || ""), "en");
  });

  const chainCount = new Set(rows.map((row) => row.chain)).size;
  if (chainCount !== chainEntries.length) {
    throw new Error(`Expected ${chainEntries.length} chains, received ${chainCount}`);
  }
  if (rows.length < 50) {
    throw new Error(`Expected at least 50 asset rows, received ${rows.length}`);
  }

  return {
    version: 1,
    generatedAt: new Date().toISOString(),
    source: "dolomite-official-rates+subgraphs",
    chainCount,
    rowCount: rows.length,
    chains: chainEntries,
    rows,
  };
}

try {
  const payload = await buildAssets();
  await writeFile(OUTPUT_PATH, `${JSON.stringify(payload, null, 2)}\n`);
  console.log(`Wrote ${payload.rowCount} assets across ${payload.chainCount} chains to ${path.relative(ROOT, OUTPUT_PATH)}`);
} catch (error) {
  console.error(error);
  process.exit(1);
}
