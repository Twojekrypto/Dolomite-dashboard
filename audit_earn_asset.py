#!/usr/bin/env python3
"""
Audit EARN asset correctness using local snapshots/netflow and optional live UI replay.

This tool is intentionally local-first:
- it reads committed public data from data/earn-snapshots and data/earn-netflow
- it can drive a local browser session against localhost for live replay verification
- generated audit inputs/results should stay local (default paths point to /tmp)

Examples:
  python3 audit_earn_asset.py static --chain arbitrum --symbol USDC \
    --output /tmp/usdc_static_audit.json \
    --unresolved-output /tmp/usdc_unresolved.json

  python3 audit_earn_asset.py live --chain arbitrum --symbol USDC \
    --localhost-url 'http://127.0.0.1:8902/index.html?cb=usdc_audit' \
    --debug-json-url 'http://127.0.0.1:9555/json' \
    --output /tmp/usdc_live_audit.json

  python3 audit_earn_asset.py summarize-live --results /tmp/usdc_live_audit.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from build_earn_verified_ledger import (
    ROOT,
    SNAPSHOT_DIR,
    _build_address_ledger,
    _collect_chain_snapshot_dates,
    _get_tolerance,
    _load_chain_snapshots,
    _load_netflow_for_chain,
    _parse_int,
    _read_json,
)


LIVE_AUDIT_JS = r"""
const fs = require('node:fs');
const { execFileSync } = require('node:child_process');

const INPUT_PATH = process.argv[2];
const OUTPUT_PATH = process.argv[3];
const BASE_URL = process.argv[4];
const DEBUG_JSON = process.argv[5];
const WORKERS = Math.max(1, Number(process.argv[6] || 2));
const MARKET_ID = String(process.argv[7] || '');
const SYMBOL = String(process.argv[8] || '');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readJson(path, fallback) {
  try {
    return JSON.parse(fs.readFileSync(path, 'utf8'));
  } catch (_) {
    return fallback;
  }
}

function curlJson(url) {
  const raw = execFileSync('curl', ['-s', url], { encoding: 'utf8' });
  return JSON.parse(raw);
}

function getDebugTargets() {
  return curlJson(DEBUG_JSON);
}

function createTarget(url) {
  const base = DEBUG_JSON.replace(/\/json$/, '/json/new?');
  try {
    const raw = execFileSync('curl', ['-s', '-X', 'PUT', base + url], { encoding: 'utf8' });
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

function getPageTargets() {
  return getDebugTargets().filter(
    (target) => target.type === 'page' && typeof target.url === 'string' && target.webSocketDebuggerUrl
  );
}

async function ensurePageTargets(url, count) {
  let pages = getPageTargets().filter((target) => target.url.startsWith(url));
  while (pages.length < count) {
    createTarget(url);
    await sleep(500);
    pages = getPageTargets().filter((target) => target.url.startsWith(url));
  }
  return pages.slice(-count);
}

function createCdpClient(wsUrl) {
  const ws = new WebSocket(wsUrl);
  let nextId = 1;
  const pending = new Map();

  const ready = new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, { once: true });
    ws.addEventListener('error', (event) => reject(event.error || new Error('WebSocket open failed')), { once: true });
  });

  ws.addEventListener('message', (event) => {
    const payload = JSON.parse(event.data);
    if (!payload.id) return;
    const entry = pending.get(payload.id);
    if (!entry) return;
    pending.delete(payload.id);
    if (payload.error) entry.reject(new Error(payload.error.message || 'CDP request failed'));
    else entry.resolve(payload.result);
  });

  ws.addEventListener('close', () => {
    for (const entry of pending.values()) entry.reject(new Error('CDP socket closed'));
    pending.clear();
  });

  return {
    ready,
    async send(method, params = {}) {
      const id = nextId++;
      const packet = { id, method, params };
      const result = new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
      ws.send(JSON.stringify(packet));
      return result;
    },
    close() {
      ws.close();
    },
  };
}

function toExpression(source) {
  return `(() => { try { ${source} } catch (error) { return { ok: false, error: String((error && error.stack) || error || 'Unknown error') }; } })()`;
}

async function evaluate(cdp, source) {
  const result = await cdp.send('Runtime.evaluate', {
    expression: toExpression(source),
    returnByValue: true,
    awaitPromise: true,
  });
  return result.result ? result.result.value : undefined;
}

async function evaluateWithTimeout(cdp, source, timeoutMs, address) {
  let timer = null;
  try {
    return await Promise.race([
      evaluate(cdp, source),
      new Promise((resolve) => {
        timer = setTimeout(() => resolve({
          ok: false,
          address,
          category: 'eval_timeout',
          error: `Evaluation timed out after ${timeoutMs}ms`,
        }), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function buildAuditSource(address) {
  return `
    const address = ${JSON.stringify(address)};
    const marketId = ${JSON.stringify(MARKET_ID)};
    const symbol = ${JSON.stringify(SYMBOL)};
    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

    const resetState = () => {
      try { sessionStorage.clear(); } catch (_) {}
      if (typeof earn_clearLookupLoadingUi === 'function') earn_clearLookupLoadingUi();
      if (!window.__auditOrigFetchVerifiedLedger && typeof earn_fetchVerifiedLedgerForAddress === 'function') {
        window.__auditOrigFetchVerifiedLedger = earn_fetchVerifiedLedgerForAddress;
      }
      if (!window.__auditOrigLoadLookupCache && typeof earn_loadLookupCache === 'function') {
        window.__auditOrigLoadLookupCache = earn_loadLookupCache;
      }
      if (!window.__auditOrigSaveLookupCache && typeof earn_saveLookupCache === 'function') {
        window.__auditOrigSaveLookupCache = earn_saveLookupCache;
      }
      earn_fetchVerifiedLedgerForAddress = async () => null;
      earn_loadLookupCache = () => null;
      earn_saveLookupCache = () => {};
      if (typeof earn_lookupResultCache === 'object' && earn_lookupResultCache) {
        Object.keys(earn_lookupResultCache).forEach((key) => delete earn_lookupResultCache[key]);
      }
      earn_cachedAssets = [];
      earn_historyData = [];
      earn_netflowData = null;
      earn_acct0NetflowData = null;
      earn_totalYieldData = null;
      earn_resolvedTotalYieldData = null;
      earn_totalYieldDays = 0;
      earn_borrowPositionData = [];
      earn_collateralPositionData = [];
      earn_lendingPositions = [];
      earn_replayAccountNumbers = [];
      earn_lastReplayError = null;
      earn_replayUsedSubgraphFallback = false;
      earn_openBorrowAccounts = new Set();
      earn_hiddenCollateralSupplyMarkets = new Set();
      earn_subgraphAccountSnapshot = null;
      earn_interestYieldData = null;
      earn_replayStateData = {};
      earn_replayBlockTag = null;
      earn_replayVerificationIncompleteMarkets = new Set();
      earn_replayActualSupplyMap = {};
      earn_replayActualBorrowMap = {};
      earn_replayActualCollateralMap = {};
      earn_replayEventData = [];
      earn_replayVerificationData = {};
      earn_replayVerificationSummary = { total: 0, verified: 0, mismatch: 0, unverified: 0 };
      earn_replayVerificationReady = false;
      earn_totalYieldStatus = 'idle';
      earn_replayStatus = 'idle';
      earn_lookupLoading = false;
      earn_lookupUsingCachedSnapshot = false;
    };

    const summarizePosition = (position) => {
      if (!position) return null;
      return {
        accountNumber: String(position.accountNumber || '0'),
        wei: String(position.wei || 0n),
        par: String(position.par || 0n),
        isCollateral: !!position.isCollateral,
        isBorrow: !!position.isBorrow,
      };
    };

    const captureState = (label) => {
      const rows = Array.from(document.querySelectorAll('#earn-table-body tr.earn-data-row'));
      const marketRow = rows.find((row) => {
        const tokenName = (row.querySelector('.earn-token-name')?.textContent || '').replace(/\\s+/g, ' ').trim();
        return tokenName === symbol;
      }) || null;
      const visiblePosition = (earn_cachedAssets || []).find((pos) =>
        String(pos.marketId || '') === marketId && !pos.isBorrow && !pos.isCollateral
      ) || null;
      const collateralPosition = (earn_collateralPositionData || []).find((pos) =>
        String(pos.marketId || '') === marketId
      ) || null;
      const borrowPosition = (earn_borrowPositionData || []).find((pos) =>
        String(pos.marketId || '') === marketId
      ) || null;
      const targetPosition = visiblePosition || collateralPosition || null;
      const calc = targetPosition
        ? earn_calculateYield(targetPosition, { requireVerifiedInterest: true })
        : null;
      const resolved = typeof earn_getResolvedTotalYieldEntry === 'function'
        ? earn_getResolvedTotalYieldEntry(marketId)
        : null;
      const verify = earn_replayVerificationData ? earn_replayVerificationData[marketId] : null;

      return {
        label,
        title: document.title,
        href: location.href,
        lookupLoading: !!earn_lookupLoading,
        loadingGateActive: !!(earn_lookupLoadingUi && earn_lookupLoadingUi.active),
        totalYieldStatus: String(earn_totalYieldStatus || ''),
        replayStatus: String(earn_replayStatus || ''),
        replayVerificationReady: !!earn_replayVerificationReady,
        replayUsedSubgraphFallback: !!earn_replayUsedSubgraphFallback,
        lastReplayError: (typeof earn_lastReplayError !== 'undefined' && earn_lastReplayError)
          ? String(earn_lastReplayError.message || earn_lastReplayError)
          : '',
        positionKind: visiblePosition
          ? 'visible_supply'
          : collateralPosition
            ? 'hidden_collateral'
            : borrowPosition
              ? 'borrow_only'
              : 'missing',
        visiblePosition: summarizePosition(visiblePosition),
        collateralPosition: summarizePosition(collateralPosition),
        borrowPosition: summarizePosition(borrowPosition),
        marketRow: marketRow ? {
          token: (marketRow.querySelector('.earn-token-name')?.textContent || '').replace(/\\s+/g, ' ').trim(),
          verifyLabel: (marketRow.querySelector('.earn-verify-badge')?.textContent || '').replace(/\\s+/g, ' ').trim(),
          sourceLabel: (marketRow.querySelector('.earn-debug-badge')?.textContent || '').replace(/\\s+/g, ' ').trim(),
          balanceCell: (marketRow.querySelectorAll('td')[2]?.textContent || '').replace(/\\s+/g, ' ').trim(),
          yieldCell: (marketRow.querySelectorAll('td')[3]?.textContent || '').replace(/\\s+/g, ' ').trim(),
        } : null,
        focusMarket: (!resolved && !calc && !verify) ? null : {
          resolvedSource: resolved ? String(resolved.resolvedSource || '') : '',
          resolvedMethod: resolved ? String(resolved.resolvedMethod || '') : '',
          resolvedVerificationStatus: resolved ? String(resolved.resolvedVerificationStatus || '') : '',
          resolvedCumulativeYield: resolved ? String(resolved.resolvedCumulativeYield || '') : '',
          calc: calc ? {
            hasData: !!calc.hasData,
            method: String(calc.method || ''),
            verificationStatus: String(calc.verificationStatus || ''),
            trustedForTotal: !!calc.trustedForTotal,
            totalYield: String(calc.totalYield || '0'),
          } : null,
          verificationData: verify ? {
            status: String(verify.status || ''),
            counted: !!verify.counted,
            canVerify: !!verify.canVerify,
            snapshotIncomplete: !!verify.snapshotIncomplete,
            expectedSupplyWei: String(verify.expectedSupplyWei || '0'),
            actualSupplyWei: String(verify.actualSupplyWei || '0'),
            expectedCollateralWei: String(verify.expectedCollateralWei || '0'),
            actualCollateralWei: String(verify.actualCollateralWei || '0'),
            expectedBorrowWei: String(verify.expectedBorrowWei || '0'),
            actualBorrowWei: String(verify.actualBorrowWei || '0'),
            supplyWeiDiff: String(verify.supplyWeiDiff || '0'),
            collateralWeiDiff: String(verify.collateralWeiDiff || '0'),
            borrowWeiDiff: String(verify.borrowWeiDiff || '0'),
            maxUsdDrift: verify.maxUsdDrift == null ? null : Number(verify.maxUsdDrift),
          } : null,
        },
      };
    };

    const isSettled = (snap) => {
      if (!snap) return false;
      const rowLoading = snap.marketRow && /Loading/i.test(String(snap.marketRow.yieldCell || ''));
      return !snap.loadingGateActive
        && !snap.lookupLoading
        && snap.totalYieldStatus !== 'loading'
        && snap.totalYieldStatus !== 'idle'
        && snap.replayStatus !== 'loading'
        && snap.replayStatus !== 'idle'
        && !rowLoading;
    };

    const classify = (snap, timedOut) => {
      if (!snap) return timedOut ? 'timeout_no_snapshot' : 'missing_snapshot';
      const verifyLabel = String((snap.marketRow && snap.marketRow.verifyLabel) || '');
      const sourceLabel = String((snap.marketRow && snap.marketRow.sourceLabel) || '');
      const balanceCell = String((snap.marketRow && snap.marketRow.balanceCell) || '');
      const balanceMatch = balanceCell.match(/≈\s*\$([0-9][0-9,]*(?:\.[0-9]+)?)(K|M|B)?/);
      const balanceMult = balanceMatch
        ? ({ '': 1, K: 1_000, M: 1_000_000, B: 1_000_000_000 }[balanceMatch[2] || ''] || 1)
        : 1;
      const balanceUsd = balanceMatch ? Number(String(balanceMatch[1]).replace(/,/g, '')) * balanceMult : null;
      const effectivePositionKind =
        snap.positionKind === 'visible_supply' &&
        verifyLabel !== 'VERIFIED' &&
        balanceUsd != null &&
        balanceUsd < 1
          ? (snap.collateralPosition ? 'hidden_collateral' : (snap.borrowPosition ? 'borrow_only' : snap.positionKind))
          : snap.positionKind;
      const focus = snap.focusMarket || {};
      const resolvedSource = String(focus.resolvedSource || '');
      const resolvedMethod = String(focus.resolvedMethod || '');
      const resolvedStatus = String(focus.resolvedVerificationStatus || '');
      const calc = focus.calc || null;
      const verify = focus.verificationData || null;
      const maxUsdDrift = verify && verify.maxUsdDrift != null ? Number(verify.maxUsdDrift) : null;
      const replayTrusted =
        resolvedSource === 'replay-ledger' &&
        (
          resolvedStatus === 'verified' ||
          resolvedStatus === 'pre_snapshot_carry' ||
          (calc && calc.verificationStatus === 'verified')
        );
      const publicTrusted =
        (resolvedSource === 'public-netflow' || resolvedSource === 'public-cycle') &&
        (resolvedStatus === 'verified' || resolvedStatus === 'pre_snapshot_carry');
      const driftTrusted =
        verify &&
        verify.canVerify &&
        verify.counted &&
        maxUsdDrift != null &&
        maxUsdDrift <= 0.1;

      if (effectivePositionKind === 'missing') return 'missing_position';
      if (effectivePositionKind === 'borrow_only') {
        if (verifyLabel === 'VERIFIED' || replayTrusted || publicTrusted || driftTrusted) {
          return 'verified_other';
        }
        return 'borrow_only';
      }
      if (effectivePositionKind === 'hidden_collateral') {
        if (replayTrusted || publicTrusted || driftTrusted) {
          return 'hidden_collateral_verified';
        }
        return timedOut ? 'timeout_hidden_collateral' : 'hidden_collateral_other';
      }
      if (verifyLabel === 'VERIFIED') {
        if (resolvedSource === 'replay-ledger') return 'replay_verified';
        if (resolvedSource === 'public-netflow' || resolvedSource === 'public-cycle') return 'public_verified';
        if (resolvedSource === 'snapshot-series') return 'snapshot_verified';
        return 'verified_other';
      }
      if (verifyLabel === 'SNAPSHOT ONLY' ||
          resolvedMethod.startsWith('snapshot') ||
          resolvedSource === 'snapshot-series' ||
          sourceLabel.includes('Snapshot')) {
        return timedOut ? 'timeout_snapshot_only' : 'snapshot_only';
      }
      if (verifyLabel === 'PENDING' || resolvedStatus === 'pending' || (calc && calc.verificationStatus === 'pending')) {
        return timedOut ? 'timeout_pending' : 'pending';
      }
      if (verify && verify.status === 'verified') return 'replay_verified';
      if (resolvedSource === 'public-netflow' || resolvedSource === 'public-cycle') return 'public_verified';
      if (timedOut) return 'timeout_other';
      return calc && calc.hasData ? 'has_data_other' : 'no_data';
    };

    return (async () => {
      if (typeof switchView !== 'function') {
        return { ok: false, address, error: 'switchView unavailable', href: location.href, title: document.title };
      }
      resetState();
      switchView('earn');
      earnChainSelect('arbitrum');
      document.getElementById('earn-address').value = address;

      const startedAt = performance.now();
      try {
        earn_lookup();
        let settled = false;
        let stableSeenAt = 0;
        let lastSnapshot = captureState('initial');
        const maxWaitMs = 90000;

        while ((performance.now() - startedAt) < maxWaitMs) {
          await sleep(500);
          lastSnapshot = captureState('poll');
          if (isSettled(lastSnapshot)) {
            if (!stableSeenAt) {
              stableSeenAt = performance.now();
            } else if ((performance.now() - stableSeenAt) >= 1200) {
              settled = true;
              break;
            }
          } else {
            stableSeenAt = 0;
          }
        }

        if (!settled) await sleep(1500);

        const finalSnapshot = captureState(settled ? 'final' : 'timeout');
        const timedOut = !settled;
        return {
          ok: true,
          address,
          timedOut,
          category: classify(finalSnapshot, timedOut),
          ...finalSnapshot,
          elapsedMs: Math.round(performance.now() - startedAt),
        };
      } catch (error) {
        return {
          ok: false,
          address,
          timedOut: false,
          category: 'script_error',
          error: String((error && error.stack) || error || 'Unknown error'),
          href: location.href,
          title: document.title,
          elapsedMs: Math.round(performance.now() - startedAt),
        };
      }
    })();
  `;
}

async function initPage(cdp, url) {
  await cdp.ready;
  await cdp.send('Page.enable');
  await cdp.send('Runtime.enable');
  await cdp.send('Page.bringToFront');
  await cdp.send('Page.navigate', { url });
  await sleep(1200);
}

function buildCounts(rows) {
  const counts = {};
  rows.forEach((row) => {
    const key = row.category || 'unknown';
    counts[key] = (counts[key] || 0) + 1;
  });
  return counts;
}

async function main() {
  const input = readJson(INPUT_PATH, {});
  const CHAIN = String((input && input.chain) || '');
  const SNAPSHOT_DATE = String((input && input.snapshotDate) || '');
  const inputRows = (input.unresolved || input.addresses || []).filter(Boolean);
  const addresses = inputRows.map((entry) => typeof entry === 'string' ? entry : entry.wallet).filter(Boolean);
  const byAddress = Object.fromEntries(
    inputRows
      .map((entry) => typeof entry === 'string' ? [entry, {}] : [entry.wallet, entry])
      .filter(([wallet]) => !!wallet)
  );
  if (!addresses.length) throw new Error(`No addresses found in ${INPUT_PATH}`);

  const existing = readJson(OUTPUT_PATH, null);
  const existingRows = Array.isArray(existing && existing.results) ? existing.results : [];
  const processed = new Map();
  for (const row of existingRows) {
    if (row && row.address) processed.set(String(row.address).toLowerCase(), row);
  }

  const results = existingRows.slice();
  const remaining = addresses.filter((address) => !processed.has(String(address).toLowerCase()));

  const startedAt = existing && existing.startedAt ? Date.parse(existing.startedAt) : Date.now();
  const targets = await ensurePageTargets(BASE_URL, WORKERS);
  const workers = [];
  for (let i = 0; i < WORKERS; i++) {
    const target = targets[i];
    const cdp = createCdpClient(target.webSocketDebuggerUrl);
    await initPage(cdp, BASE_URL);
    workers.push({ id: i + 1, cdp, runs: 0 });
  }

  let cursor = 0;

  const writeSnapshot = () => {
    fs.writeFileSync(OUTPUT_PATH, JSON.stringify({
      chain: CHAIN,
      marketId: MARKET_ID,
      symbol: SYMBOL,
      snapshotDate: SNAPSHOT_DATE,
      startedAt: new Date(startedAt).toISOString(),
      updatedAt: new Date().toISOString(),
      inputCount: addresses.length,
      completed: results.length,
      counts: buildCounts(results),
      results,
    }, null, 2));
  };

  async function runWorker(worker) {
    while (cursor < remaining.length) {
      const index = cursor++;
      const address = remaining[index];
      const sourceMeta = byAddress[address] || {};
      if (worker.runs > 0 && worker.runs % 25 === 0) {
        await initPage(worker.cdp, BASE_URL);
      }
      const row = await evaluateWithTimeout(worker.cdp, buildAuditSource(address), 110000, address);
      if (row && row.category === 'eval_timeout') {
        await initPage(worker.cdp, BASE_URL);
      }
      results.push({
        ...row,
        staticStatus: sourceMeta.status || '',
        staticMethod: sourceMeta.method || '',
        staticReason: sourceMeta.reason || '',
        staticWei: sourceMeta.wei || '',
        staticUsd: sourceMeta.balanceUsd || '',
        workerId: worker.id,
      });
      worker.runs++;
      writeSnapshot();
      console.log(JSON.stringify({
        workerId: worker.id,
        done: results.length,
        total: addresses.length,
        address,
        category: row && row.category ? row.category : 'unknown',
        elapsedMs: row && row.elapsedMs !== undefined ? row.elapsedMs : null,
      }));
    }
  }

  try {
    await Promise.all(workers.map(runWorker));
  } finally {
    workers.forEach((worker) => worker.cdp.close());
    writeSnapshot();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    ok: false,
    error: String((error && error.stack) || error || 'Unknown error'),
  }, null, 2));
  process.exitCode = 1;
});
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def latest_snapshot_date(chain: str) -> str:
    manifest = _read_json(SNAPSHOT_DIR / "manifest.json")
    dates = _collect_chain_snapshot_dates(manifest, chain)
    if not dates:
        raise SystemExit(f"No snapshot dates found for chain '{chain}'")
    return dates[-1]


def load_snapshot_payload(snapshot_date: str) -> dict:
    path = SNAPSHOT_DIR / f"{snapshot_date}.json"
    if not path.exists():
        raise SystemExit(f"Snapshot file not found: {path}")
    return _read_json(path)


def resolve_market(
    chain: str,
    symbol: str,
    market_id: Optional[str],
    snapshot_date: str,
) -> Tuple[str, dict]:
    payload = load_snapshot_payload(snapshot_date)
    chain_data = (payload.get("snapshots") or {}).get(chain) or {}
    matches: Dict[str, dict] = {}
    symbol_norm = symbol.upper()
    for addr_data in chain_data.values():
        markets = (addr_data.get("markets") or {})
        for mid_raw, market in markets.items():
            if str(market.get("symbol") or "").upper() != symbol_norm:
                continue
            matches[str(mid_raw)] = {
                "symbol": str(market.get("symbol") or symbol_norm),
                "token": str(market.get("token") or "").lower(),
                "decimals": _parse_int(market.get("decimals"), 18),
            }
    if market_id is not None:
        selected = matches.get(str(market_id))
        if not selected:
            found = ", ".join(sorted(matches)) if matches else "none"
            raise SystemExit(f"Market id '{market_id}' not found for {symbol_norm} on {chain}. Available ids: {found}")
        return str(market_id), selected
    if not matches:
        raise SystemExit(f"No markets found for symbol '{symbol_norm}' on chain '{chain}'")
    if len(matches) > 1:
        formatted = ", ".join(f"{mid}:{meta['token'] or '?'}" for mid, meta in sorted(matches.items()))
        raise SystemExit(
            f"Symbol '{symbol_norm}' is ambiguous on {chain}. Pass --market-id explicitly. Options: {formatted}"
        )
    only_mid = next(iter(matches))
    return only_mid, matches[only_mid]


def get_active_holders(chain: str, snapshot_date: str, market_id: str) -> List[dict]:
    payload = load_snapshot_payload(snapshot_date)
    chain_data = (payload.get("snapshots") or {}).get(chain) or {}
    out = []
    for address, addr_data in chain_data.items():
        market = ((addr_data.get("markets") or {}).get(str(market_id)) or {})
        if not market:
            continue
        wei = _parse_int(market.get("wei"), 0)
        if wei <= 0:
            continue
        decimals = _parse_int(market.get("decimals"), 18)
        balance = wei / (10 ** decimals) if decimals >= 0 else float(wei)
        out.append(
            {
                "wallet": str(address).lower(),
                "wei": str(wei),
                "balance": balance,
                "symbol": str(market.get("symbol") or ""),
                "token": str(market.get("token") or "").lower(),
                "decimals": decimals,
            }
        )
    return out


def explain_nonverified_reason(market: Optional[dict]) -> str:
    if not market:
        return "missing-market"
    status = str(market.get("status") or "unknown")
    if status in ("verified", "pre_snapshot_carry"):
        return status
    reasons: List[str] = []

    def baseline_reason(prefix: str, t_value: Optional[int]) -> Optional[str]:
        if t_value is None:
            return None
        first_wei = _parse_int(market.get("firstWei"), 0)
        if first_wei < t_value:
            return f"{prefix}:netflow_exceeds_first_snapshot"
        first_par = _parse_int(market.get("firstPar"), 0)
        last_par = _parse_int(market.get("lastPar"), 0)
        decimals = _parse_int(market.get("decimals"), 18)
        if abs(last_par - first_par) > _get_tolerance(decimals):
            return f"{prefix}:par_drift_above_tolerance"
        return f"{prefix}:{status}"

    if market.get("netflowYield") is not None:
        reasons.append(baseline_reason("all-time-netflow", _parse_int(market.get("lastWei"), 0) - _parse_int(market.get("netflowYield"), 0)))
    if market.get("recentCycleYield") is not None:
        reasons.append(
            baseline_reason(
                "recent-cycle",
                _parse_int(market.get("lastWei"), 0) - _parse_int(market.get("recentCycleYield"), 0),
            )
        )
    reasons = [r for r in reasons if r]
    if reasons:
        return " | ".join(reasons)
    return f"{status}:{market.get('method') or 'unknown'}"


def parse_reason_tokens(reason: str) -> List[str]:
    raw = str(reason or "").strip()
    if not raw:
        return []
    return [part.strip() for part in raw.split(" | ") if part.strip()]


def extract_root_causes(reason: str) -> List[str]:
    out: List[str] = []
    for token in parse_reason_tokens(reason):
        if ":" not in token:
            out.append(token)
            continue
        _, suffix = token.split(":", 1)
        out.append(suffix)
    return out


def build_static_report(
    chain: str,
    symbol: str,
    market_id: str,
    snapshot_date: str,
    limit: Optional[int] = None,
) -> dict:
    manifest = _read_json(SNAPSHOT_DIR / "manifest.json")
    chain_dates = _collect_chain_snapshot_dates(manifest, chain)
    snapshots = _load_chain_snapshots(chain, chain_dates)
    netflow = _load_netflow_for_chain(chain)["netflows"]

    holders = get_active_holders(chain, snapshot_date, market_id)
    holders.sort(key=lambda row: (-float(row["balance"]), row["wallet"]))
    if limit is not None:
        holders = holders[: max(0, int(limit))]

    status_counts = Counter()
    method_counts = Counter()
    reason_counts = Counter()
    root_cause_counts = Counter()
    unresolved = []

    for holder in holders:
        address = holder["wallet"]
        ledger = _build_address_ledger(address, chain, snapshot_date, snapshots, netflow)
        market = ((ledger or {}).get("markets") or {}).get(str(market_id))
        status = str((market or {}).get("status") or "missing")
        method = str((market or {}).get("method") or "missing")
        status_counts[status] += 1
        method_counts[method] += 1

        if status not in ("verified", "pre_snapshot_carry"):
            reason = explain_nonverified_reason(market)
            if reason:
                reason_counts[reason] += 1
                root_cause_counts.update(extract_root_causes(reason))
            unresolved.append(
                {
                    "wallet": address,
                    "status": status,
                    "method": method,
                    "reason": reason,
                    "wei": holder["wei"],
                    "balance": holder["balance"],
                    "balanceUsd": None,
                }
            )

    return {
        "generatedAt": utc_now_iso(),
        "mode": "static",
        "chain": chain,
        "symbol": symbol,
        "marketId": str(market_id),
        "snapshotDate": snapshot_date,
        "holderCount": len(holders),
        "statusCounts": dict(status_counts),
        "methodCounts": dict(method_counts),
        "reasonCounts": dict(reason_counts),
        "rootCauseCounts": dict(root_cause_counts),
        "resolvedCount": status_counts.get("verified", 0) + status_counts.get("pre_snapshot_carry", 0),
        "unresolvedCount": len(unresolved),
        "unresolved": unresolved,
    }


def default_static_output(chain: str, symbol: str) -> Path:
    return Path("/tmp") / f"earn_audit_{chain}_{symbol.lower()}_static.json"


def default_unresolved_output(chain: str, symbol: str) -> Path:
    return Path("/tmp") / f"earn_audit_{chain}_{symbol.lower()}_unresolved.json"


def default_live_output(chain: str, symbol: str) -> Path:
    return Path("/tmp") / f"earn_audit_{chain}_{symbol.lower()}_live.json"


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def print_static_summary(report: dict) -> None:
    status_counts = report["statusCounts"]
    print(
        json.dumps(
            {
                "chain": report["chain"],
                "symbol": report["symbol"],
                "marketId": report["marketId"],
                "snapshotDate": report["snapshotDate"],
                "holderCount": report["holderCount"],
                "resolvedCount": report["resolvedCount"],
                "unresolvedCount": report["unresolvedCount"],
                "statusCounts": status_counts,
                "rootCauseCounts": report["rootCauseCounts"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def run_static_command(args: argparse.Namespace) -> int:
    snapshot_date = args.snapshot_date or latest_snapshot_date(args.chain)
    market_id, _ = resolve_market(args.chain, args.symbol, args.market_id, snapshot_date)
    report = build_static_report(args.chain, args.symbol, market_id, snapshot_date, args.limit)

    output_path = Path(args.output) if args.output else default_static_output(args.chain, args.symbol)
    unresolved_output = (
        Path(args.unresolved_output)
        if args.unresolved_output
        else default_unresolved_output(args.chain, args.symbol)
    )

    write_json(output_path, report)
    write_json(
        unresolved_output,
        {
            "generatedAt": utc_now_iso(),
            "chain": report["chain"],
            "symbol": report["symbol"],
            "marketId": report["marketId"],
            "snapshotDate": report["snapshotDate"],
            "inputCount": report["unresolvedCount"],
            "unresolved": report["unresolved"],
        },
    )
    print_static_summary(report)
    print(f"\nStatic report: {output_path}")
    print(f"Unresolved cohort: {unresolved_output}")
    return 0


def ensure_live_input(
    args: argparse.Namespace,
) -> Tuple[Path, str, str, str]:
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            raise SystemExit(f"Live input file not found: {input_path}")
        payload = _read_json(input_path)
        chain = str(args.chain or payload.get("chain") or "")
        market_id = str(args.market_id or payload.get("marketId") or "")
        symbol = str(args.symbol or payload.get("symbol") or "")
        if not chain or not market_id or not symbol:
            raise SystemExit("Live input is missing chain/marketId/symbol; pass them explicitly if absent in the file.")
        return input_path, chain, market_id, symbol

    snapshot_date = args.snapshot_date or latest_snapshot_date(args.chain)
    market_id, _ = resolve_market(args.chain, args.symbol, args.market_id, snapshot_date)
    report = build_static_report(args.chain, args.symbol, market_id, snapshot_date, args.limit)
    input_path = default_unresolved_output(args.chain, args.symbol)
    write_json(
        input_path,
        {
            "generatedAt": utc_now_iso(),
            "chain": report["chain"],
            "symbol": report["symbol"],
            "marketId": report["marketId"],
            "snapshotDate": report["snapshotDate"],
            "inputCount": report["unresolvedCount"],
            "unresolved": report["unresolved"],
        },
    )
    return input_path, report["chain"], market_id, args.symbol


def run_live_command(args: argparse.Namespace) -> int:
    input_path, chain, market_id, symbol = ensure_live_input(args)
    output_path = Path(args.output) if args.output else default_live_output(chain, symbol)

    with tempfile.NamedTemporaryFile("w", suffix="_earn_live_audit.js", delete=False, encoding="utf-8") as tmp:
        tmp.write(LIVE_AUDIT_JS)
        js_path = Path(tmp.name)

    cmd = [
        "node",
        str(js_path),
        str(input_path),
        str(output_path),
        str(args.localhost_url),
        str(args.debug_json_url),
        str(args.workers),
        str(market_id),
        str(symbol),
    ]

    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), check=False)
        if proc.returncode != 0:
            raise SystemExit(proc.returncode)
    finally:
        try:
            js_path.unlink()
        except OSError:
            pass

    print(f"\nLive audit results: {output_path}")
    return 0


TIMEOUT_CATEGORIES = {
    "eval_timeout",
    "timeout_pending",
    "timeout_snapshot_only",
    "timeout_hidden_collateral",
    "timeout_other",
    "timeout_no_snapshot",
}

VERIFIED_CATEGORIES = {
    "replay_verified",
    "public_verified",
    "hidden_collateral_verified",
    "snapshot_verified",
    "verified_other",
}

NON_ACTIVE_CATEGORIES = {
    "missing_position",
    "borrow_only",
}

NON_BLOCKING_REAL_PATTERNS = {
    "hidden_collateral_dust",
    "tiny_snapshot_dust",
    "tiny_snapshot_balance",
}


def parse_balance_usd_from_cell(cell: Optional[str]) -> Optional[float]:
    text = str(cell or "")
    match = re.search(r"≈\s*\$([0-9][0-9,]*(?:\.[0-9]+)?)(K|M|B)?", text)
    if not match:
        return None
    value = float(match.group(1).replace(",", ""))
    suffix = match.group(2) or ""
    mult = {"": 1.0, "K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}[suffix]
    return value * mult


def parse_bigint_like(value: object) -> int:
    text = str(value or "0").strip()
    if not text:
        return 0
    try:
        return int(text)
    except Exception:
        return 0


def normalize_live_row_category(row: dict) -> str:
    category = str(row.get("category") or "unknown")
    position_kind = str(row.get("positionKind") or "")
    market_row = row.get("marketRow") or {}
    balance_usd = parse_balance_usd_from_cell(market_row.get("balanceCell"))
    if (
        position_kind == "visible_supply"
        and balance_usd is not None
        and balance_usd < 1
        and str(market_row.get("verifyLabel") or "") != "VERIFIED"
    ):
        if row.get("collateralPosition"):
            position_kind = "hidden_collateral"
        elif row.get("borrowPosition"):
            position_kind = "borrow_only"
    verify_label = str(market_row.get("verifyLabel") or "")
    source_label = str(market_row.get("sourceLabel") or "")
    focus = row.get("focusMarket") or {}
    resolved_source = str(focus.get("resolvedSource") or "")
    resolved_method = str(focus.get("resolvedMethod") or "")
    resolved_status = str(focus.get("resolvedVerificationStatus") or "")
    calc = focus.get("calc") or {}
    verify = focus.get("verificationData") or {}
    timed_out = bool(row.get("timedOut"))
    max_usd_drift = verify.get("maxUsdDrift")
    if max_usd_drift is not None:
        max_usd_drift = float(max_usd_drift)

    replay_trusted = (
        resolved_source == "replay-ledger"
        and (
            resolved_status == "verified"
            or resolved_status == "pre_snapshot_carry"
            or calc.get("verificationStatus") == "verified"
        )
    )
    public_trusted = (
        resolved_source in {"public-netflow", "public-cycle"}
        and resolved_status in {"verified", "pre_snapshot_carry"}
    )
    drift_trusted = (
        bool(verify)
        and verify.get("canVerify")
        and verify.get("counted")
        and max_usd_drift is not None
        and max_usd_drift <= 0.1
    )

    if position_kind == "missing":
        return "missing_position"
    if position_kind == "borrow_only":
        if verify_label == "VERIFIED" or replay_trusted or public_trusted or drift_trusted:
            return "verified_other"
        return "borrow_only"
    if position_kind == "hidden_collateral":
        if replay_trusted or public_trusted or drift_trusted:
            return "hidden_collateral_verified"
        return "timeout_hidden_collateral" if timed_out else "hidden_collateral_other"
    if verify_label == "VERIFIED":
        if resolved_source == "replay-ledger":
            return "replay_verified"
        if resolved_source in {"public-netflow", "public-cycle"}:
            return "public_verified"
        if resolved_source == "snapshot-series":
            return "snapshot_verified"
        return "verified_other"
    if (
        verify_label == "SNAPSHOT ONLY"
        or resolved_method.startswith("snapshot")
        or resolved_source == "snapshot-series"
        or "Snapshot" in source_label
    ):
        return "timeout_snapshot_only" if timed_out else "snapshot_only"
    if (
        verify_label == "PENDING"
        or resolved_status == "pending"
        or calc.get("verificationStatus") == "pending"
    ):
        return "timeout_pending" if timed_out else "pending"
    if verify.get("status") == "verified":
        return "replay_verified"
    if resolved_source in {"public-netflow", "public-cycle"}:
        return "public_verified"
    if timed_out:
        return "timeout_other"
    if calc.get("hasData"):
        return "has_data_other"
    return category or "no_data"


def parse_live_row_pattern(row: dict) -> Tuple[str, str]:
    category = normalize_live_row_category(row)
    if category in VERIFIED_CATEGORIES:
        return ("verified", "verified")
    if category == "missing_position":
        return ("missing_live_position", "info")
    if category == "borrow_only":
        return ("non_active_borrow_only", "info")

    focus = row.get("focusMarket") or {}
    verify = focus.get("verificationData") or {}
    market_row = row.get("marketRow") or {}
    reason = str(row.get("staticReason") or "")
    root_causes = extract_root_causes(reason)
    max_usd_drift = verify.get("maxUsdDrift")
    balance_usd = parse_balance_usd_from_cell(market_row.get("balanceCell"))
    loading_row = "loading" in str(market_row.get("yieldCell") or "").lower()

    if category in TIMEOUT_CATEGORIES:
        if "par_drift_above_tolerance" in root_causes:
            return ("loading_timeout_par_drift", "medium")
        if "netflow_exceeds_first_snapshot" in root_causes:
            severity = "medium"
            if max_usd_drift is not None and float(max_usd_drift) < 1:
                severity = "low"
            return ("loading_timeout_netflow_baseline", severity)
        return ("loading_timeout_other", "medium" if loading_row else "low")

    if category == "snapshot_only":
        drift = float(max_usd_drift) if max_usd_drift is not None else None
        if drift is not None and drift >= 1:
            return ("material_snapshot_mismatch", "high")
        if drift is not None and drift <= 0.01:
            return ("tiny_snapshot_dust", "low")
        if balance_usd is not None and balance_usd < 0.5:
            return ("tiny_snapshot_balance", "low")
        return ("snapshot_only_other", "medium")

    if category == "hidden_collateral_other":
        drift = float(max_usd_drift) if max_usd_drift is not None else None
        actual_supply_wei = parse_bigint_like(verify.get("actualSupplyWei"))
        expected_supply_wei = parse_bigint_like(verify.get("expectedSupplyWei"))
        actual_collateral_wei = parse_bigint_like(verify.get("actualCollateralWei"))
        expected_collateral_wei = parse_bigint_like(verify.get("expectedCollateralWei"))
        actual_borrow_wei = parse_bigint_like(verify.get("actualBorrowWei"))
        expected_borrow_wei = parse_bigint_like(verify.get("expectedBorrowWei"))
        if actual_supply_wei > 0 and not row.get("visiblePosition"):
            has_hidden_overlap = (
                actual_collateral_wei > 0
                or expected_collateral_wei > 0
                or actual_borrow_wei > 0
                or expected_borrow_wei > 0
                or expected_supply_wei == 0
            )
            if has_hidden_overlap:
                if drift is not None and drift >= 100:
                    return ("mixed_hidden_visible_overlap", "high")
                if drift is not None and drift >= 1:
                    return ("mixed_hidden_visible_overlap", "medium")
                return ("mixed_hidden_visible_overlap", "low")
        if drift is not None and drift >= 1:
            return ("material_hidden_collateral_gap", "medium")
        return ("hidden_collateral_dust", "low")

    return (f"unclassified_{category}", "medium")


def summarize_live_results(payload: dict) -> dict:
    results = payload.get("results") or []
    counts = Counter(payload.get("counts") or {})
    if results:
        counts = Counter(normalize_live_row_category(row) for row in results)

    real_nonverified_categories = {
        "snapshot_only",
        "hidden_collateral_other",
        "has_data_other",
    }

    verified = sum(counts.get(cat, 0) for cat in VERIFIED_CATEGORIES)
    real_nonverified = sum(counts.get(cat, 0) for cat in real_nonverified_categories)
    timeouts = sum(counts.get(cat, 0) for cat in TIMEOUT_CATEGORIES)
    non_active = sum(counts.get(cat, 0) for cat in NON_ACTIVE_CATEGORIES)
    missing = counts.get("missing_position", 0)
    completed = int(payload.get("completed") or sum(counts.values()))
    input_count = int(payload.get("inputCount") or completed)
    active_checked = max(0, completed - non_active)
    pattern_counts = Counter()
    severity_counts = Counter()
    root_cause_counts = Counter()
    blocking_real_nonverified = 0
    informational_real_nonverified = 0
    for row in results:
        pattern, severity = parse_live_row_pattern(row)
        pattern_counts[pattern] += 1
        severity_counts[severity] += 1
        normalized_category = normalize_live_row_category(row)
        if normalized_category in real_nonverified_categories:
            if pattern in NON_BLOCKING_REAL_PATTERNS:
                informational_real_nonverified += 1
            else:
                blocking_real_nonverified += 1
        root_causes = extract_root_causes(str(row.get("staticReason") or ""))
        if root_causes:
            root_cause_counts.update(root_causes)

    top_real_patterns = []
    for row in results:
        pattern, severity = parse_live_row_pattern(row)
        if pattern == "verified" or pattern == "missing_live_position":
            continue
        if pattern.startswith("loading_timeout"):
            continue
        focus = row.get("focusMarket") or {}
        verify = focus.get("verificationData") or {}
        top_real_patterns.append(
            {
                "pattern": pattern,
                "severity": severity,
                "category": row.get("category"),
                "rootCauses": extract_root_causes(str(row.get("staticReason") or "")),
                "maxUsdDrift": verify.get("maxUsdDrift"),
                "balanceCell": (row.get("marketRow") or {}).get("balanceCell"),
                "yieldCell": (row.get("marketRow") or {}).get("yieldCell"),
            }
        )

    return {
        "generatedAt": utc_now_iso(),
        "chain": payload.get("chain") or "",
        "marketId": payload.get("marketId"),
        "symbol": payload.get("symbol"),
        "snapshotDate": payload.get("snapshotDate") or "",
        "inputCount": input_count,
        "completed": completed,
        "verifiedChecked": verified,
        "activeChecked": active_checked,
        "verifiedCheckedRatio": (verified / active_checked) if active_checked else None,
        "realNonVerifiedChecked": real_nonverified,
        "blockingRealNonVerifiedChecked": blocking_real_nonverified,
        "informationalRealNonVerifiedChecked": informational_real_nonverified,
        "timeouts": timeouts,
        "nonActiveChecked": non_active,
        "missingPosition": missing,
        "counts": dict(counts),
        "patternCounts": dict(pattern_counts),
        "severityCounts": dict(severity_counts),
        "rootCauseCounts": dict(root_cause_counts),
        "topRealPatterns": top_real_patterns[:10],
    }


def run_summarize_live_command(args: argparse.Namespace) -> int:
    payload = _read_json(Path(args.results))
    summary = summarize_live_results(payload)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def merge_live_payloads(paths: Sequence[Path]) -> dict:
    merged_rows: Dict[str, dict] = {}
    started_candidates: List[str] = []
    chain = ""
    symbol = ""
    market_id = ""
    snapshot_date = ""

    for path in paths:
        payload = _read_json(path)
        chain = str(payload.get("chain") or chain or "")
        symbol = str(payload.get("symbol") or symbol or "")
        market_id = str(payload.get("marketId") or market_id or "")
        snapshot_date = str(payload.get("snapshotDate") or snapshot_date or "")
        started = str(payload.get("startedAt") or "").strip()
        if started:
            started_candidates.append(started)
        for row in payload.get("results") or []:
            address = str(row.get("address") or "").lower()
            if not address:
                continue
            merged_rows[address] = row

    results = list(merged_rows.values())
    results.sort(key=lambda row: str(row.get("address") or "").lower())
    counts = Counter(normalize_live_row_category(row) for row in results)

    return {
        "chain": chain,
        "marketId": market_id,
        "symbol": symbol,
        "snapshotDate": snapshot_date,
        "startedAt": min(started_candidates) if started_candidates else utc_now_iso(),
        "updatedAt": utc_now_iso(),
        "inputCount": len(results),
        "completed": len(results),
        "counts": dict(counts),
        "results": results,
        "mergedFrom": [str(path) for path in paths],
    }


def row_to_retry_entry(row: dict) -> dict:
    pattern, severity = parse_live_row_pattern(row)
    return {
        "wallet": str(row.get("address") or "").lower(),
        "status": str(row.get("staticStatus") or ""),
        "method": str(row.get("staticMethod") or ""),
        "reason": str(row.get("staticReason") or ""),
        "wei": str(row.get("staticWei") or ""),
        "balance": None,
        "balanceUsd": row.get("staticUsd"),
        "liveCategory": normalize_live_row_category(row),
        "pattern": pattern,
        "severity": severity,
    }


def select_live_rows(
    payload: dict,
    mode: str,
    categories: Optional[Sequence[str]] = None,
) -> List[dict]:
    selected: List[dict] = []
    category_set = {str(cat).strip() for cat in (categories or []) if str(cat).strip()}

    for row in payload.get("results") or []:
        category = normalize_live_row_category(row)
        pattern, _severity = parse_live_row_pattern(row)
        is_real_nonverified = category in {"snapshot_only", "hidden_collateral_other", "has_data_other"}
        is_blocking = is_real_nonverified and pattern not in NON_BLOCKING_REAL_PATTERNS
        is_timeout = category in TIMEOUT_CATEGORIES
        is_missing = category == "missing_position"

        keep = False
        if mode == "timeouts":
            keep = is_timeout
        elif mode == "blocking":
            keep = is_blocking
        elif mode == "real":
            keep = is_real_nonverified
        elif mode == "informational":
            keep = is_real_nonverified and pattern in NON_BLOCKING_REAL_PATTERNS
        elif mode == "missing":
            keep = is_missing
        elif mode == "categories":
            keep = category in category_set

        if keep:
            selected.append(row)
    return selected


def build_extracted_live_payload(payload: dict, rows: Sequence[dict]) -> dict:
    return {
        "generatedAt": utc_now_iso(),
        "chain": str(payload.get("chain") or ""),
        "symbol": str(payload.get("symbol") or ""),
        "marketId": str(payload.get("marketId") or ""),
        "snapshotDate": str(payload.get("snapshotDate") or ""),
        "inputCount": len(rows),
        "unresolved": [row_to_retry_entry(row) for row in rows],
    }


def compact_position_summary(position: Optional[dict]) -> Optional[dict]:
    if not position:
        return None
    return {
        "accountNumber": str(position.get("accountNumber") or ""),
        "wei": str(position.get("wei") or ""),
        "par": str(position.get("par") or ""),
        "isCollateral": bool(position.get("isCollateral")),
        "isBorrow": bool(position.get("isBorrow")),
    }


def build_forensic_live_report(payload: dict) -> dict:
    summary = summarize_live_results(payload)
    blocking_rows = []
    informational_rows = []

    for row in payload.get("results") or []:
        category = normalize_live_row_category(row)
        if category not in {"snapshot_only", "hidden_collateral_other", "has_data_other"}:
            continue
        pattern, severity = parse_live_row_pattern(row)
        focus = row.get("focusMarket") or {}
        verify = focus.get("verificationData") or {}
        entry = {
            "address": str(row.get("address") or "").lower(),
            "normalizedCategory": category,
            "pattern": pattern,
            "severity": severity,
            "staticStatus": str(row.get("staticStatus") or ""),
            "staticMethod": str(row.get("staticMethod") or ""),
            "staticReason": str(row.get("staticReason") or ""),
            "rootCauses": extract_root_causes(str(row.get("staticReason") or "")),
            "positionKind": str(row.get("positionKind") or ""),
            "marketRow": row.get("marketRow") or None,
            "visiblePosition": compact_position_summary(row.get("visiblePosition")),
            "collateralPosition": compact_position_summary(row.get("collateralPosition")),
            "borrowPosition": compact_position_summary(row.get("borrowPosition")),
            "resolvedSource": str(focus.get("resolvedSource") or ""),
            "resolvedMethod": str(focus.get("resolvedMethod") or ""),
            "resolvedVerificationStatus": str(focus.get("resolvedVerificationStatus") or ""),
            "resolvedCumulativeYield": str(focus.get("resolvedCumulativeYield") or ""),
            "calc": focus.get("calc") or None,
            "verificationData": verify or None,
            "elapsedMs": row.get("elapsedMs"),
        }
        if pattern in NON_BLOCKING_REAL_PATTERNS:
            informational_rows.append(entry)
        else:
            blocking_rows.append(entry)

    blocking_rows.sort(
        key=lambda entry: (
            -float(((entry.get("verificationData") or {}).get("maxUsdDrift") or 0) or 0),
            entry["normalizedCategory"],
            entry["address"],
        )
    )
    informational_rows.sort(
        key=lambda entry: (
            -float(((entry.get("verificationData") or {}).get("maxUsdDrift") or 0) or 0),
            entry["normalizedCategory"],
            entry["address"],
        )
    )

    return {
        "generatedAt": utc_now_iso(),
        "summary": summary,
        "blockingRows": blocking_rows,
        "informationalRows": informational_rows,
    }


def run_merge_live_command(args: argparse.Namespace) -> int:
    paths = [Path(path) for path in args.results]
    for path in paths:
        if not path.exists():
            raise SystemExit(f"Live results file not found: {path}")
    payload = merge_live_payloads(paths)
    output_path = Path(args.output)
    write_json(output_path, payload)
    print(json.dumps(summarize_live_results(payload), ensure_ascii=False, indent=2))
    print(f"\nMerged live results: {output_path}")
    return 0


def run_extract_live_command(args: argparse.Namespace) -> int:
    payload = _read_json(Path(args.results))
    rows = select_live_rows(payload, args.mode, args.category or [])
    extracted = build_extracted_live_payload(payload, rows)
    if args.chain:
        extracted["chain"] = args.chain
    output_path = Path(args.output)
    write_json(output_path, extracted)
    print(
        json.dumps(
            {
                "mode": args.mode,
                "selected": len(rows),
                "output": str(output_path),
                "chain": extracted.get("chain") or "",
                "symbol": extracted.get("symbol") or "",
                "marketId": extracted.get("marketId") or "",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def run_forensic_live_command(args: argparse.Namespace) -> int:
    payload = _read_json(Path(args.results))
    report = build_forensic_live_report(payload)
    if args.output:
        write_json(Path(args.output), report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit EARN asset cohorts using static and live checks")
    subparsers = parser.add_subparsers(dest="command", required=True)

    static_parser = subparsers.add_parser("static", help="Build a static cohort audit from snapshots/netflow")
    static_parser.add_argument("--chain", required=True, help="Chain id, e.g. arbitrum")
    static_parser.add_argument("--symbol", required=True, help="Asset symbol, e.g. USDC")
    static_parser.add_argument("--market-id", help="Optional explicit market id")
    static_parser.add_argument("--snapshot-date", help="Optional snapshot date (YYYY-MM-DD)")
    static_parser.add_argument("--limit", type=int, help="Optional holder limit for a smaller sample")
    static_parser.add_argument("--output", help="Where to write the full static report (default: /tmp)")
    static_parser.add_argument("--unresolved-output", help="Where to write the unresolved input cohort (default: /tmp)")
    static_parser.set_defaults(func=run_static_command)

    live_parser = subparsers.add_parser("live", help="Run a live replay audit against localhost/Chrome")
    live_parser.add_argument("--chain", help="Chain id, e.g. arbitrum")
    live_parser.add_argument("--symbol", help="Asset symbol, e.g. USDC")
    live_parser.add_argument("--market-id", help="Optional explicit market id")
    live_parser.add_argument("--snapshot-date", help="Optional snapshot date (YYYY-MM-DD)")
    live_parser.add_argument("--limit", type=int, help="Optional holder limit before building unresolved cohort")
    live_parser.add_argument("--input", help="Optional prebuilt unresolved cohort JSON")
    live_parser.add_argument("--localhost-url", default="http://127.0.0.1:8902/index.html?cb=earn_audit", help="Local dashboard URL")
    live_parser.add_argument("--debug-json-url", default="http://127.0.0.1:9555/json", help="Chrome remote debugger /json endpoint")
    live_parser.add_argument("--workers", type=int, default=2, help="Parallel browser workers")
    live_parser.add_argument("--output", help="Where to write live audit results (default: /tmp)")
    live_parser.set_defaults(func=run_live_command)

    summarize_parser = subparsers.add_parser("summarize-live", help="Summarize a live audit results JSON")
    summarize_parser.add_argument("--results", required=True, help="Path to live audit results JSON")
    summarize_parser.set_defaults(func=run_summarize_live_command)

    merge_parser = subparsers.add_parser("merge-live", help="Merge multiple live audit results with latest row per wallet")
    merge_parser.add_argument("--results", required=True, nargs="+", help="Live results JSON files in oldest->newest order")
    merge_parser.add_argument("--output", required=True, help="Where to write the merged live results JSON")
    merge_parser.set_defaults(func=run_merge_live_command)

    extract_parser = subparsers.add_parser("extract-live", help="Extract a focused rerun cohort from live results")
    extract_parser.add_argument("--results", required=True, help="Path to live audit results JSON")
    extract_parser.add_argument(
        "--mode",
        required=True,
        choices=["timeouts", "blocking", "real", "informational", "missing", "categories"],
        help="Which cohort to extract",
    )
    extract_parser.add_argument("--category", action="append", help="Category name(s) when --mode=categories")
    extract_parser.add_argument("--chain", help="Optional explicit chain to stamp into the extracted input")
    extract_parser.add_argument("--output", required=True, help="Where to write the extracted unresolved cohort JSON")
    extract_parser.set_defaults(func=run_extract_live_command)

    forensic_parser = subparsers.add_parser("forensic-live", help="Build a detailed blocker report from live results")
    forensic_parser.add_argument("--results", required=True, help="Path to live audit results JSON")
    forensic_parser.add_argument("--output", help="Optional forensic report output path")
    forensic_parser.set_defaults(func=run_forensic_live_command)

    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
