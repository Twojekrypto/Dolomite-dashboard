(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    if (root) root.EarnCachePolicy = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';

    function isTrustedResolvedEntry(entry) {
        if (!entry || entry.resolvedTrustedForTotal !== true) return false;
        if (String(entry.resolvedVerificationStatus || '') !== 'verified') return false;
        const method = String(entry.resolvedMethod || '');
        return method === 'interest-ledger' || method === 'interest-ledger-override';
    }

    function mergeMarketMap(previousMap, candidateMap, preservedIds) {
        const merged = { ...(candidateMap || {}) };
        preservedIds.forEach(marketId => {
            if (previousMap && previousMap[marketId] !== undefined) {
                merged[marketId] = previousMap[marketId];
            }
        });
        return merged;
    }

    function buildVerificationSummary(verificationData) {
        const rows = Object.values(verificationData || {}).filter(row => row && row.counted !== false);
        const summary = { total: rows.length, verified: 0, mismatch: 0, unverified: 0 };
        rows.forEach(row => {
            const status = String((row && (row.strictStatus || row.status)) || '').toLowerCase();
            if (status === 'verified') summary.verified += 1;
            else if (status === 'mismatch') summary.mismatch += 1;
            else summary.unverified += 1;
        });
        return summary;
    }

    function marketPositionFingerprint(snapshot, marketId) {
        const target = String(marketId);
        const rows = (snapshot && Array.isArray(snapshot.cachedAssets) ? snapshot.cachedAssets : [])
            .filter(position => String(position && position.marketId) === target)
            .map(position => [
                String(position.par ?? ''),
                position.isBorrow === true ? 'borrow' : (position.isCollateral === true ? 'collateral' : 'supply'),
                String(position.accountNumber ?? ''),
            ].join(':'))
            .sort();
        return rows.length > 0 ? rows.join('|') : null;
    }

    function mergeTrustedLookupSnapshot(previous, candidate, options) {
        const maxTrustedAgeMs = Number(options && options.maxTrustedAgeMs);
        const maxAge = Number.isFinite(maxTrustedAgeMs) && maxTrustedAgeMs >= 0
            ? maxTrustedAgeMs
            : Number.POSITIVE_INFINITY;
        if (!previous) return { ...(candidate || {}), preservedTrustedMarketIds: [] };
        if (!candidate) return { ...previous, preservedTrustedMarketIds: [] };

        const previousResolved = previous.resolvedTotalYieldData || {};
        const candidateResolved = candidate.resolvedTotalYieldData || {};
        const marketIds = new Set([...Object.keys(previousResolved), ...Object.keys(candidateResolved)]);
        const preservedIds = [];
        const trustedMarketSavedAt = {};
        const candidateSavedAt = Number(candidate.savedAt || 0);
        const previousSavedAtByMarket = previous.trustedMarketSavedAt || {};

        marketIds.forEach(marketId => {
            const candidateTrusted = isTrustedResolvedEntry(candidateResolved[marketId]);
            if (candidateTrusted) {
                trustedMarketSavedAt[marketId] = candidateSavedAt;
                return;
            }
            const previousTrusted = isTrustedResolvedEntry(previousResolved[marketId]);
            const previousTrustedAt = Number(previousSavedAtByMarket[marketId] || previous.savedAt || 0);
            const trustedAge = Math.max(0, candidateSavedAt - previousTrustedAt);
            const previousFingerprint = marketPositionFingerprint(previous, marketId);
            const candidateFingerprint = marketPositionFingerprint(candidate, marketId);
            const positionUnchanged = previousFingerprint !== null && previousFingerprint === candidateFingerprint;
            if (previousTrusted && trustedAge <= maxAge && positionUnchanged) {
                preservedIds.push(String(marketId));
                trustedMarketSavedAt[marketId] = previousTrustedAt;
            }
        });
        preservedIds.sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));

        const mergedVerificationData = mergeMarketMap(
            previous.replayVerificationData,
            candidate.replayVerificationData,
            preservedIds
        );
        return {
            ...candidate,
            totalYieldData: mergeMarketMap(previous.totalYieldData, candidate.totalYieldData, preservedIds),
            resolvedTotalYieldData: mergeMarketMap(previousResolved, candidateResolved, preservedIds),
            interestYieldData: mergeMarketMap(previous.interestYieldData, candidate.interestYieldData, preservedIds),
            replayVerificationData: mergedVerificationData,
            replayVerificationSummary: buildVerificationSummary(mergedVerificationData),
            replayVerificationReady: Boolean(candidate.replayVerificationReady || previous.replayVerificationReady),
            preservedTrustedMarketIds: preservedIds,
            trustedMarketSavedAt,
        };
    }

    return {
        buildVerificationSummary,
        isTrustedResolvedEntry,
        marketPositionFingerprint,
        mergeTrustedLookupSnapshot,
    };
}));
