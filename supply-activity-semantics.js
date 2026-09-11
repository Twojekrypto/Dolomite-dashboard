/* Shared activity cache/filter contract. Financial replay lives in Python. */
(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    else root.SupplyActivitySemantics = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    const types = ['deposit', 'withdraw', 'borrow', 'repay', 'zap', 'trade', 'transfer', 'liquidation', 'vaporization'];
    function pack(row) {
        return [row.id || '', row.type || '', Number(row.timestamp || 0), row.txHash || '',
            row.amount ?? '0', row.usd ?? '0', row.primaryAddress || '', row.secondaryAddress || '', row.semantics || null];
    }
    function unpack(item) {
        const row = {id:item[0] || '',type:item[1] || '',timestamp:Number(item[2] || 0),txHash:item[3] || '',
            amount:item[4] ?? '0',usd:item[5] ?? '0',primaryAddress:item[6] || '',secondaryAddress:item[7] || ''};
        if (item[8] && item[8].version === 2) row.semantics = item[8];
        return row;
    }
    function actions(row) {
        const meta = row.semantics;
        if (meta?.version !== 2 || !Array.isArray(meta.actions)) return [row.type];
        if (meta.status === 'verified') return meta.actions.filter(type => types.includes(type));
        // Route evidence can be valid even when balance replay is unavailable.
        return [...new Set([row.type, ...meta.actions.filter(type => ['zap','trade'].includes(type))])];
    }
    function matches(row, selected) { return actions(row).some(type => selected.has(type)); }
    function toggle(selected, type) {
        if (type === 'all') return new Set(types);
        if (!types.includes(type)) return new Set(selected);
        if (types.every(item => selected.has(item))) return new Set([type]);
        const next = new Set(selected);
        if (next.has(type)) next.delete(type); else next.add(type);
        return next.size ? next : new Set(types);
    }
    function merge(...sets) {
        const byId = new Map();
        const priority = row => row.semantics?.version === 2 ? (row.semantics.status === 'verified' ? 2 : 1) : 0;
        for (const rows of sets) for (const row of rows || []) {
            if (!row) continue;
            const key = row.id || [row.type,row.txHash,row.primaryAddress,row.secondaryAddress,row.amount,row.timestamp].join(':');
            const previous = byId.get(key);
            if (!previous || priority(row) > priority(previous) || (priority(row) === priority(previous) && Number(row.semantics?.blockNumber || 0) > Number(previous.semantics?.blockNumber || 0))) byId.set(key,row);
        }
        const covered = new Set();
        for (const row of byId.values()) if (row.semantics?.version === 2) {
            for (const id of row.semantics.sourceIds || []) if (id !== row.id) covered.add(id);
        }
        return [...byId.values()].filter(row => !covered.has(row.id)).sort((a,b) => Number(b.timestamp)-Number(a.timestamp));
    }
    const finite = value => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)) ? Number(value) : null;
    function periodContext(points, start, end) {
        const sorted = (points || []).filter(p => Number.isFinite(p.timestamp)).slice().sort((a,b) => a.timestamp-b.timestamp);
        const opening = sorted.filter(p => p.timestamp <= start).at(-1);
        const closing = sorted.filter(p => p.timestamp <= end).at(-1);
        // Do not substitute a much earlier close or extrapolate beyond available coverage.
        if (!opening || !closing || closing.timestamp <= opening.timestamp || start-opening.timestamp > 86400 || end-closing.timestamp > 86400) return null;
        const s0=finite(opening.supply),s1=finite(closing.supply),d0=finite(opening.debt),d1=finite(closing.debt);
        return {start:opening.timestamp,end:closing.timestamp,
            supplyChange:s0 === null || s1 === null ? null : s1-s0,
            debtChange:d0 === null || d1 === null ? null : d1-d0,
            utilizationStart:s0 > 0 && d0 !== null ? d0/s0*100 : null,
            utilizationEnd:s1 > 0 && d1 !== null ? d1/s1*100 : null,
            aprStart:finite(opening.apr),aprEnd:finite(closing.apr)};
    }
    return { types, pack, unpack, actions, matches, toggle, merge, periodContext };
});
