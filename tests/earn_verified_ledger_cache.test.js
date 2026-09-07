const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const test = require('node:test');

const A = '0xaa' + '1'.repeat(38);
const B = '0xaa' + '2'.repeat(38);
const C = '0xbb' + '3'.repeat(38);
const D = '0xaa' + '4'.repeat(38);
const DATE = '2026-09-07';
const response = (body, status = 200) => ({ ok: status === 200, status, json: async () => body });
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const ledger = (value, date = DATE) => ({ snapshotDate: date, markets: { '1': { strictStatus: 'verified', value } } });
const shard = (entries) => ({ version: 1, chain: 'ethereum', prefix: 'aa', snapshotDate: DATE, ledgers: entries });
const manifest = (generation, aaCount = 1) => ({
    version: 1, prefixLength: 2, generatedAt: `2026-09-07T0${generation}:00:00Z`,
    chains: { ethereum: { generatedAt: 'unchanged-source-ledger-time', snapshotDate: DATE,
        prefixLength: 2, addressCount: aaCount + 1, shardCount: 2, shards: { aa: { path: 'ethereum/aa.json', addressCount: aaCount }, bb: { path: 'ethereum/bb.json', addressCount: 1 } } } },
});

function runtime(handler) {
    const source = fs.readFileSync('dashboard-core.js', 'utf8');
    const declarations = source.slice(source.indexOf('let earn_verifiedLedgerCache ='), source.indexOf('let earn_publishedResolvedInterestLedger ='));
    const functions = source.slice(source.indexOf('async function earn_fetchVerifiedLedgerForAddress('), source.indexOf('function earn_canUseVerifiedLedgerMarketEntry('));
    let now = 1000;
    let latest = DATE;
    const calls = [];
    const context = vm.createContext({
        console: { warn() {}, info() {} },
        Date: { now: () => now, parse: Date.parse },
        VERIFIED_LEDGER_SHARD_BASE: '/shards', VERIFIED_LEDGER_BASE: '/individual',
        earn_getLatestSnapshotDateForChain: () => latest,
        fetch: async (url, options) => { calls.push({ url, options }); return handler(url, options); },
    });
    vm.runInContext(`${declarations}\n${functions}\nthis.lookup = earn_fetchVerifiedLedgerForAddress;`, context);
    return { lookup: address => context.lookup('ethereum', address), calls,
        expire() { now += 61000; }, latest(date) { latest = date; }, context };
}

test('transient shard failure plus individual 404 does not poison retries', async () => {
    let attempts = 0;
    const r = runtime(url => {
        if (url.includes('manifest.json')) return response(manifest(1));
        if (url.startsWith('/individual')) return response(null, 404);
        return ++attempts === 1 ? response(null, 503) : response(shard({ [A]: ledger('recovered') }));
    });
    assert.equal(await r.lookup(A), null);
    assert.equal((await r.lookup(A)).markets['1'].value, 'recovered');
    assert.equal(attempts, 2);
    assert.ok(r.calls.every(call => call.options.cache === 'no-cache'));
});

test('top-level generation change invalidates address, negative and shard caches', async () => {
    let generation = 1;
    const r = runtime(url => {
        if (url.includes('manifest.json')) return response(manifest(generation, 2));
        if (url.startsWith('/individual')) return response(null, 404);
        return response(shard(generation === 1 ? { [A]: ledger('old'), [D]: ledger('other') } : { [A]: ledger('new'), [B]: ledger('newly-published') }));
    });
    assert.equal((await r.lookup(A)).markets['1'].value, 'old');
    assert.equal(await r.lookup(B), null);
    const before = r.calls.length;
    assert.equal(await r.lookup(B), null);
    assert.equal(r.calls.length, before, 'authoritative negative is shared for this version');
    generation = 2; r.expire();
    assert.equal((await r.lookup(A)).markets['1'].value, 'new');
    assert.equal((await r.lookup(B)).markets['1'].value, 'newly-published');
    assert.equal(r.calls.filter(c => c.url.includes('manifest.json')).length, 2);
});

test('concurrent first wallets share manifest and shard; same wallet shares inflight', async () => {
    const gate = deferred();
    const r = runtime(async url => {
        if (url.includes('manifest.json')) { await gate.promise; return response(manifest(1, 2)); }
        return response(shard({ [A]: ledger('a'), [B]: ledger('b') }));
    });
    const requests = [r.lookup(A), r.lookup(A), r.lookup(B)];
    await new Promise(setImmediate);
    assert.equal(r.calls.length, 1);
    assert.ok(r.calls[0].url.includes('manifest.json'));
    gate.resolve();
    const values = await Promise.all(requests);
    assert.equal(values[0], values[1]);
    assert.equal(values[2].markets['1'].value, 'b');
    assert.equal(r.calls.filter(c => c.url.includes('/ethereum/aa.json')).length, 1);
});

test('latest snapshot freshness is rechecked on hits and after a pending response', async () => {
    const gate = deferred();
    const r = runtime(async url => {
        if (url.includes('manifest.json')) return response(manifest(1));
        if (url.includes('/bb.json')) { await gate.promise; return response({ ...shard({ [C]: ledger('late') }), prefix: 'bb' }); }
        return response(shard({ [A]: ledger('cached') }));
    });
    assert.ok(await r.lookup(A));
    const pending = r.lookup(C);
    await new Promise(setImmediate);
    r.latest('2026-09-08');
    assert.equal(await r.lookup(A), null);
    gate.resolve();
    assert.equal(await pending, null);
});

test('late obsolete shard cannot poison or remove current inflight requests', async () => {
    let generation = 1;
    const oldGate = deferred(); const newGate = deferred();
    const r = runtime(async url => {
        if (url.includes('manifest.json')) return response(manifest(generation));
        const captured = generation;
        await (captured === 1 ? oldGate.promise : newGate.promise);
        return response(shard({ [A]: ledger(captured === 1 ? 'obsolete' : 'current') }));
    });
    const old = r.lookup(A); await new Promise(setImmediate);
    generation = 2; r.expire();
    const current = r.lookup(A); await new Promise(setImmediate);
    oldGate.resolve();
    assert.equal(await old, null, 'obsolete response must not be returned');
    const duplicate = r.lookup(A); await new Promise(setImmediate);
    assert.equal(r.calls.filter(c => c.url.includes('/ethereum/aa.json')).length, 2);
    newGate.resolve();
    assert.equal((await current).markets['1'].value, 'current');
    assert.equal((await duplicate).markets['1'].value, 'current');
    assert.equal((await r.lookup(A)).markets['1'].value, 'current');
});

test('invalid manifest and malformed shard are never authoritative negative evidence', async () => {
    for (const badManifest of [false, true]) {
        let recovered = false;
        const r = runtime(url => {
            if (url.includes('manifest.json')) return response(badManifest && !recovered ? {} : manifest(1));
            if (url.startsWith('/individual')) return response(null, 404);
            return response(recovered ? shard({ [A]: ledger('recovered') }) : { ledgers: [] });
        });
        assert.equal(await r.lookup(A), null);
        recovered = true;
        assert.equal((await r.lookup(A)).markets['1'].value, 'recovered');
    }
});

test('a null or malformed compact entry is evicted so a corrected shard can retry', async () => {
    for (const entry of [null, [DATE, { '1': {} }], { snapshotDate: DATE, markets: [] }]) {
        let recovered = false;
        const r = runtime(url => {
            if (url.includes('manifest.json')) return response(manifest(1));
            if (url.startsWith('/individual')) return response(null, 404);
            return response(shard({ [A]: recovered ? ledger('corrected') : entry }));
        });
        assert.equal(await r.lookup(A), null);
        recovered = true;
        assert.equal((await r.lookup(A))?.markets['1'].value, 'corrected');
    }
});

test('incomplete chain manifest metadata cannot cache a false missing address', async () => {
    let recovered = false;
    const r = runtime(url => {
        if (url.includes('manifest.json')) {
            const payload = manifest(1);
            if (!recovered) payload.chains.ethereum.shards = {};
            return response(payload);
        }
        if (url.startsWith('/individual')) return response(null, 404);
        return response(shard({ [A]: ledger('recovered') }));
    });
    assert.equal(await r.lookup(A), null);
    recovered = true;
    assert.equal((await r.lookup(A))?.markets['1'].value, 'recovered');
});

test('shard entry count must match the manifest before caching a value or absence', async () => {
    for (const [declaredCount, incomplete] of [
        [1, {}],
        [2, { [A]: ledger('partial') }],
        [1, { [A]: ledger('unexpected-extra'), [B]: ledger('extra') }],
    ]) {
        let recovered = false;
        const complete = declaredCount === 1
            ? { [A]: ledger('complete') }
            : { [A]: ledger('complete'), [B]: ledger('other') };
        const r = runtime(url => {
            if (url.includes('manifest.json')) return response(manifest(1, declaredCount));
            if (url.startsWith('/individual')) return response(null, 404);
            return response(shard(recovered ? complete : incomplete));
        });
        assert.equal(await r.lookup(A), null, 'count-inconsistent shard cannot supply trusted cache data');
        recovered = true;
        assert.equal((await r.lookup(A))?.markets['1'].value, 'complete');
        assert.equal(r.calls.filter(c => c.url.includes('/ethereum/aa.json')).length, 2);
        assert.equal(r.calls.filter(c => c.url.includes('manifest.json')).length, 1, 'same publication is retried');
        assert.equal(r.calls.filter(c => c.url.startsWith('/individual')).length, 1);
    }
});

test('unversioned compatibility response cannot outlive a newly established generation', async () => {
    let recovered = false;
    const gate = deferred();
    const r = runtime(async url => {
        if (url.includes('manifest.json')) return response(recovered ? manifest(1) : {}, recovered ? 200 : 503);
        if (url.startsWith('/individual')) { await gate.promise; return response(ledger('unversioned')); }
        return response(shard({ [B]: ledger('current') }));
    });
    const pending = r.lookup(A); await new Promise(setImmediate);
    recovered = true;
    assert.ok(await r.lookup(B));
    gate.resolve();
    assert.equal(await pending, null);
});

test('compact shard preserves market values and resolved proof; obsolete caches are discarded', async () => {
    let generation = 1;
    const proof = { strictStatus: 'verified', strictMethod: 'interest-ledger', snapshotDate: DATE };
    const r = runtime(url => {
        if (url.includes('manifest.json')) return response(manifest(generation));
        return response({ ...shard({ [generation === 1 ? A : B]: [DATE, { '1': ['verified', '12345678901234567890'] }, proof] }),
            version: 2,
            schema: { ledger: ['snapshotDate', 'markets'], market: ['strictStatus', 'lastWei'] } });
    });
    const first = await r.lookup(A);
    assert.equal(first.markets['1'].lastWei, '12345678901234567890');
    assert.equal(first.resolvedInterestLedger.strictStatus, 'verified');
    assert.equal(r.calls.filter(c => c.url.startsWith('/individual')).length, 0);
    generation = 2; r.expire();
    assert.ok(await r.lookup(B));
    assert.equal(vm.runInContext('Object.keys(earn_verifiedLedgerCache).length', r.context), 1);
    assert.equal(vm.runInContext('Object.keys(earn_verifiedLedgerShardRequestCache).length', r.context), 1);
});
