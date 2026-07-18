import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EarnRuntimePoliciesTest(unittest.TestCase):
    def _run_node(self, script):
        completed = subprocess.run(
            ["node", "-e", textwrap.dedent(script)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_rpc_policy_rotates_endpoint_failures_without_retrying_bad_requests(self):
        self._run_node("""
            const assert = require('assert');
            const policyModule = require('./earn/earn-rpc-policy.js');
            let now = 1000;
            const policy = policyModule.create(
              ['https://rpc-a.example', 'https://rpc-b.example', 'https://rpc-c.example'],
              { now: () => now }
            );
            assert.equal(policy.next(), 'https://rpc-a.example');

            const forbidden = new Error('RPC HTTP 403');
            forbidden.httpStatus = 403;
            assert.equal(policy.recordFailure('https://rpc-a.example', forbidden).kind, 'endpoint_fatal');
            assert.equal(policy.next(), 'https://rpc-b.example');

            const metadataMissing = new Error('account metadata is not found, 1015000062');
            assert.equal(policy.recordFailure('https://rpc-b.example', metadataMissing).kind, 'request_fatal');
            assert.equal(policy.shouldRetry(metadataMissing), false);

            const internal = new Error('Temporary internal error. Please retry');
            internal.rpcCode = -32603;
            assert.equal(policy.classify(internal).kind, 'retryable');

            const archiveAuth = new Error('Archive requests require a personal token');
            archiveAuth.rpcCode = -32602;
            assert.equal(policy.recordFailure('https://rpc-b.example', archiveAuth).kind, 'endpoint_fatal');

            policy.reset();
            assert.equal(policy.next(), 'https://rpc-a.example');
        """)

    def test_cache_policy_preserves_only_recent_trusted_counted_markets(self):
        self._run_node("""
            const assert = require('assert');
            const cachePolicy = require('./earn/earn-cache-policy.js');
            function snapshot(status, value, savedAt, balance, par = '100') {
              const trusted = status === 'verified';
              return {
                version: 15,
                savedAt,
                cachedAssets: [{
                  marketId: '0', wei: balance, par,
                  isBorrow: false, isCollateral: false,
                }],
                totalYieldData: { '0': { cumulativeYield: value } },
                resolvedTotalYieldData: {
                  '0': {
                    resolvedVerificationStatus: status,
                    resolvedTrustedForTotal: trusted,
                    resolvedMethod: trusted ? 'interest-ledger' : 'interest-ledger-pending',
                    resolvedCumulativeYield: value,
                  },
                },
                interestYieldData: { '0': { earnYield: value } },
                replayVerificationData: { '0': { status, counted: true } },
              };
            }
            const previous = snapshot('verified', '1600', 100, '10');
            const transient = snapshot('pending', '0', 101, '12');
            const preserved = cachePolicy.mergeTrustedLookupSnapshot(previous, transient);
            assert.equal(preserved.cachedAssets[0].wei, '12');
            assert.equal(preserved.resolvedTotalYieldData['0'].resolvedCumulativeYield, '1600');
            assert.deepEqual(preserved.preservedTrustedMarketIds, ['0']);

            const changedPrincipal = cachePolicy.mergeTrustedLookupSnapshot(
              previous,
              snapshot('pending', '0', 102, '12', '90')
            );
            assert.deepEqual(changedPrincipal.preservedTrustedMarketIds, []);
            assert.equal(changedPrincipal.resolvedTotalYieldData['0'].resolvedVerificationStatus, 'pending');

            const expired = cachePolicy.mergeTrustedLookupSnapshot(
              previous,
              snapshot('pending', '0', 700101, '14'),
              { maxTrustedAgeMs: 600000 }
            );
            assert.equal(expired.resolvedTotalYieldData['0'].resolvedVerificationStatus, 'pending');

            assert.deepEqual(
              cachePolicy.buildVerificationSummary({
                '0': { status: 'verified', counted: true },
                '1': { status: 'unverified', counted: false },
              }),
              { total: 1, verified: 1, mismatch: 0, unverified: 0 }
            );
        """)


if __name__ == "__main__":
    unittest.main()
