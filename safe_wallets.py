#!/usr/bin/env python3
"""
Single source of truth for Safe (Gnosis Safe) singleton/mastercopy addresses.

Safe proxies store the singleton address in storage slot 0, so a holder
contract is classified as a user Safe wallet when
eth_getStorageAt(addr, 0x0) points at one of these official deployments.
Addresses are identical across EVM chains (deterministic deployments).

Source: safe-global/safe-deployments (canonical + eip155 variants).
Consumers: generate_dolo_holders.py, generate_dolo_flows.py.
"""

SAFE_SINGLETON_ADDRS = {
    # Safe 1.4.1
    "0x41675c099f32341bf84bfc5382af534df5c7461a",  # Safe
    "0x29fcb43b46531bca003ddc8fcb67ffe91900c762",  # SafeL2
    # Safe 1.3.0 — canonical deployments
    "0xd9db270c1b5e3bd161e8c8503c55ceabee709552",  # GnosisSafe
    "0x3e5c63644e683549055b9be8653de26e0b4cd36e",  # GnosisSafeL2
    # Safe 1.3.0 — eip155 (alternative deployer used on some chains)
    "0x69f4d1788e39c87893c980c06edf4b7f686e2938",  # GnosisSafe
    "0xfb1bffc9d739b8d520daf37df666da4c687191ea",  # GnosisSafeL2
    # Safe 1.2.0 / 1.1.1 — legacy mainnet Safes still in active use
    "0x6851d6fdfafd08c0295c392436245e5bc78b0185",  # GnosisSafe 1.2.0
    "0x34cfac646f301356faa8b21e94227e3583fe3f5f",  # GnosisSafe 1.1.1
}
