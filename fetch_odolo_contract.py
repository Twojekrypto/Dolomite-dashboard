#!/usr/bin/env python3
"""
Fetch oDOLO contract data via Berachain RPC and save as static JSON.
Replicates the RPC calls the browser currently makes, but runs server-side
in GitHub Actions for reliability.
"""

import json
import os
import requests
from datetime import datetime, timezone

from rpc_client import RpcClient, RpcError, decode_uint256, safe_host

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, "odolo_contract_data.json")

ODOLO_TOKEN = "0x02E513b5B54eE216Bf836ceb471507488fC89543"
ODOLO_VESTER = "0x3E9b9A16743551DA49b5e136C716bBa7932d2cEc"
FUTURE_REWARDS_WALLET = "0x79E6E932bf6686a4D357D7821E6e08835Ba8A026"

# Function selectors
SEL = {
    "totalSupply": "0x18160ddd",
    "balanceOf": "0x70a08231",
    "decimals": "0x313ce567",
    "promisedTokens": "0x5e17b694",
    "pushedTokens": "0x818c16e2",
    "availableTokens": "0x69bb4dc2",
}

ROUTESCAN_API = "https://api.routescan.io/v2/network/mainnet/evm/80094/etherscan/api"


def pad_address(address):
    return address.replace("0x", "").lower().zfill(64)


VESTER_PADDED = pad_address(ODOLO_VESTER)
FUTURE_REWARDS_PADDED = pad_address(FUTURE_REWARDS_WALLET)


def derive_supply_metrics(total_supply, in_vester_balance, future_rewards_reserve):
    """Calculate market-circulating oDOLO from on-chain balances."""
    in_circulation = max(0.0, total_supply - future_rewards_reserve - in_vester_balance)
    return {
        "futureRewardsWallet": FUTURE_REWARDS_WALLET,
        "futureRewardsReserve": future_rewards_reserve,
        "circulatingExclusions": {
            "futureRewardsReserve": future_rewards_reserve,
            "vesterBalance": in_vester_balance,
        },
        "circulatingMethodology": "totalSupply - futureRewardsReserve - inVesterBalance",
        "inCirculation": in_circulation,
    }


def get_holder_count():
    """Fetch oDOLO token holder count from Routescan API."""
    try:
        params = {
            "module": "token",
            "action": "tokenholdercount",
            "contractaddress": ODOLO_TOKEN
        }
        resp = requests.get(ROUTESCAN_API, params=params, timeout=10)
        data = resp.json()
        if data.get("status") == "1" and data.get("result"):
            return int(data["result"])
    except Exception as e:
        print(f"   ⚠️ Could not fetch holder count: {e}")
    return None


def main():
    print("📡 Fetching oDOLO contract data via RPC...")

    client = RpcClient(chain="berachain")
    try:
        # Batch 1: Token data
        batch1 = client.eth_call_batch([
            (ODOLO_TOKEN, SEL["totalSupply"]),
            (ODOLO_TOKEN, SEL["decimals"]),
            (ODOLO_TOKEN, SEL["balanceOf"] + VESTER_PADDED),
            (ODOLO_TOKEN, SEL["balanceOf"] + FUTURE_REWARDS_PADDED),
        ])

        # Batch 2: Vester data
        batch2 = client.eth_call_batch([
            (ODOLO_VESTER, SEL["promisedTokens"]),
            (ODOLO_VESTER, SEL["pushedTokens"]),
            (ODOLO_VESTER, SEL["availableTokens"]),
        ])
    except RpcError as e:
        print(f"   ❌ All RPC endpoints failed! {e}")
        if os.path.exists(OUTPUT_FILE):
            print(f"   Keeping existing {OUTPUT_FILE}")
        else:
            print(f"   No existing file — cannot create placeholder")
        return

    decimals = decode_uint256(batch1[1]) or 18
    divisor = 10 ** decimals

    data = {
        "totalSupply": decode_uint256(batch1[0]) / divisor,
        "inVesterBalance": decode_uint256(batch1[2]) / divisor,
        "futureRewardsReserve": decode_uint256(batch1[3]) / divisor,
        "promisedTokens": decode_uint256(batch2[0]) / divisor,
        "pushedTokens": decode_uint256(batch2[1]) / divisor,
        "availableTokens": decode_uint256(batch2[2]) / divisor,
        "decimals": decimals,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        # Store only the provider host, never the full URL (it contains the API key).
        "rpc_source": safe_host(client.last_endpoint or ""),
    }

    data.update(derive_supply_metrics(
        data["totalSupply"],
        data["inVesterBalance"],
        data["futureRewardsReserve"],
    ))

    # Fetch holder count from Routescan
    holders = get_holder_count()
    if holders is not None:
        data["holders"] = holders
        print(f"   Holders: {holders:,}")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)

    print(f"   ✅ Saved odolo_contract_data.json")
    print(f"   Total Supply: {data['totalSupply']:,.2f}")
    print(f"   Future Rewards Reserve: {data['futureRewardsReserve']:,.2f}")
    print(f"   Vester Balance: {data['inVesterBalance']:,.2f}")
    print(f"   Available Tokens (vester accounting): {data['availableTokens']:,.2f}")
    print(f"   Pushed Tokens (vester accounting): {data['pushedTokens']:,.2f}")
    print(f"   In Circulation: {data['inCirculation']:,.2f}")


if __name__ == "__main__":
    main()
