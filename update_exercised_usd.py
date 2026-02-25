#!/usr/bin/env python3
"""
Incremental update of Exercised Volume in USD.
Reads existing exercised_usd.json, scans only NEW transactions since last block,
and updates the total. Run periodically (cron, GitHub Action, etc).
"""

import requests
import time
import json
import os
from datetime import datetime, timezone

ROUTESCAN_API = "https://api.routescan.io/v2/network/mainnet/evm/80094/etherscan/api"
VESTER_CONTRACT = "0x3E9b9A16743551DA49b5e136C716bBa7932d2cEc"
USDC_E_CONTRACT = "0x549943E04F40284185054145c6e4e9568c1D3241".lower()
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
EXERCISE_METHOD_ID = "0xa88f8139"
DATA_FILENAME = "exercised_usd.json"

RATE_LIMIT_DELAY = 0.35
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3


def get_tx_details_from_receipt(tx_hash, retries=MAX_RETRIES):
        """Get USDC.e amount from receipt WITH RETRY support."""
        params = {
            "module": "proxy",
            "action": "eth_getTransactionReceipt",
            "txhash": tx_hash
        }

    for attempt in range(retries):
                try:
                                resp = requests.get(ROUTESCAN_API, params=params, timeout=REQUEST_TIMEOUT)
                                data = resp.json()
                                if "result" not in data or data["result"] is None:
                                                    if attempt < retries - 1:
                                                                            delay = (2 ** attempt)
                                                                            time.sleep(delay)
                                                                            continue
                                                                        return None

            for log in data["result"].get("logs", []):
                                if len(log["topics"]) < 3 or log["topics"][0] != TRANSFER_TOPIC:
                                                        continue
                                                    token_addr = log["address"].lower()
                to_addr = "0x" + log["topics"][2][26:].lower()
                if token_addr == USDC_E_CONTRACT and to_addr == VESTER_CONTRACT.lower():
                                        usdc_amount = int(log["data"], 16) / 10**6
                                        return usdc_amount
                                return 0.0
except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            if attempt < retries - 1:
                                delay = (2 ** attempt)
                time.sleep(delay)
else:
                print(f"    Receipt failed after {retries} retries: {tx_hash[:16]}... ({e})")
                return None
    return None


def main():
        print("=" * 60)
    print("oDOLO Exercised Volume Incremental Updater")
    print("=" * 60)

    # Load existing state
    if os.path.exists(DATA_FILENAME):
                with open(DATA_FILENAME, "r") as f:
                                state = json.load(f)
        print(f"Loaded existing data: ${state['total_exercised_usd']:,.2f} (up to block {state['last_block']})")
else:
        state = {"total_exercised_usd": 0, "last_block": 0, "last_updated": ""}
        print("No existing data found. Starting from scratch.")

    start_block = state["last_block"] + 1
    all_txs = []
    page = 1
    print(f"\n[1/2] Fetching new transactions from block {start_block}...")

    while True:
                params = {
                                "module": "account", "action": "txlist",
                                "address": VESTER_CONTRACT,
                                "startblock": start_block, "endblock": 99999999,
                                "page": page, "offset": 100, "sort": "asc"
                }
        resp = requests.get(ROUTESCAN_API, params=params, timeout=REQUEST_TIMEOUT)
        data = resp.json()

        if data["status"] != "1" or not data["result"]:
                        break

        txs = data["result"]
        all_txs.extend(txs)
        if len(txs) < 100:
                        break
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    exercise_txs = [
                tx for tx in all_txs
                if tx.get("methodId") == EXERCISE_METHOD_ID
                and tx.get("isError") == "0"
                and tx.get("txreceipt_status") == "1"
    ]
    print(f"  Found {len(all_txs)} total txs, {len(exercise_txs)} are 'exercise'")

    if not exercise_txs:
                print("\nNo new exercise transactions found. State remains unchanged.")
        return

    print("\n[2/2] Processing receipts with retries...")
    new_usd = 0
    max_block = state["last_block"]
    failed_txs = []

    for i, tx in enumerate(exercise_txs):
                tx_hash = tx["hash"]
        block_num = int(tx["blockNumber"])
        usdc_amount = get_tx_details_from_receipt(tx_hash)

        if usdc_amount is not None:
                        new_usd += usdc_amount
            if block_num > max_block:
                                max_block = block_num
else:
            failed_txs.append(tx)

        if (i + 1) % 50 == 0 or i == len(exercise_txs) - 1:
                        print(f"  [{i+1}/{len(exercise_txs)}] New USD: +${new_usd:,.2f}")
        time.sleep(RATE_LIMIT_DELAY)

    if failed_txs:
                print(f"\n  Retrying {len(failed_txs)} failed receipts with longer delays...")
        recovered_usd = 0
        for i, tx in enumerate(failed_txs):
                        time.sleep(1.5)
            usdc_amount = get_tx_details_from_receipt(tx["hash"], retries=5)
            if usdc_amount is not None:
                                recovered_usd += usdc_amount
                new_usd += usdc_amount
                block_num = int(tx["blockNumber"])
                if block_num > max_block:
                                        max_block = block_num
                                if (i + 1) % 20 == 0 or i == len(failed_txs) - 1:
                                                    print(f"    Retry [{i+1}/{len(failed_txs)}] Recovered: +${recovered_usd:,.2f}")

    # Update state
    state["total_exercised_usd"] = round(state["total_exercised_usd"] + new_usd, 2)
    state["last_block"] = max_block
    state["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    with open(DATA_FILENAME, "w") as f:
                json.dump(state, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"SUCCESS!")
    print(f"  Added:         ${new_usd:,.2f}")
    print(f"  New Total:     ${state['total_exercised_usd']:,.2f}")
    print(f"  Final Block:   {state['last_block']}")
    print(f"  Errors:        {len(failed_txs) - (recovered_usd > 0 if 'recovered_usd' in locals() else 0)}")
    print(f"  Saved to {DATA_FILENAME}")


if __name__ == "__main__":
        main()
