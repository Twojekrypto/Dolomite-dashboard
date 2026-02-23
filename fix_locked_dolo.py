#!/usr/bin/env python3
"""Fix locked DOLO amounts in vedolo_holders.json using NEW selectors."""
import json, requests, time

VEDOLO = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
LOCKED_SEL = "0xb45a3c0e"      # locked(uint256) - NEW selector after upgrade
BALANCE_NFT_SEL = "0xe7e242d4"  # balanceOfNFT(uint256) - NEW selector

RPCs = [
    "https://berachain-rpc.publicnode.com/",
    "https://berachain.drpc.org/",
    "https://rpc.berachain.com/",
]

def fetch_locked(token_id, rpc_url):
    encoded = hex(token_id)[2:].zfill(64)
    resp = requests.post(rpc_url, json={
        "jsonrpc": "2.0", "method": "eth_call",
        "params": [{"to": VEDOLO, "data": LOCKED_SEL + encoded}, "latest"], "id": 1
    }, timeout=10)
    r = resp.json()
    if 'error' in r:
        return 0, 0
    result = r.get("result", "0x" + "0" * 128)
    if len(result) >= 130:
        amount = int(result[2:66], 16) / 1e18
        end = int(result[66:130], 16)
        return amount, end
    return 0, 0

def fetch_vote_weight(token_id, rpc_url):
    encoded = hex(token_id)[2:].zfill(64)
    resp = requests.post(rpc_url, json={
        "jsonrpc": "2.0", "method": "eth_call",
        "params": [{"to": VEDOLO, "data": BALANCE_NFT_SEL + encoded}, "latest"], "id": 1
    }, timeout=10)
    r = resp.json()
    if 'error' in r:
        return 0
    result = r.get("result", "0x0")
    return int(result, 16) / 1e18 if result else 0

def main():
    with open("vedolo_holders.json") as f:
        data = json.load(f)

    holders = data["holders"]
    all_tokens = []
    for h in holders:
        for td in h.get("token_details", []):
            all_tokens.append(td["id"])

    print(f"Total tokens: {len(all_tokens)}")

    locked_data = {}
    vote_data = {}
    rpc_idx = 0

    for i, tid in enumerate(all_tokens):
        rpc = RPCs[rpc_idx % len(RPCs)]
        for retry in range(3):
            try:
                amount, end = fetch_locked(tid, rpc)
                vw = fetch_vote_weight(tid, rpc)
                locked_data[tid] = {"amount": amount, "end": end}
                vote_data[tid] = vw
                break
            except Exception:
                rpc = RPCs[(rpc_idx + retry + 1) % len(RPCs)]
                time.sleep(0.5)

        if (i + 1) % 500 == 0:
            non_zero_l = sum(1 for v in locked_data.values() if v["amount"] > 0)
            non_zero_v = sum(1 for v in vote_data.values() if v > 0)
            print(f"  {i+1}/{len(all_tokens)} | locked>0: {non_zero_l} | vw>0: {non_zero_v}", flush=True)
            rpc_idx += 1

        time.sleep(0.02)

    # Update holders
    total_locked = 0
    total_vw = 0
    for h in holders:
        holder_dolo = 0
        holder_vw = 0
        for td in h.get("token_details", []):
            ld = locked_data.get(td["id"], {"amount": 0, "end": 0})
            td["dolo"] = round(ld["amount"], 2)
            td["end"] = ld["end"]
            td["vote_weight"] = round(vote_data.get(td["id"], 0), 4)
            holder_dolo += ld["amount"]
            holder_vw += vote_data.get(td["id"], 0)

        h["total_dolo"] = round(holder_dolo, 2)
        h["total_vote_weight"] = round(holder_vw, 4)

        ends = [td["end"] for td in h.get("token_details", []) if td["end"] > 0]
        h["earliest_lock_end"] = min(ends) if ends else 0
        h["latest_lock_end"] = max(ends) if ends else 0

        total_locked += holder_dolo
        total_vw += holder_vw

    data["stats"]["total_locked_dolo"] = round(total_locked, 2)
    data["stats"]["total_vote_weight"] = round(total_vw, 4)

    # Re-sort by total_dolo
    holders.sort(key=lambda h: h["total_dolo"], reverse=True)
    for i, h in enumerate(holders, 1):
        h["rank"] = i

    with open("vedolo_holders.json", "w") as f:
        json.dump(data, f, indent=2)

    non_zero_l = sum(1 for v in locked_data.values() if v["amount"] > 0)
    non_zero_v = sum(1 for v in vote_data.values() if v > 0)
    print(f"\n✅ Done!")
    print(f"   Locked > 0: {non_zero_l}/{len(all_tokens)}")
    print(f"   Vote weight > 0: {non_zero_v}/{len(all_tokens)}")
    print(f"   Total locked DOLO: {total_locked:,.2f}")
    print(f"   Total vote weight: {total_vw:,.2f}")

    print(f"\n🏆 Top 10:")
    for h in holders[:10]:
        print(f"  #{h['rank']:<4} {h['address'][:12]}… {h['total_dolo']:>14,.2f} DOLO  {h['total_vote_weight']:>12,.2f} veDOLO")

if __name__ == "__main__":
    main()
