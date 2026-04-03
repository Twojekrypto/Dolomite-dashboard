import requests
import time

rpc = "https://berachain-rpc.publicnode.com/"
vedolo = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
topic_transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

resp = requests.post(rpc, json={"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1})
end_block = int(resp.json()["result"], 16)
start_block = 2_925_000

print(f"Fetching logs from {start_block} to {end_block}...")

all_logs = []
current = start_block
chunk = 50000

while current <= end_block:
    to_block = min(current + chunk - 1, end_block)
    resp = requests.post(rpc, json={
        "jsonrpc":"2.0",
        "method":"eth_getLogs",
        "params":[{
            "address": vedolo,
            "topics": [topic_transfer],
            "fromBlock": hex(current),
            "toBlock": hex(to_block)
        }],
        "id":1
    })
    data = resp.json()
    if "error" in data:
        chunk = chunk // 2
        continue
    all_logs.extend(data["result"])
    current = to_block + 1

transfers = {}
for log in all_logs:
    if len(log["topics"]) == 4:
        from_addr = "0x" + log["topics"][1][26:]
        to_addr = "0x" + log["topics"][2][26:]
        token_id = int(log["topics"][3], 16)
        tx_hash = log["transactionHash"]
        # only care if transferred from the vester
        if from_addr.lower() == "0x3e9b9a16743551da49b5e136c716bba7932d2cec":
            transfers[token_id] = {"to": to_addr, "tx": tx_hash}

print(f"Total transfers from Vester: {len(transfers)}")
if transfers:
    print("Example:", list(transfers.items())[:2])

