import json
import os
import sys

STATE_FILE = "dolo_flows_state.json"
OUTPUT = "vesting_investors.json"

PROXY_ADDR = "0x7efd088ae500598a19a242d6d48b9f7e0d061176".lower()
INVESTOR_ADDR = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07".lower()
TEAM_ADDR = "0x185000fb4d98acea1a771db3714a431f7fe51cac".lower()

early_set = set()
inv_set = set()
team_set = set()

if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
    print("Loaded state")
    
    for tx in state["chains"]["arbitrum"]["transfers"]:
        f_addr, t_addr, v = tx["f"].lower(), tx["t"].lower(), tx["v"]
        if f_addr == PROXY_ADDR:
            early_set.add(t_addr)
        if f_addr == INVESTOR_ADDR:
            inv_set.add(t_addr)
        if f_addr == TEAM_ADDR:
            team_set.add(t_addr)
            
    for tx in state["chains"]["ethereum"]["transfers"]:
        f_addr, t_addr, v = tx["f"].lower(), tx["t"].lower(), tx["v"]
        if f_addr == PROXY_ADDR: early_set.add(t_addr)
        if f_addr == INVESTOR_ADDR: inv_set.add(t_addr)
        if f_addr == TEAM_ADDR: team_set.add(t_addr)

out = {
    "early_investors": list(early_set),
    "investors": list(inv_set),
    "team": list(team_set)
}

with open(OUTPUT, "w") as f:
    json.dump(out, f, indent=2)
print(f"Saved! Early: {len(early_set)}, Inv: {len(inv_set)}, Team: {len(team_set)}")
