import json
try:
    d = json.load(open("dolo_flows_state.json"))
except:
    d = {}

vesting_ca = "0x7efd088ae500598a19a242d6d48b9f7e0d061176".lower()
investor_ca = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07".lower()
team_ca = "0x185000fB4D98ACEa1A771dB3714A431F7FE51caC".lower()

out_investors = {
    "early_investors": [],
    "investors": [],
    "team": []
}

early_set = set()
inv_set = set()
team_set = set()

for chain_key in ['eth', 'bera']:
    transfers = d.get(f"{chain_key}_transfers", [])
    for t in transfers:
        if t[0].lower() == vesting_ca:
            early_set.add(t[1].lower())
        elif t[0].lower() == investor_ca:
            inv_set.add(t[1].lower())
        elif t[0].lower() == team_ca:
            team_set.add(t[1].lower())

out_investors["early_investors"] = list(early_set)
out_investors["investors"] = list(inv_set)
out_investors["team"] = list(team_set)

with open("vesting_investors.json", "w") as f:
    json.dump(out_investors, f, indent=2)

print(f"Generated locally: {len(early_set)} early, {len(inv_set)} normal investors, {len(team_set)} team members")
