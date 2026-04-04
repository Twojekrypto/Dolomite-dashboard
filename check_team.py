import json

CA = "0x185000fB4D98ACEa1A771dB3714A431F7FE51caC".lower()
try:
    d = json.load(open("dolo_flows_state.json"))
except:
    d = {}

receivers = set()
for chain_key in ['eth', 'bera']:
    transfers = d.get(f"{chain_key}_transfers", [])
    for t in transfers:
        if t[0].lower() == CA:
            receivers.add(t[1].lower())

print("Found", len(receivers), "receivers from", CA)
for r in receivers:
    print(r)
