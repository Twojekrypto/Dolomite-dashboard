import json

CA = "0x7efd088ae500598a19a242d6d48b9f7e0d061176".lower()
try:
    d = json.load(open("dolo_flows_state.json"))
except Exception:
    d = {}

receivers = set()
for t in d.get("eth_transfers", []):
    if t[0].lower() == CA: receivers.add(t[1].lower())
for t in d.get("bera_transfers", []):
    if t[0].lower() == CA: receivers.add(t[1].lower())

print(f"Found {len(receivers)} investors.")

with open("index.html", "r") as f:
    content = f.read()

# find DOLO_ADDR_LABELS
if "const DOLO_ADDR_LABELS = {" in content:
    lines = content.splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("// Vesting Claims") or "early_investor_end" in line:
            pass # just a check
            
    # let's just create a new chunk and inject it before "}; // end config"
    pass

