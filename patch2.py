with open("generate_dolo_flows.py", "r") as f:
    text = f.read()

import re

old_chunk = """    # Dynamically extract all Vesting Claimers (Early Investors)
    vesting_ca = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
    early_investors = set()
    for chain_key in ['eth', 'bera']:
        if chain_key in all_transfers:
            for t in all_transfers[chain_key]:
                if t[0].lower() == vesting_ca:
                    early_investors.add(t[1].lower())
    
    with open(os.path.join(DATA_DIR, "vesting_investors.json"), "w") as f:
        json.dump(list(early_investors), f, indent=2)
    print(f"  🧑‍💼 Saved {len(early_investors)} vesting early investors to vesting_investors.json")"""

new_chunk = """    # Dynamically extract all Vesting Claimers (Early Investors)
    vesting_ca = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
    investor_ca = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"
    
    out_investors = {
        "early_investors": [],
        "investors": []
    }
    
    early_set = set()
    inv_set = set()
    
    for chain_key in ['eth', 'bera']:
        if chain_key in all_transfers:
            for t in all_transfers[chain_key]:
                if t[0].lower() == vesting_ca:
                    early_set.add(t[1].lower())
                elif t[0].lower() == investor_ca:
                    inv_set.add(t[1].lower())
                    
    out_investors["early_investors"] = list(early_set)
    out_investors["investors"] = list(inv_set)
    
    with open(os.path.join(DATA_DIR, "vesting_investors.json"), "w") as f:
        json.dump(out_investors, f, indent=2)
    print(f"  🧑‍💼 Saved {len(early_set)} early investors and {len(inv_set)} investors to vesting_investors.json")"""

if old_chunk in text:
    text = text.replace(old_chunk, new_chunk)
    with open("generate_dolo_flows.py", "w") as f:
        f.write(text)
    print("Patched successfully")
else:
    print("Could not find old chunk")
