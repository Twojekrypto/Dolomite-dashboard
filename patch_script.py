with open("generate_dolo_flows.py", "r") as f:
    text = f.read()

# I want to inject the vesting extraction right before:
# with open('dolo_flows.json', 'w') as f:
#     json.dump(output, f, indent=2)

inject = """
    # Dynamically extract all Vesting Claimers (Early Investors)
    vesting_ca = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
    early_investors = set()
    for chain_key in ['eth', 'bera']:
        if chain_key in all_transfers:
            for t in all_transfers[chain_key]:
                if t[0].lower() == vesting_ca:
                    early_investors.add(t[1].lower())
    
    with open('vesting_investors.json', 'w') as f:
        json.dump(list(early_investors), f, indent=2)
    print(f"  🧑‍💼 Saved {len(early_investors)} vesting early investors to vesting_investors.json")

    # Now dump the main file
    with open('dolo_flows.json', 'w') as f:
        json.dump(output, f, indent=2)
"""

if "with open('dolo_flows.json', 'w') as f:" in text:
    text = text.replace("    with open('dolo_flows.json', 'w') as f:\n        json.dump(output, f, indent=2)", inject)

with open("generate_dolo_flows.py", "w") as f:
    f.write(text)
print("generate_dolo_flows.py patched.")
