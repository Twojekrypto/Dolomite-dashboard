import json
import os
import re

# MemPalace: Contradiction Detection Script
# Sprawdza czy nasze lokalne bazy danych nie naruszają zakodowanej wiedzy w mózgu .mempalace-brain.yaml

MEM_PALACE_FILE = ".mempalace-brain.yaml"
INVESTORS_FILE = "vesting_investors.json"

def get_yaml_value(content, key_path):
    """Prosty parser YAML do wyciągnięcia konkretnych adresów bez paczki PyYAML"""
    lines = content.split("\n")
    for line in lines:
        if key_path in line:
            match = re.search(r'["\'](0x[a-fA-F0-9]{40})["\']', line)
            if match:
                return match.group(1).lower()
    return None

def verify_palace_rules():
    print("🏰 MemPalace: Inicjalizacja skanera sprzeczności...")
    
    if not os.path.exists(MEM_PALACE_FILE):
        print(f"❌ BLĄD: Nie znaleziono pliku mózgu {MEM_PALACE_FILE}")
        return

    with open(MEM_PALACE_FILE, "r") as f:
        brain_data = f.read()

    # Wyciągnij kluczowe dane z mózgu MemPalace
    odolo_vester = get_yaml_value(brain_data, "odolo_vester:")
    okx_wallet = get_yaml_value(brain_data, "okx:")
    robinhood_wallet = get_yaml_value(brain_data, "robinhood:")
    
    cex_wallets = [w for w in [okx_wallet, robinhood_wallet] if w]

    print(f"✅ Wczytano logikę z MemPalace: oDoloVester={odolo_vester[:6]}..., Zdefiniowane CEX={len(cex_wallets)}")

    # Walidacja twardych danych
    if not os.path.exists(INVESTORS_FILE):
        print(f"⚠️ Pomijam weryfikację inwestorów - brak {INVESTORS_FILE}")
        return

    with open(INVESTORS_FILE, "r") as f:
        investors_data = json.load(f)

    all_investors = set(investors_data.get("early_investors", []) + 
                        investors_data.get("investors", []) + 
                        investors_data.get("team", []))
    
    all_investors = {addr.lower() for addr in all_investors}

    contradictions_found = 0

    print("🛡️ Skanowanie sprzeczności (Contradiction Detection)...")

    # RULE 1: CEX wallets cannot be tagged as Investors
    for cex in cex_wallets:
        if cex in all_investors:
            print(f"❌ [CONFLICT ALERT]: Adres Giełdy CEX ({cex}) znajduje się na liście inwestorów vestingowych!")
            contradictions_found += 1

    # RULE 2: oDolo Vester is a neutral contract, not an investor
    if odolo_vester and odolo_vester in all_investors:
        print(f"❌ [CONFLICT ALERT]: Kontrakt oDolo Vester ({odolo_vester}) znajduje się błędnie na liście inwestorów. Vester ma status NIEUTRALNY wg MemPalace.")
        contradictions_found += 1

    if contradictions_found == 0:
        print("✅ Pomyślnie zweryfikowano: Żadne reguły biznesowe MemPalace nie wykluczają się z plikami bazy danych Dolomite.")
    else:
        print(f"🔥 Wykryto {contradictions_found} sprzeczności! Popraw dane zanim zbuildjesz aplikację.")

if __name__ == "__main__":
    verify_palace_rules()
