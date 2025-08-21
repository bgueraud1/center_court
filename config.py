# Configuration file for the player_data_wta enrichment Pipeline
# Created Aug 7 2025
# Ran to end
# config.py (patch auto-detect)
from pathlib import Path
import os
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]   # repo root
# candidate relative locations (ordered by preference)
CANDIDATE_PATHS = [
    REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv",
    REPO_ROOT / "player_base_and_maps" / "data" / "player_data_wta.csv",
    REPO_ROOT / "player_data_wta.csv",
    REPO_ROOT / "data" / "player_data_wta.csv",
]

found = None
found_candidates = []
for p in CANDIDATE_PATHS:
    if p.exists():
        found_candidates.append(p)

# also allow any case-insensitive match near root (helpful if someone committed with different case)
if not found_candidates:
    for p in REPO_ROOT.rglob("*player_data_wta.csv"):
        found_candidates.append(p)

if found_candidates:
    # prefer the one under player_base_and_maps if present
    pref = next((p for p in found_candidates if "player_base_and_maps" in str(p)), None)
    found = pref or found_candidates[0]
else:
    # no file found — keep default location under player_base_and_maps (so code fails early & deterministically)
    found = REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv"

# expose same names your scripts expect
DATA_DIR = found.parent
players_path = found
output_path = found
REPO_ROOT = REPO_ROOT
rankings_dir = REPO_ROOT / "wta_rankings"

# debug prints (useful in CI)
print("DEBUG(config): REPO_ROOT =", REPO_ROOT)
print("DEBUG(config): candidate CSVs checked:", [str(p) for p in CANDIDATE_PATHS])
print("DEBUG(config): found_candidates:", [str(p) for p in found_candidates])
print("DEBUG(config): players_path =", players_path)
print("DEBUG(config): DATA_DIR  =", DATA_DIR)
print("DEBUG(config): rankings_dir =", rankings_dir)






min_first_date = '2015-01-01' # date under which player's data won't be overwritten if overwriting activated



overwrite_wiki = False
overwrite_ioc = False

begin_index_wiki = 0  # index under which player won't be scraped for wiki data
end_index_wiki = None # index above which player won't be scraped for wiki data

begin_index_ioc = 0  # index under which player won't be scraped for ioc
end_index_ioc = None # index above which player won't be scraped for ioc




