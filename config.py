# config.py — robust auto-detection of canonical player_data_wta.csv
from pathlib import Path
import os

REPO_ROOT = Path(__file__).resolve().parents[1]   # repo root

# Candidate locations in order of preference:
CANDIDATE_PATHS = [
    REPO_ROOT / "player_data_wta.csv",                             # prefer root copy
    REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv",    # legacy location
    REPO_ROOT / "player_data_wta" / "player_data_wta.csv",
    REPO_ROOT / "data" / "player_data_wta.csv",
]

found_candidates = []
for p in CANDIDATE_PATHS:
    if p.exists():
        found_candidates.append(p)

# fallback: any case-insensitive match near root
if not found_candidates:
    for p in REPO_ROOT.rglob("*player_data_wta.csv"):
        found_candidates.append(p)

if found_candidates:
    # pick the highest-priority from the candidate list
    # (we already added candidates in priority order)
    found = found_candidates[0]
else:
    # if nothing found, default to canonical legacy path (will cause fail early)
    found = REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv"

# canonical paths exposed to scripts
players_path = found
output_path = found
DATA_DIR = players_path.parent
rankings_dir = REPO_ROOT / "wta_rankings"

# DEBUG prints for CI logs
print("DEBUG(config): REPO_ROOT =", REPO_ROOT)
print("DEBUG(config): candidate CSVs checked:", [str(p) for p in CANDIDATE_PATHS])
print("DEBUG(config): found_candidates:", [str(p) for p in found_candidates])
print("DEBUG(config): players_path  ->", players_path)
print("DEBUG(config): DATA_DIR      ->", DATA_DIR)
print("DEBUG(config): rankings_dir  ->", rankings_dir)

# other config values (unchanged)
min_first_date = '2015-01-01'
overwrite_wiki = False
overwrite_ioc = False
begin_index_wiki = 0
end_index_wiki = None
begin_index_ioc = 0
end_index_ioc = None
