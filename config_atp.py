# config_atp.py — robust auto-detection of canonical player_data_atp.csv
from pathlib import Path
import os

REPO_ROOT = Path(__file__).resolve().parents[1]   # repo root (one level up from config file)

# Candidate locations in order of preference:
CANDIDATE_PATHS = [
    REPO_ROOT / "player_data_atp.csv",           # preferred canonical dir (project layout)
    REPO_ROOT / "data" / "player_data_atp.csv",  # common alternate
    REPO_ROOT / "datasets" / "player_data_atp.csv",
]

found_candidates = []
for p in CANDIDATE_PATHS:
    if p.exists():
        found_candidates.append(p)

# fallback: any case-insensitive match near root
if not found_candidates:
    for p in REPO_ROOT.rglob("*player_data_atp.csv"):
        found_candidates.append(p)

if found_candidates:
    # pick the highest-priority match (first in candidate list)
    found = found_candidates[0]
else:
    # nothing found: default to canonical path (will fail loudly later if missing)
    found = REPO_ROOT / "player_data_atp.csv"

players_path = found
# output path default: same directory as players_path / prefixed filename
output_path = players_path
DATA_DIR = players_path.parent
rankings_dir = REPO_ROOT / "atp_rankings"

# DEBUG prints helpful for CI logs
print("DEBUG(config_atp): REPO_ROOT =", REPO_ROOT)
print("DEBUG(config_atp): CANDIDATE_PATHS =", [str(p) for p in CANDIDATE_PATHS])
print("DEBUG(config_atp): found_candidates:", [str(p) for p in found_candidates])
print("DEBUG(config_atp): players_path  ->", players_path)
print("DEBUG(config_atp): DATA_DIR      ->", DATA_DIR)
print("DEBUG(config_atp): rankings_dir  ->", rankings_dir)

# other config values (tweakable)
min_first_date = '2015-01-01'
overwrite_wiki = False
overwrite_ioc = False
begin_index_wiki = 0
end_index_wiki = None
begin_index_ioc = 0
end_index_ioc = None
