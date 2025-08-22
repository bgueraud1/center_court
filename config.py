from pathlib import Path
import shutil
import os

REPO_ROOT = Path(__file__).resolve().parents[1]   # repo root
CANONICAL = REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv"

# try to find an existing CSV anywhere (case-insensitive) — fallback to canonical
found_candidates = []
for p in (
    REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv",
    REPO_ROOT / "player_base_and_maps" / "data" / "player_data_wta.csv",
    REPO_ROOT / "player_data_wta.csv",
    REPO_ROOT / "data" / "player_data_wta.csv",
):
    if p.exists():
        found_candidates.append(p)

if not found_candidates:
    # case-insensitive search near root
    for p in REPO_ROOT.rglob("*player_data_wta.csv"):
        found_candidates.append(p)

if found_candidates:
    found = found_candidates[0]
else:
    found = CANONICAL  # none found, will create at canonical later

# If found somewhere else than canonical, copy it to canonical (safe, non-destructive)
if found.exists() and found.resolve() != CANONICAL.resolve():
    CANONICAL.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(found, CANONICAL)
        copied_msg = f"Copied existing CSV from {found} -> {CANONICAL}"
    except Exception as e:
        copied_msg = f"Could not copy {found} -> {CANONICAL}: {e}"
else:
    copied_msg = "No external CSV to copy (either canonical already present or none found)."

# Expose variables used by scripts (always use CANONICAL)
players_path = CANONICAL
output_path = CANONICAL
DATA_DIR = players_path.parent
rankings_dir = REPO_ROOT / "wta_rankings"

# Debug prints (CI logs)
print("DEBUG(config): REPO_ROOT =", REPO_ROOT)
print("DEBUG(config): canonical CSV =", CANONICAL)
print("DEBUG(config): found_candidates:", [str(p) for p in found_candidates])
print("DEBUG(config): players_path (used) =", players_path)
print("DEBUG(config): copy status ->", copied_msg)
print("DEBUG(config): DATA_DIR  =", DATA_DIR)
print("DEBUG(config): rankings_dir =", rankings_dir)

# other options
min_first_date = '2015-01-01'
overwrite_wiki = False
overwrite_ioc = False
begin_index_wiki = 0
end_index_wiki = None
begin_index_ioc = 0
end_index_ioc = None