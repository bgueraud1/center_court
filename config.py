# config.py — robust CI-aware
from pathlib import Path
import shutil
import os

# Prefer GITHUB_WORKSPACE in CI (this is the path where actions/checkout places the repo)
gw = os.environ.get("GITHUB_WORKSPACE")
if gw:
    REPO_ROOT = Path(gw)
else:
    # fallback: file-based discovery (works locally)
    REPO_ROOT = Path(__file__).resolve().parents[1]

CANONICAL = REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv"

# Look for any existing player_data_wta.csv in repo or near repo root
found_candidates = []
# some likely locations (ordered)
candidates = [
    REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv",
    REPO_ROOT / "player_base_and_maps" / "data" / "player_data_wta.csv",
    REPO_ROOT / "player_data_wta.csv",
    REPO_ROOT / "data" / "player_data_wta.csv",
]
for p in candidates:
    if p.exists():
        found_candidates.append(p)

# also search case-insensitive if none found
if not found_candidates:
    for p in REPO_ROOT.rglob("*player_data_wta.csv"):
        found_candidates.append(p)

if found_candidates:
    found = found_candidates[0]
else:
    found = None

copied_msg = ""
if found and found.resolve() != CANONICAL.resolve():
    CANONICAL.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(found, CANONICAL)
        copied_msg = f"Copied existing CSV from {found} -> {CANONICAL}"
    except Exception as e:
        copied_msg = f"Could not copy {found} -> {CANONICAL}: {e}"
elif found:
    copied_msg = f"Found canonical CSV at {CANONICAL}"
else:
    copied_msg = "No CSV found at any candidate path; will create canonical if needed."

# Expose variables used by scripts
players_path = CANONICAL
output_path = CANONICAL
DATA_DIR = players_path.parent
rankings_dir = REPO_ROOT / "wta_rankings"

# debug
print("DEBUG(config): GITHUB_WORKSPACE =", gw)
print("DEBUG(config): REPO_ROOT =", REPO_ROOT)
print("DEBUG(config): canonical CSV =", CANONICAL)
print("DEBUG(config): found_candidates:", [str(p) for p in found_candidates])
print("DEBUG(config): players_path (used) =", players_path)
print("DEBUG(config): copy status ->", copied_msg)
print("DEBUG(config): DATA_DIR  =", DATA_DIR)
print("DEBUG(config): rankings_dir =", rankings_dir)

# rest of config
min_first_date = '2015-01-01'
overwrite_wiki = False
overwrite_ioc = False
begin_index_wiki = 0
end_index_wiki = None
begin_index_ioc = 0
end_index_ioc = None
