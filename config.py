# config.py (CI-aware)
from pathlib import Path
import shutil
import os

gw = os.environ.get("GITHUB_WORKSPACE")
if gw:
    REPO_ROOT = Path(gw)
else:
    REPO_ROOT = Path(__file__).resolve().parents[1]

CANONICAL = REPO_ROOT / "player_base_and_maps" / "player_data_wta.csv"

# search for any existing csv inside repo (or in strange nested dirs)
found = None
for p in REPO_ROOT.rglob("*player_data_wta.csv"):
    found = p
    break

if found and found.resolve() != CANONICAL.resolve():
    CANONICAL.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(found, CANONICAL)
        copy_status = f"Copied {found} -> {CANONICAL}"
    except Exception as e:
        copy_status = f"Could not copy {found} -> {CANONICAL}: {e}"
elif found:
    copy_status = f"Using found CSV at {found}"
else:
    copy_status = "No CSV found; canonical path will be used/created when writing"

players_path = CANONICAL
output_path = CANONICAL
DATA_DIR = players_path.parent
rankings_dir = REPO_ROOT / "wta_rankings"

print("DEBUG(config): GITHUB_WORKSPACE =", gw)
print("DEBUG(config): REPO_ROOT =", REPO_ROOT)
print("DEBUG(config): players_path (canonical) =", players_path)
print("DEBUG(config): copy_status ->", copy_status)


# rest of config
min_first_date = '2015-01-01'
overwrite_wiki = False
overwrite_ioc = False
begin_index_wiki = 0
end_index_wiki = None
begin_index_ioc = 0
end_index_ioc = None
